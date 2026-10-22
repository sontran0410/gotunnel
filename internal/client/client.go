package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"

	"gotunnel/internal/config"
	"gotunnel/internal/daemon"
	"gotunnel/internal/proto"
	"gotunnel/internal/sched"
	"gotunnel/internal/transport"
	"gotunnel/internal/tunnel"
	"gotunnel/internal/version"
)

// Client is the client daemon.
type Client struct {
	log     *slog.Logger
	sup     *Supervisor
	started time.Time

	mu       sync.Mutex
	cfg      *config.Config
	forwards map[string]*forwarder
	socks    map[string]*socksProxy
	reverses map[string]*reverse
	closed   bool
}

// Params derives session parameters from the configuration.
func Params(cfg *config.Config) tunnel.Params {
	cn := cfg.Connection
	return tunnel.Params{
		TTL:          cn.SessionTTL.D(),
		PingInterval: cn.PingInterval.D(),
		DeadAfter:    cn.DeadAfter.D(),
		MaxInflight:  int(cn.ReplayBuffer),
		Inflight:     int(cn.MaxInflight),
		Adaptive:     cn.Adaptive(),
	}
}

// New creates a client. Call Start to open listeners and Run to operate.
func New(cfg *config.Config, tr transport.Transport, log *slog.Logger) *Client {
	c := &Client{
		log:      log,
		started:  time.Now(),
		cfg:      cfg,
		forwards: map[string]*forwarder{},
		socks:    map[string]*socksProxy{},
		reverses: map[string]*reverse{},
	}
	c.sup = NewSupervisor(tr, Params(cfg), cfg.Connection.ConnectTimeout.D(), cfg.Connection.IdleTimeout.D(), log, c.onSession)
	return c
}

// Supervisor returns the transport supervisor.
func (c *Client) Supervisor() *Supervisor { return c.sup }

// Start opens all configured listeners. Listeners that cannot be opened are
// retried in the background; their errors are returned and shown in status.
func (c *Client) Start() []error {
	c.mu.Lock()
	cfg := c.cfg
	c.mu.Unlock()
	var errs []error
	for _, f := range cfg.Forwards {
		if err := c.addForward(f, false); err != nil {
			errs = append(errs, err)
		}
	}
	for _, s := range cfg.Socks {
		if err := c.addSocks(s, false); err != nil {
			errs = append(errs, err)
		}
	}
	for _, r := range cfg.Reverse {
		c.addReverse(r)
	}
	c.updateAlwaysUp()
	return errs
}

// Run operates the transport until ctx ends, then closes everything.
func (c *Client) Run(ctx context.Context) {
	c.sup.Run(ctx)
	c.mu.Lock()
	c.closed = true
	for _, f := range c.forwards {
		f.stop()
	}
	for _, s := range c.socks {
		s.stop()
	}
	c.mu.Unlock()
}

func (c *Client) updateAlwaysUp() {
	c.mu.Lock()
	up := len(c.reverses) > 0
	for _, f := range c.forwards {
		if f.cfg.Mode == config.ModeAlways {
			up = true
		}
	}
	c.mu.Unlock()
	c.sup.SetAlwaysUp(up)
}

// openTimeout bounds how long a local connection waits for its stream.
func (c *Client) openTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.Connection.ConnectTimeout.D() + 30*time.Second
}

// open requests a stream from the agent, retrying once if the session is
// replaced while the request is in flight.
func (c *Client) open(ctx context.Context, req proto.Request, class sched.Class) (*yamux.Stream, *tunnel.Stack, error) {
	for attempt := 0; ; attempt++ {
		stack, err := c.sup.Stack(ctx)
		if err != nil {
			return nil, nil, err
		}
		st, err := stack.Request(ctx, req, class)
		if err == nil {
			return st, stack, nil
		}
		if attempt == 0 && errors.Is(err, yamux.ErrSessionShutdown) && ctx.Err() == nil {
			continue
		}
		return nil, nil, err
	}
}

// counters are per-listener statistics.
type counters struct {
	active, total atomic.Int64
	up, down      atomic.Uint64
}

func (k *counters) count(up bool, n int) {
	if up {
		k.up.Add(uint64(n))
	} else {
		k.down.Add(uint64(n))
	}
}

// listener holds a local listener that is retried until it can be opened.
type listener struct {
	addr   string
	accept func(net.Conn)
	log    *slog.Logger

	mu      sync.Mutex
	ln      net.Listener
	err     string
	stopped bool
}

func (l *listener) open() error {
	ln, err := net.Listen("tcp", l.addr)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		if ln != nil {
			ln.Close()
		}
		return nil
	}
	if err != nil {
		l.err = err.Error()
		return err
	}
	l.ln, l.err = ln, ""
	go l.serve(ln)
	return nil
}

// start opens the listener; on failure it keeps retrying in the background.
func (l *listener) start() error {
	err := l.open()
	if err != nil {
		go func() {
			for {
				time.Sleep(30 * time.Second)
				l.mu.Lock()
				stopped := l.stopped
				l.mu.Unlock()
				if stopped || l.open() == nil {
					return
				}
			}
		}()
	}
	return err
}

func (l *listener) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			l.mu.Lock()
			stopped := l.stopped
			l.mu.Unlock()
			if stopped {
				return
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			l.log.Error("accept failed", "addr", l.addr, "err", err)
			time.Sleep(time.Second)
			continue
		}
		go l.accept(conn)
	}
}

func (l *listener) stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	if l.ln != nil {
		l.ln.Close()
	}
}

func (l *listener) status() (listening bool, err string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ln != nil, l.err
}

// forwarder is a local port forward.
type forwarder struct {
	c     *Client
	cfg   config.Forward
	class sched.Class
	ln    *listener
	stats counters
}

func (c *Client) addForward(f config.Forward, strict bool) error {
	config.DefaultForward(&f)
	if errs := config.ValidateForward(&f); len(errs) > 0 {
		return errors.Join(errs...)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("client is shutting down")
	}
	if _, ok := c.forwards[f.Name]; ok {
		c.mu.Unlock()
		return fmt.Errorf("forward %q already exists", f.Name)
	}
	if owner := c.listenOwnerLocked(f.Listen); owner != "" {
		c.mu.Unlock()
		return fmt.Errorf("listen address %s is already used by %s", f.Listen, owner)
	}
	fw := &forwarder{c: c, cfg: f, class: sched.ParseClass(f.Priority)}
	fw.ln = &listener{addr: f.Listen, accept: fw.handle, log: c.log}
	c.forwards[f.Name] = fw
	c.mu.Unlock()

	if err := fw.ln.start(); err != nil {
		if strict {
			c.mu.Lock()
			delete(c.forwards, f.Name)
			c.mu.Unlock()
			fw.ln.stop()
			return fmt.Errorf("forward %s: %w", f.Name, err)
		}
		c.log.Error("cannot listen; will retry", "forward", f.Name, "addr", f.Listen, "err", err)
		return fmt.Errorf("forward %s: %w", f.Name, err)
	}
	c.log.Info("forward ready", "forward", f.Name, "listen", f.Listen, "target", f.Target, "mode", f.Mode)
	return nil
}

func (c *Client) listenOwnerLocked(addr string) string {
	for _, f := range c.forwards {
		if f.cfg.Listen == addr {
			return "forward " + f.cfg.Name
		}
	}
	for _, s := range c.socks {
		if s.cfg.Listen == addr {
			return "socks " + s.cfg.Name
		}
	}
	return ""
}

func (f *forwarder) stop() { f.ln.stop() }

func (f *forwarder) handle(conn net.Conn) {
	c := f.c
	f.stats.total.Add(1)
	f.stats.active.Add(1)
	defer f.stats.active.Add(-1)
	c.sup.ConnStart()
	defer c.sup.ConnEnd()

	ctx, cancel := context.WithTimeout(context.Background(), c.openTimeout())
	st, stack, err := c.open(ctx, proto.Request{Op: proto.OpDial, Target: f.cfg.Target, Prio: f.cfg.Priority}, f.class)
	cancel()
	if err != nil {
		c.log.Warn("forward connection failed", "forward", f.cfg.Name, "target", f.cfg.Target, "err", err)
		conn.Close()
		return
	}
	stack.Proxy(conn, st, f.stats.count)
}

// socksProxy is a local SOCKS5 proxy.
type socksProxy struct {
	c     *Client
	cfg   config.Socks
	class sched.Class
	ln    *listener
	stats counters
}

func (c *Client) addSocks(s config.Socks, strict bool) error {
	config.DefaultSocks(&s)
	if errs := config.ValidateSocks(&s); len(errs) > 0 {
		return errors.Join(errs...)
	}
	c.mu.Lock()
	if _, ok := c.socks[s.Name]; ok {
		c.mu.Unlock()
		return fmt.Errorf("socks %q already exists", s.Name)
	}
	if owner := c.listenOwnerLocked(s.Listen); owner != "" {
		c.mu.Unlock()
		return fmt.Errorf("listen address %s is already used by %s", s.Listen, owner)
	}
	sp := &socksProxy{c: c, cfg: s, class: sched.ParseClass(s.Priority)}
	sp.ln = &listener{addr: s.Listen, accept: sp.handle, log: c.log}
	c.socks[s.Name] = sp
	c.mu.Unlock()
	if err := sp.ln.start(); err != nil {
		if strict {
			c.mu.Lock()
			delete(c.socks, s.Name)
			c.mu.Unlock()
			sp.ln.stop()
		} else {
			c.log.Error("cannot listen; will retry", "socks", s.Name, "addr", s.Listen, "err", err)
		}
		return fmt.Errorf("socks %s: %w", s.Name, err)
	}
	c.log.Info("socks proxy ready", "socks", s.Name, "listen", s.Listen)
	return nil
}

func (s *socksProxy) stop() { s.ln.stop() }

func (s *socksProxy) handle(conn net.Conn) {
	c := s.c
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	target, err := socksHandshake(conn)
	if err != nil {
		c.log.Debug("socks handshake failed", "socks", s.cfg.Name, "err", err)
		conn.Close()
		return
	}
	conn.SetDeadline(time.Time{})
	s.stats.total.Add(1)
	s.stats.active.Add(1)
	defer s.stats.active.Add(-1)
	c.sup.ConnStart()
	defer c.sup.ConnEnd()

	ctx, cancel := context.WithTimeout(context.Background(), c.openTimeout())
	st, stack, err := c.open(ctx, proto.Request{Op: proto.OpDial, Target: target, Prio: s.cfg.Priority}, s.class)
	cancel()
	if err != nil {
		c.log.Debug("socks connection failed", "target", target, "err", err)
		socksReply(conn, socksReplyCode(err))
		conn.Close()
		return
	}
	if err := socksReply(conn, socksSucceeded); err != nil {
		stack.Sched.Reset(st.StreamID())
		st.Close()
		conn.Close()
		return
	}
	stack.Proxy(conn, st, s.stats.count)
}

// reverse is a reverse forward: the agent listens, we connect locally.
type reverse struct {
	cfg   config.Reverse
	class sched.Class
	stats counters

	mu    sync.Mutex
	state string // pending, listening, error
	err   string
}

func (r *reverse) set(state, err string) {
	r.mu.Lock()
	r.state, r.err = state, err
	r.mu.Unlock()
}

func (c *Client) addReverse(r config.Reverse) error {
	config.DefaultReverse(&r)
	if errs := config.ValidateReverse(&r); len(errs) > 0 {
		return errors.Join(errs...)
	}
	c.mu.Lock()
	if _, ok := c.reverses[r.Name]; ok {
		c.mu.Unlock()
		return fmt.Errorf("reverse %q already exists", r.Name)
	}
	rv := &reverse{cfg: r, class: sched.ParseClass(r.Priority), state: "pending"}
	c.reverses[r.Name] = rv
	c.mu.Unlock()
	if st := c.sup.currentStack(); st != nil {
		go c.registerReverse(st, rv)
	}
	return nil
}

// onSession runs for every new session: it registers the reverse forwards
// and serves streams the agent opens towards us.
func (c *Client) onSession(st *tunnel.Stack) {
	c.mu.Lock()
	var rs []*reverse
	for _, r := range c.reverses {
		rs = append(rs, r)
	}
	c.mu.Unlock()
	for _, r := range rs {
		go c.registerReverse(st, r)
	}
	for {
		s, err := st.Mux.AcceptStream()
		if err != nil {
			return
		}
		go c.handleIncoming(st, s)
	}
}

func (c *Client) registerReverse(st *tunnel.Stack, r *reverse) {
	r.set("pending", "")
	ctx, cancel := context.WithTimeout(context.Background(), c.openTimeout())
	defer cancel()
	s, err := st.Request(ctx, proto.Request{Op: proto.OpListen, Name: r.cfg.Name, Addr: r.cfg.RemoteListen, Prio: r.cfg.Priority}, sched.ClassInteractive)
	if err != nil {
		c.log.Error("reverse listen failed", "reverse", r.cfg.Name, "remote_listen", r.cfg.RemoteListen, "err", err)
		r.set("error", err.Error())
		return
	}
	st.Sched.Forget(s.StreamID())
	s.Close()
	r.set("listening", "")
	c.log.Info("reverse forward ready", "reverse", r.cfg.Name, "remote_listen", r.cfg.RemoteListen, "target", r.cfg.Target)
}

func (c *Client) unregisterReverse(st *tunnel.Stack, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.openTimeout())
	defer cancel()
	if s, err := st.Request(ctx, proto.Request{Op: proto.OpUnlisten, Name: name}, sched.ClassInteractive); err == nil {
		st.Sched.Forget(s.StreamID())
		s.Close()
	}
}

func (c *Client) handleIncoming(st *tunnel.Stack, s *yamux.Stream) {
	s.SetReadDeadline(time.Now().Add(time.Minute))
	var req proto.Request
	if err := proto.ReadMsg(s, &req); err != nil {
		st.Sched.Reset(s.StreamID())
		s.Close()
		return
	}
	s.SetReadDeadline(time.Time{})
	c.mu.Lock()
	r := c.reverses[req.Name]
	c.mu.Unlock()
	if req.Op != proto.OpReverse || r == nil {
		tunnel.Reply(s, &proto.RemoteError{Code: proto.CodeError, Msg: "no such reverse forward: " + req.Name})
		st.Sched.Forget(s.StreamID())
		s.Close()
		return
	}
	st.Sched.SetClass(s.StreamID(), r.class)
	conn, err := net.DialTimeout("tcp", r.cfg.Target, 10*time.Second)
	if err != nil {
		c.log.Warn("reverse target unreachable", "reverse", r.cfg.Name, "target", r.cfg.Target, "err", err)
		tunnel.Reply(s, err)
		st.Sched.Forget(s.StreamID())
		s.Close()
		return
	}
	if err := tunnel.Reply(s, nil); err != nil {
		conn.Close()
		st.Sched.Reset(s.StreamID())
		s.Close()
		return
	}
	r.stats.total.Add(1)
	r.stats.active.Add(1)
	defer r.stats.active.Add(-1)
	c.sup.ConnStart()
	defer c.sup.ConnEnd()
	// As for forwards, bytes leaving this machine (local target → agent) count as "up".
	st.Proxy(conn, s, r.stats.count)
}

// AddForward adds a forward at runtime.
func (c *Client) AddForward(f config.Forward) error {
	if err := c.addForward(f, true); err != nil {
		return err
	}
	c.updateAlwaysUp()
	return nil
}

// RemoveForward closes a forward's listener. Open connections continue.
func (c *Client) RemoveForward(name string) error {
	c.mu.Lock()
	f, ok := c.forwards[name]
	delete(c.forwards, name)
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("no forward named %q", name)
	}
	f.stop()
	c.updateAlwaysUp()
	c.log.Info("forward removed", "forward", name)
	return nil
}

// AddSocks adds a SOCKS proxy at runtime.
func (c *Client) AddSocks(s config.Socks) error { return c.addSocks(s, true) }

// RemoveSocks closes a SOCKS proxy.
func (c *Client) RemoveSocks(name string) error {
	c.mu.Lock()
	s, ok := c.socks[name]
	delete(c.socks, name)
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("no socks proxy named %q", name)
	}
	s.stop()
	c.log.Info("socks proxy removed", "socks", name)
	return nil
}

// AddReverse adds a reverse forward at runtime.
func (c *Client) AddReverse(r config.Reverse) error {
	if err := c.addReverse(r); err != nil {
		return err
	}
	c.updateAlwaysUp()
	return nil
}

// RemoveReverse closes a reverse forward on the agent.
func (c *Client) RemoveReverse(name string) error {
	c.mu.Lock()
	_, ok := c.reverses[name]
	delete(c.reverses, name)
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("no reverse forward named %q", name)
	}
	if st := c.sup.currentStack(); st != nil {
		go c.unregisterReverse(st, name)
	}
	c.updateAlwaysUp()
	c.log.Info("reverse forward removed", "reverse", name)
	return nil
}

// Apply reconciles the running listeners with a new configuration. Settings
// of the ssh and connection sections need a restart and are reported.
func (c *Client) Apply(cfg *config.Config) []error {
	var errs []error
	c.mu.Lock()
	old := c.cfg
	c.cfg = cfg
	c.mu.Unlock()
	if !reflect.DeepEqual(old.SSH, cfg.SSH) || !reflect.DeepEqual(old.Connection, cfg.Connection) || old.Daemon != cfg.Daemon {
		errs = append(errs, errors.New("ssh, connection and daemon settings changed: restart gotunnel to apply them"))
	}

	wantF := map[string]config.Forward{}
	for _, f := range cfg.Forwards {
		wantF[f.Name] = f
	}
	c.mu.Lock()
	var dropF []string
	for name, fw := range c.forwards {
		if w, ok := wantF[name]; !ok || w != fw.cfg {
			dropF = append(dropF, name)
		} else {
			delete(wantF, name)
		}
	}
	c.mu.Unlock()
	for _, name := range dropF {
		c.RemoveForward(name)
	}

	wantS := map[string]config.Socks{}
	for _, s := range cfg.Socks {
		wantS[s.Name] = s
	}
	c.mu.Lock()
	var dropS []string
	for name, sp := range c.socks {
		if w, ok := wantS[name]; !ok || w != sp.cfg {
			dropS = append(dropS, name)
		} else {
			delete(wantS, name)
		}
	}
	c.mu.Unlock()
	for _, name := range dropS {
		c.RemoveSocks(name)
	}

	wantR := map[string]config.Reverse{}
	for _, r := range cfg.Reverse {
		wantR[r.Name] = r
	}
	c.mu.Lock()
	var dropR []string
	for name, rv := range c.reverses {
		if w, ok := wantR[name]; !ok || w != rv.cfg {
			dropR = append(dropR, name)
		} else {
			delete(wantR, name)
		}
	}
	c.mu.Unlock()
	for _, name := range dropR {
		c.RemoveReverse(name)
	}

	// Add in configuration order.
	for _, f := range cfg.Forwards {
		if _, ok := wantF[f.Name]; ok {
			if err := c.addForward(f, false); err != nil {
				errs = append(errs, err)
			}
		}
	}
	for _, s := range cfg.Socks {
		if _, ok := wantS[s.Name]; ok {
			if err := c.addSocks(s, false); err != nil {
				errs = append(errs, err)
			}
		}
	}
	for _, r := range cfg.Reverse {
		if _, ok := wantR[r.Name]; ok {
			if err := c.addReverse(r); err != nil {
				errs = append(errs, err)
			}
		}
	}
	c.updateAlwaysUp()
	c.log.Info("configuration applied", "forwards", len(cfg.Forwards), "socks", len(cfg.Socks), "reverse", len(cfg.Reverse))
	return errs
}

// Config returns the configuration in use.
func (c *Client) Config() *config.Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg
}

// Status is the client daemon status served on the control socket.
type Status struct {
	Version   string          `json:"version"`
	PID       int             `json:"pid"`
	Started   time.Time       `json:"started"`
	Managed   string          `json:"managed"`
	Config    string          `json:"config"`
	Transport TransportStatus `json:"transport"`
	Forwards  []ListenStatus  `json:"forwards"`
	Socks     []ListenStatus  `json:"socks"`
	Reverse   []ReverseStatus `json:"reverse"`
}

// ListenStatus describes a forward or SOCKS listener.
type ListenStatus struct {
	Name      string `json:"name"`
	Listen    string `json:"listen"`
	Target    string `json:"target,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Priority  string `json:"priority"`
	Listening bool   `json:"listening"`
	Error     string `json:"error,omitempty"`
	Active    int64  `json:"active"`
	Total     int64  `json:"total"`
	BytesUp   uint64 `json:"bytes_up"`
	BytesDown uint64 `json:"bytes_down"`
}

// ReverseStatus describes a reverse forward.
type ReverseStatus struct {
	Name         string `json:"name"`
	RemoteListen string `json:"remote_listen"`
	Target       string `json:"target"`
	Priority     string `json:"priority"`
	State        string `json:"state"`
	Error        string `json:"error,omitempty"`
	Active       int64  `json:"active"`
	Total        int64  `json:"total"`
	BytesUp      uint64 `json:"bytes_up"`
	BytesDown    uint64 `json:"bytes_down"`
}

// Status returns a snapshot of the daemon.
func (c *Client) Status() Status {
	c.mu.Lock()
	st := Status{
		Version: version.Version,
		PID:     os.Getpid(),
		Started: c.started,
		Managed: daemon.ManagedBy(),
		Config:  c.cfg.Path,
	}
	for _, f := range c.forwards {
		ok, err := f.ln.status()
		st.Forwards = append(st.Forwards, ListenStatus{
			Name: f.cfg.Name, Listen: f.cfg.Listen, Target: f.cfg.Target, Mode: f.cfg.Mode, Priority: f.cfg.Priority,
			Listening: ok, Error: err, Active: f.stats.active.Load(), Total: f.stats.total.Load(),
			BytesUp: f.stats.up.Load(), BytesDown: f.stats.down.Load(),
		})
	}
	for _, s := range c.socks {
		ok, err := s.ln.status()
		st.Socks = append(st.Socks, ListenStatus{
			Name: s.cfg.Name, Listen: s.cfg.Listen, Priority: s.cfg.Priority,
			Listening: ok, Error: err, Active: s.stats.active.Load(), Total: s.stats.total.Load(),
			BytesUp: s.stats.up.Load(), BytesDown: s.stats.down.Load(),
		})
	}
	for _, r := range c.reverses {
		r.mu.Lock()
		st.Reverse = append(st.Reverse, ReverseStatus{
			Name: r.cfg.Name, RemoteListen: r.cfg.RemoteListen, Target: r.cfg.Target, Priority: r.cfg.Priority,
			State: r.state, Error: r.err, Active: r.stats.active.Load(), Total: r.stats.total.Load(),
			BytesUp: r.stats.up.Load(), BytesDown: r.stats.down.Load(),
		})
		r.mu.Unlock()
	}
	c.mu.Unlock()
	sort.Slice(st.Forwards, func(i, j int) bool { return st.Forwards[i].Name < st.Forwards[j].Name })
	sort.Slice(st.Socks, func(i, j int) bool { return st.Socks[i].Name < st.Socks[j].Name })
	sort.Slice(st.Reverse, func(i, j int) bool { return st.Reverse[i].Name < st.Reverse[j].Name })
	st.Transport = c.sup.Status()
	return st
}
