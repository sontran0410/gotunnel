// Package agent implements the destination side: agentd, which owns sessions
// and connects to target services, and the relay that sshd starts for each
// link and that pipes the ssh channel to agentd.
package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"

	"gotunnel/internal/proto"
	"gotunnel/internal/sched"
	"gotunnel/internal/session"
	"gotunnel/internal/tunnel"
	"gotunnel/internal/version"
)

// Options configures agentd.
type Options struct {
	// IdleExit makes agentd exit after having no sessions for this long
	// (0: never). Used when a relay starts agentd on demand.
	IdleExit time.Duration
	// DialTimeout bounds connecting to target services.
	DialTimeout time.Duration
	// Managed tells how agentd was started (shown in status).
	Managed string
	Logger  *slog.Logger
}

// Server is agentd.
type Server struct {
	opts    Options
	log     *slog.Logger
	started time.Time

	mu          sync.Mutex
	sessions    map[session.ID]*agentSession
	handshaking int
	lastActive  time.Time
	listener    net.Listener

	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// NewServer creates agentd.
func NewServer(opts Options) *Server {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 10 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	return &Server{
		opts:       opts,
		log:        opts.Logger,
		started:    time.Now(),
		sessions:   map[session.ID]*agentSession{},
		lastActive: time.Now(),
		shutdown:   make(chan struct{}),
	}
}

// Serve accepts relay and admin connections until Shutdown.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()
	go s.janitor()
	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return nil
			default:
			}
			return err
		}
		go s.HandleConn(c)
	}
}

// Done is closed when the server shuts down.
func (s *Server) Done() <-chan struct{} { return s.shutdown }

// Shutdown closes all sessions (telling attached clients) and stops serving.
func (s *Server) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdown)
		s.mu.Lock()
		if s.listener != nil {
			s.listener.Close()
		}
		var all []*agentSession
		for _, as := range s.sessions {
			all = append(all, as)
		}
		s.mu.Unlock()
		for _, as := range all {
			as.close(true, "agent shutting down")
		}
	})
}

type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

// HandleConn serves one connection: a link starting with HELLO, or an admin
// request. After a rejected resume the client may retry on the same link.
func (s *Server) HandleConn(c io.ReadWriteCloser) {
	s.mu.Lock()
	s.handshaking++
	s.lastActive = time.Now()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.handshaking--
		s.lastActive = time.Now()
		s.mu.Unlock()
	}()

	setDeadline := func(t time.Time) {
		if d, ok := c.(readDeadliner); ok {
			d.SetReadDeadline(t)
		}
	}
	br := bufio.NewReaderSize(c, 64<<10)
	for attempt := 0; attempt < 4; attempt++ {
		setDeadline(time.Now().Add(2 * time.Minute))
		typ, payload, err := proto.ReadFrame(br, nil)
		if err != nil {
			c.Close()
			return
		}
		switch typ {
		case proto.FrameAdmin:
			s.handleAdmin(c, payload)
			c.Close()
			return
		case proto.FrameHello:
			var h proto.Hello
			if err := json.Unmarshal(payload, &h); err != nil {
				reject(c, proto.RejectBadRequest, "malformed hello")
				c.Close()
				return
			}
			setDeadline(time.Time{})
			if retry := s.handleHello(br, c, &h); !retry {
				return
			}
		default:
			c.Close()
			return
		}
	}
	c.Close()
}

func reject(w io.Writer, code, msg string) {
	_ = proto.WriteJSONFrame(w, proto.FrameReject, proto.Reject{Code: code, Message: msg})
}

// handleHello creates or resumes a session. It returns true if the client may
// send another Hello on the same link.
func (s *Server) handleHello(r *bufio.Reader, c io.ReadWriteCloser, h *proto.Hello) (retry bool) {
	if h.Proto != version.Proto {
		reject(c, proto.RejectProto, fmt.Sprintf("agent %s speaks protocol %d, client speaks %d; run 'gotunnel install-agent'",
			version.Version, version.Proto, h.Proto))
		c.Close()
		return false
	}

	if h.Session == "" {
		as, err := s.newSession(h)
		if err != nil {
			reject(c, proto.RejectBadRequest, err.Error())
			c.Close()
			return false
		}
		w := proto.Welcome{Proto: version.Proto, Version: version.Version, Session: as.id.String(), Token: as.token.String()}
		if err := proto.WriteJSONFrame(c, proto.FrameWelcome, w); err != nil {
			as.close(false, "handshake failed")
			c.Close()
			return false
		}
		if err := as.stack.Sess.Attach(r, c, c, h.Recv); err != nil {
			as.close(false, err.Error())
			c.Close()
		}
		return false
	}

	id, err1 := session.ParseID(h.Session)
	token, err2 := session.ParseToken(h.Token)
	if err1 != nil || err2 != nil {
		reject(c, proto.RejectBadRequest, "malformed session id or token")
		return true
	}
	as := s.lookup(id)
	if as == nil {
		reject(c, proto.RejectUnknownSession, "session expired or agent restarted")
		return true
	}
	if !as.token.Equal(token) {
		s.log.Warn("resume with wrong token", "session", id.Short())
		reject(c, proto.RejectBadToken, "token does not match session")
		return true
	}
	err := as.stack.Sess.Resume(r, c, c, h.Recv, func(recv uint64) error {
		return proto.WriteJSONFrame(c, proto.FrameWelcome, proto.Welcome{
			Proto: version.Proto, Version: version.Version,
			Session: as.id.String(), Token: as.token.String(),
			Recv: recv, Resumed: true,
		})
	})
	if err != nil {
		s.log.Warn("resume failed", "session", id.Short(), "err", err)
		if errors.Is(err, session.ErrBadOffset) {
			as.close(false, "inconsistent resume offset")
		}
		c.Close()
		return false
	}
	as.log.Info("session resumed")
	return false
}

func (s *Server) lookup(id session.ID) *agentSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *Server) newSession(h *proto.Hello) (*agentSession, error) {
	select {
	case <-s.shutdown:
		return nil, errors.New("agent is shutting down")
	default:
	}
	ttl := clampDur(h.TTL(), time.Minute, 24*time.Hour)
	id, token := session.NewID(), session.NewToken()
	log := s.log.With("session", id.Short())
	p := tunnel.Params{
		TTL:          ttl,
		PingInterval: clampDur(h.PingInterval(), time.Second, 5*time.Minute),
		// The client owns reconnection; we only need to notice it is gone.
		DeadAfter:   2 * clampDur(h.DeadAfter(), 5*time.Second, 30*time.Minute),
		MaxInflight: clampInt(h.MaxInflight, 256<<10, 64<<20),
		Inflight:    clampInt(h.Inflight, 8<<10, 64<<20),
		Adaptive:    h.Adaptive,
		Logger:      log,
	}
	stack, err := tunnel.NewStack(id, token, p, false)
	if err != nil {
		return nil, err
	}
	as := &agentSession{
		srv:       s,
		id:        id,
		token:     token,
		stack:     stack,
		ttl:       ttl,
		created:   time.Now(),
		log:       log,
		listeners: map[string]*revListener{},
	}
	s.mu.Lock()
	s.sessions[id] = as
	s.lastActive = time.Now()
	s.mu.Unlock()
	log.Info("session created", "client_version", h.Version, "ttl", ttl)
	go as.acceptLoop()
	return as, nil
}

func (s *Server) remove(id session.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
	s.lastActive = time.Now()
}

// janitor expires detached sessions and implements the idle exit.
func (s *Server) janitor() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.shutdown:
			return
		case <-t.C:
		}
		s.mu.Lock()
		var expired []*agentSession
		for _, as := range s.sessions {
			if since := as.stack.Sess.DetachedSince(); !since.IsZero() && time.Since(since) > as.ttl {
				expired = append(expired, as)
			}
		}
		idle := s.opts.IdleExit > 0 && len(s.sessions) == 0 && s.handshaking == 0 &&
			time.Since(s.lastActive) > s.opts.IdleExit
		s.mu.Unlock()
		for _, as := range expired {
			as.close(false, fmt.Sprintf("expired: detached for more than %s", as.ttl))
		}
		if idle {
			s.log.Info("no sessions, exiting", "idle_exit", s.opts.IdleExit)
			s.Shutdown()
			return
		}
	}
}

func (s *Server) handleAdmin(w io.Writer, payload []byte) {
	var req proto.AdminRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = proto.WriteJSONFrame(w, proto.FrameAdminResp, proto.AdminResponse{Error: "malformed request"})
		return
	}
	switch req.Cmd {
	case proto.AdminStatus:
		_ = proto.WriteJSONFrame(w, proto.FrameAdminResp, s.Status())
	case proto.AdminStop:
		resp := s.Status()
		_ = proto.WriteJSONFrame(w, proto.FrameAdminResp, resp)
		s.log.Info("stop requested")
		go s.Shutdown()
	default:
		_ = proto.WriteJSONFrame(w, proto.FrameAdminResp, proto.AdminResponse{Error: "unknown command " + req.Cmd})
	}
}

// Status describes agentd and its sessions.
func (s *Server) Status() proto.AdminResponse {
	s.mu.Lock()
	all := make([]*agentSession, 0, len(s.sessions))
	for _, as := range s.sessions {
		all = append(all, as)
	}
	s.mu.Unlock()
	resp := proto.AdminResponse{
		OK:      true,
		PID:     os.Getpid(),
		Version: version.Version,
		Started: s.started,
		Managed: s.opts.Managed,
	}
	if s.opts.IdleExit > 0 {
		resp.IdleExit = s.opts.IdleExit.String()
	}
	for _, as := range all {
		resp.Sessions = append(resp.Sessions, as.info())
	}
	sort.Slice(resp.Sessions, func(i, j int) bool { return resp.Sessions[i].Created.Before(resp.Sessions[j].Created) })
	return resp
}

// agentSession is one client session and the resources it owns.
type agentSession struct {
	srv     *Server
	id      session.ID
	token   session.Token
	stack   *tunnel.Stack
	ttl     time.Duration
	created time.Time
	log     *slog.Logger
	streams atomic.Int64

	mu        sync.Mutex
	listeners map[string]*revListener
	closeOnce sync.Once
}

type revListener struct {
	name  string
	addr  string
	ln    net.Listener
	class sched.Class
}

func (as *agentSession) info() proto.SessionInfo {
	st := as.stack.Sess.Stats()
	si := proto.SessionInfo{
		ID:       as.id.String(),
		Created:  as.created,
		Attached: st.Attached,
		Detached: st.DetachedAt,
		TTL:      as.ttl.String(),
		Streams:  int(as.streams.Load()),
		BytesIn:  st.BytesIn,
		BytesOut: st.BytesOut,
	}
	as.mu.Lock()
	for _, rl := range as.listeners {
		si.Listeners = append(si.Listeners, rl.name+"="+rl.addr)
	}
	as.mu.Unlock()
	sort.Strings(si.Listeners)
	return si
}

func (as *agentSession) acceptLoop() {
	for {
		st, err := as.stack.Mux.AcceptStream()
		if err != nil {
			reason := "session ended"
			if e := as.stack.Sess.Err(); e != nil {
				reason = e.Error()
			}
			as.close(false, reason)
			return
		}
		go as.handleStream(st)
	}
}

func (as *agentSession) handleStream(st *yamux.Stream) {
	st.SetReadDeadline(time.Now().Add(5 * time.Minute))
	var req proto.Request
	if err := proto.ReadMsg(st, &req); err != nil {
		as.stack.Sched.Reset(st.StreamID())
		st.Close()
		return
	}
	st.SetReadDeadline(time.Time{})
	switch req.Op {
	case proto.OpDial:
		as.stack.Sched.SetClass(st.StreamID(), sched.ParseClass(req.Prio))
		ctx, cancel := context.WithTimeout(context.Background(), as.srv.opts.DialTimeout)
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", req.Target)
		cancel()
		if err != nil {
			as.log.Debug("dial failed", "target", req.Target, "err", err)
			tunnel.Reply(st, err)
			as.stack.Sched.Forget(st.StreamID())
			st.Close()
			return
		}
		if err := tunnel.Reply(st, nil); err != nil {
			conn.Close()
			as.stack.Sched.Reset(st.StreamID())
			st.Close()
			return
		}
		as.streams.Add(1)
		defer as.streams.Add(-1)
		as.stack.Proxy(conn, st, nil)
	case proto.OpListen:
		err := as.listen(req.Name, req.Addr, sched.ParseClass(req.Prio))
		tunnel.Reply(st, err)
		as.stack.Sched.Forget(st.StreamID())
		st.Close()
	case proto.OpUnlisten:
		as.unlisten(req.Name)
		tunnel.Reply(st, nil)
		as.stack.Sched.Forget(st.StreamID())
		st.Close()
	default:
		tunnel.Reply(st, &proto.RemoteError{Code: proto.CodeError, Msg: "unknown operation " + req.Op})
		as.stack.Sched.Forget(st.StreamID())
		st.Close()
	}
}

func (as *agentSession) listen(name, addr string, class sched.Class) error {
	as.mu.Lock()
	if old := as.listeners[name]; old != nil {
		old.ln.Close()
		delete(as.listeners, name)
	}
	as.mu.Unlock()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		as.log.Warn("reverse listen failed", "name", name, "addr", addr, "err", err)
		return err
	}
	rl := &revListener{name: name, addr: addr, ln: ln, class: class}
	as.mu.Lock()
	if as.listeners == nil { // session closed meanwhile
		as.mu.Unlock()
		ln.Close()
		return errors.New("session closed")
	}
	as.listeners[name] = rl
	as.mu.Unlock()
	as.log.Info("reverse listener open", "name", name, "addr", addr)
	go as.acceptReverse(rl)
	return nil
}

func (as *agentSession) unlisten(name string) {
	as.mu.Lock()
	defer as.mu.Unlock()
	if rl := as.listeners[name]; rl != nil {
		rl.ln.Close()
		delete(as.listeners, name)
		as.log.Info("reverse listener closed", "name", name)
	}
}

func (as *agentSession) acceptReverse(rl *revListener) {
	for {
		conn, err := rl.ln.Accept()
		if err != nil {
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			st, err := as.stack.Request(ctx, proto.Request{Op: proto.OpReverse, Name: rl.name}, rl.class)
			cancel()
			if err != nil {
				as.log.Debug("reverse connection refused by client", "name", rl.name, "err", err)
				conn.Close()
				return
			}
			as.streams.Add(1)
			defer as.streams.Add(-1)
			as.stack.Proxy(conn, st, nil)
		}()
	}
}

// close releases the session. graceful tells an attached client (BYE).
func (as *agentSession) close(graceful bool, reason string) {
	as.closeOnce.Do(func() {
		as.log.Info("session closed", "reason", reason)
		if graceful {
			as.stack.Close()
		} else {
			as.stack.Abort(session.ErrClosed)
		}
		as.mu.Lock()
		for _, rl := range as.listeners {
			rl.ln.Close()
		}
		as.listeners = nil
		as.mu.Unlock()
		as.srv.remove(as.id)
	})
}

func clampDur(v, lo, hi time.Duration) time.Duration { return max(lo, min(v, hi)) }
func clampInt(v, lo, hi int) int                     { return max(lo, min(v, hi)) }
