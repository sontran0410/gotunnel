// Package client implements the client daemon: local listeners (forwards,
// SOCKS, reverse targets) and the supervisor that keeps one session to the
// agent alive over an expensive transport.
package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/sontran0410/gotunnel/internal/proto"
	"github.com/sontran0410/gotunnel/internal/session"
	"github.com/sontran0410/gotunnel/internal/transport"
	"github.com/sontran0410/gotunnel/internal/tunnel"
	"github.com/sontran0410/gotunnel/internal/version"
)

// State of the transport.
type State string

// Supervisor states.
const (
	StateIdle         State = "idle"
	StateConnecting   State = "connecting"
	StateConnected    State = "connected"
	StateReconnecting State = "reconnecting"
	StateStopped      State = "stopped"
)

var errReconnectRequested = errors.New("reconnect requested")

// Supervisor owns the transport and the session. It brings the transport up
// when it is wanted (always-on forwards, active or recent connections), keeps
// it up, resumes the session after link failures and closes it when idle.
//
//	           wanted                   handshake ok
//	idle ───────────────▶ connecting ─────────────────▶ connected
//	 ▲                         │ ▲ failure: backoff 1s→60s   │
//	 │ not wanted              ▼ │                           │ link lost
//	 └──────────────────── reconnecting ◀─────────────────────┘
//	   (session closed       (session kept: the agent resumes it,
//	    with BYE)             or rejects it and a new one starts)
//
// Run is the only goroutine that changes the transport; other goroutines get
// the current session through Stack, which also marks the transport wanted.
type Supervisor struct {
	tr             transport.Transport
	params         tunnel.Params
	connectTimeout time.Duration
	idleTimeout    time.Duration
	log            *slog.Logger
	onSession      func(*tunnel.Stack)

	kick        chan struct{}
	reconnectCh chan struct{}

	mu           sync.Mutex
	state        State
	since        time.Time
	changed      chan struct{} // closed and replaced on every change
	stack        *tunnel.Stack
	alwaysUp     bool
	active       int
	lastActive   time.Time
	lastErr      string
	linkLosses   int
	resumes      int
	sessions     int
	agentVersion string
	warnedVer    bool
}

// NewSupervisor creates a supervisor. onSession is called (in its own
// goroutine) for every newly created session.
func NewSupervisor(tr transport.Transport, p tunnel.Params, connectTimeout, idleTimeout time.Duration,
	log *slog.Logger, onSession func(*tunnel.Stack)) *Supervisor {
	return &Supervisor{
		tr:             tr,
		params:         p,
		connectTimeout: connectTimeout,
		idleTimeout:    idleTimeout,
		log:            log,
		onSession:      onSession,
		kick:           make(chan struct{}, 1),
		reconnectCh:    make(chan struct{}, 1),
		state:          StateIdle,
		since:          time.Now(),
		changed:        make(chan struct{}),
	}
}

// SetAlwaysUp sets whether the transport must stay up permanently.
func (s *Supervisor) SetAlwaysUp(v bool) {
	s.mu.Lock()
	s.alwaysUp = v
	s.mu.Unlock()
	s.poke()
}

// ConnStart and ConnEnd track proxied connections for the idle timeout.
func (s *Supervisor) ConnStart() {
	s.mu.Lock()
	s.active++
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// ConnEnd marks the end of a proxied connection.
func (s *Supervisor) ConnEnd() {
	s.mu.Lock()
	s.active--
	s.lastActive = time.Now()
	s.mu.Unlock()
	s.poke()
}

// Reconnect drops the current link; the session is resumed on a new one.
func (s *Supervisor) Reconnect() {
	select {
	case s.reconnectCh <- struct{}{}:
	default:
	}
}

func (s *Supervisor) poke() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *Supervisor) wantUp() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alwaysUp || s.active > 0 || time.Since(s.lastActive) < s.idleTimeout
}

func (s *Supervisor) setState(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == st {
		return
	}
	s.state = st
	s.since = time.Now()
	s.notifyLocked()
}

func (s *Supervisor) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Supervisor) currentStack() *tunnel.Stack {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stack
}

// Stack returns the current session, bringing the transport up if needed and
// waiting until a session exists or ctx ends. A session whose link is down is
// returned as is: streams opened on it are delivered after it resumes.
func (s *Supervisor) Stack(ctx context.Context) (*tunnel.Stack, error) {
	for {
		s.mu.Lock()
		if s.stack != nil {
			st := s.stack
			s.mu.Unlock()
			return st, nil
		}
		if s.state == StateStopped {
			s.mu.Unlock()
			return nil, errors.New("client is shutting down")
		}
		s.lastActive = time.Now()
		ch, lastErr := s.changed, s.lastErr
		s.mu.Unlock()
		s.poke()
		select {
		case <-ch:
		case <-ctx.Done():
			if lastErr != "" {
				return nil, fmt.Errorf("transport not ready (%v); last error: %s", ctx.Err(), lastErr)
			}
			return nil, fmt.Errorf("transport not ready: %w", ctx.Err())
		}
	}
}

// Run keeps the transport in the wanted state until ctx ends, then closes
// the session gracefully.
func (s *Supervisor) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			s.dropSession(true)
			s.setState(StateStopped)
			return
		}
		if !s.wantUp() {
			if s.currentStack() != nil {
				s.log.Info("idle: closing session")
				s.dropSession(true)
			}
			s.setState(StateIdle)
			select {
			case <-ctx.Done():
			case <-s.kick:
			case <-s.reconnectCh:
			}
			continue
		}

		if s.currentStack() != nil {
			s.setState(StateReconnecting)
		} else {
			s.setState(StateConnecting)
		}
		start := time.Now()
		if err := s.connect(ctx); err != nil {
			if ctx.Err() != nil {
				continue
			}
			s.mu.Lock()
			s.lastErr = err.Error()
			s.notifyLocked()
			s.mu.Unlock()
			wait := backoff/2 + rand.N(backoff/2+1)
			s.log.Warn("connect failed", "err", err, "retry_in", wait.Round(100*time.Millisecond))
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			case <-s.reconnectCh:
			}
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = time.Second
		s.log.Info("connected", "took", time.Since(start).Round(time.Millisecond))
		s.setState(StateConnected)
		s.waitConnected(ctx)
	}
}

// waitConnected returns when the link drops, the session ends, the
// transport should go idle, or ctx ends.
func (s *Supervisor) waitConnected(ctx context.Context) {
	st := s.currentStack()
	if st == nil {
		return
	}
	linkDone := st.Sess.LinkDone()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-linkDone:
			s.mu.Lock()
			s.linkLosses++
			s.mu.Unlock()
			if st.Sess.Err() != nil {
				s.log.Warn("session ended", "err", st.Sess.Err())
				s.dropSession(false)
			} else {
				s.log.Warn("link lost; resuming", "err", st.Sess.Stats().DetachErr)
			}
			return
		case <-s.reconnectCh:
			s.log.Info("reconnect requested")
			st.Sess.DetachLink(errReconnectRequested)
		case <-tick.C:
			if !s.wantUp() {
				return
			}
		case <-s.kick:
			if !s.wantUp() {
				return
			}
		}
	}
}

// connect opens a link, performs the handshake and attaches the session.
func (s *Supervisor) connect(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.connectTimeout)
	defer cancel()
	link, err := s.tr.Dial(ctx)
	if err != nil {
		return err
	}
	attached := false
	stop := context.AfterFunc(ctx, func() { link.Close() })
	defer func() {
		stop()
		if !attached {
			link.Close()
		}
	}()
	fail := func(err error) error {
		if ctx.Err() != nil {
			err = fmt.Errorf("no answer from agent within %s", s.connectTimeout)
		}
		if d, ok := link.(transport.Diagnoser); ok {
			if diag := d.Diagnose(); diag != "" {
				return fmt.Errorf("%w (%s)", err, diag)
			}
		}
		return err
	}

	br := bufio.NewReaderSize(link, 64<<10)
	skipped, err := proto.ReadPreamble(br, 64<<10)
	if len(skipped) > 0 {
		s.log.Debug("remote printed text before the agent started", "text", truncate(string(skipped), 200))
	}
	if err != nil {
		return fail(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		hello := s.hello()
		if err := proto.WriteJSONFrame(link, proto.FrameHello, hello); err != nil {
			return fail(err)
		}
		typ, payload, err := proto.ReadFrame(br, nil)
		if err != nil {
			return fail(err)
		}
		switch typ {
		case proto.FrameReject:
			var rj proto.Reject
			_ = json.Unmarshal(payload, &rj)
			// The agent forgot our session (expired or restarted). Start a new
			// one on the same link instead of paying for another ssh setup.
			if hello.Session != "" && (rj.Code == proto.RejectUnknownSession || rj.Code == proto.RejectBadToken) {
				s.log.Warn("session could not be resumed; open connections are lost", "reason", rj.Message)
				s.dropSession(false)
				continue
			}
			return &rj
		case proto.FrameWelcome:
			var w proto.Welcome
			if err := json.Unmarshal(payload, &w); err != nil {
				return fail(err)
			}
			if !stop() {
				return fail(ctx.Err())
			}
			if err := s.attach(br, link, &w); err != nil {
				return err
			}
			attached = true
			return nil
		default:
			return fail(fmt.Errorf("unexpected frame 0x%02x during handshake", typ))
		}
	}
	return errors.New("handshake failed")
}

func (s *Supervisor) hello() proto.Hello {
	h := proto.Hello{
		Proto:           version.Proto,
		Version:         version.Version,
		TTLMillis:       s.params.TTL.Milliseconds(),
		PingMillis:      s.params.PingInterval.Milliseconds(),
		DeadAfterMillis: s.params.DeadAfter.Milliseconds(),
		MaxInflight:     s.params.MaxInflight,
		Inflight:        s.params.Inflight,
		Adaptive:        s.params.Adaptive,
	}
	if st := s.currentStack(); st != nil {
		h.Session = st.Sess.ID().String()
		h.Token = st.Sess.Token().String()
		h.Recv = st.Sess.RecvOffset()
	}
	return h
}

func (s *Supervisor) attach(br *bufio.Reader, link transport.Link, w *proto.Welcome) error {
	if w.Resumed {
		st := s.currentStack()
		if st == nil {
			return errors.New("agent resumed a session we do not have")
		}
		if err := st.Sess.Attach(br, link, link, w.Recv); err != nil {
			s.dropSession(false)
			return err
		}
		s.mu.Lock()
		s.resumes++
		s.mu.Unlock()
		s.log.Info("session resumed", "session", st.Sess.ID().Short())
	} else {
		s.dropSession(false)
		id, err1 := session.ParseID(w.Session)
		token, err2 := session.ParseToken(w.Token)
		if err1 != nil || err2 != nil {
			return errors.New("agent sent a malformed session id or token")
		}
		p := s.params
		p.Logger = s.log.With("session", id.Short())
		st, err := tunnel.NewStack(id, token, p, true)
		if err != nil {
			return err
		}
		if err := st.Sess.Attach(br, link, link, w.Recv); err != nil {
			st.Abort(err)
			return err
		}
		s.mu.Lock()
		s.stack = st
		s.sessions++
		s.mu.Unlock()
		s.log.Info("session established", "session", id.Short(), "agent_version", w.Version)
		if s.onSession != nil {
			go s.onSession(st)
		}
	}
	s.mu.Lock()
	s.agentVersion = w.Version
	s.lastErr = ""
	if w.Version != version.Version && !s.warnedVer {
		s.warnedVer = true
		s.log.Warn("agent version differs from client; run 'gotunnel install-agent' to update it",
			"agent", w.Version, "client", version.Version)
	}
	s.notifyLocked()
	s.mu.Unlock()
	return nil
}

// dropSession forgets the current session. graceful tells the agent (BYE).
func (s *Supervisor) dropSession(graceful bool) {
	s.mu.Lock()
	st := s.stack
	s.stack = nil
	if st != nil {
		s.notifyLocked()
	}
	s.mu.Unlock()
	if st == nil {
		return
	}
	if graceful {
		st.Close()
	} else {
		st.Abort(session.ErrClosed)
	}
}

// TransportStatus describes the transport and session.
type TransportStatus struct {
	State         State     `json:"state"`
	Target        string    `json:"target"`
	Since         time.Time `json:"since"`
	LastError     string    `json:"last_error,omitempty"`
	LinkLosses    int       `json:"link_losses"`
	Resumes       int       `json:"resumes"`
	Sessions      int       `json:"sessions"`
	AgentVersion  string    `json:"agent_version,omitempty"`
	Session       string    `json:"session,omitempty"`
	RTTMillis     float64   `json:"rtt_ms"`
	MinRTTMillis  float64   `json:"min_rtt_ms"`
	Inflight      int       `json:"inflight"`
	InflightLimit int       `json:"inflight_limit"`
	Queued        int       `json:"queued"`
	BytesIn       uint64    `json:"bytes_in"`
	BytesOut      uint64    `json:"bytes_out"`
	ActiveConns   int       `json:"active_conns"`
}

// Status returns a snapshot of the transport state.
func (s *Supervisor) Status() TransportStatus {
	s.mu.Lock()
	ts := TransportStatus{
		State:        s.state,
		Target:       s.tr.String(),
		Since:        s.since,
		LastError:    s.lastErr,
		LinkLosses:   s.linkLosses,
		Resumes:      s.resumes,
		Sessions:     s.sessions,
		AgentVersion: s.agentVersion,
		ActiveConns:  s.active,
	}
	st := s.stack
	s.mu.Unlock()
	if st != nil {
		ss := st.Sess.Stats()
		ts.Session = st.Sess.ID().Short()
		ts.RTTMillis = float64(ss.RTT.Microseconds()) / 1000
		ts.MinRTTMillis = float64(ss.MinRTT.Microseconds()) / 1000
		ts.Inflight = ss.Unacked
		ts.InflightLimit = ss.Limit
		ts.Queued = st.Sched.Queued()
		ts.BytesIn = ss.BytesIn
		ts.BytesOut = ss.BytesOut
	}
	return ts
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
