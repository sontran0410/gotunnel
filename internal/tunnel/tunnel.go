// Package tunnel assembles the protocol stack shared by client and agent:
//
//	yamux streams → sched.Conn (priorities, in-flight control) → session.Session (resume) → link
package tunnel

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"syscall"
	"time"

	"github.com/hashicorp/yamux"

	"gotunnel/internal/netutil"
	"gotunnel/internal/proto"
	"gotunnel/internal/sched"
	"gotunnel/internal/session"
)

// Params configures one end of a session.
type Params struct {
	TTL          time.Duration // how long the agent keeps a detached session
	PingInterval time.Duration
	DeadAfter    time.Duration
	MaxInflight  int  // upper bound of bytes in flight (replay buffer size)
	Inflight     int  // initial in-flight limit
	Adaptive     bool // tune the in-flight limit from RTT samples
	Logger       *slog.Logger
}

// minInflight is the smallest in-flight limit the controller may choose.
const minInflight = 8 << 10

// Stack is a session with its scheduler and yamux session.
type Stack struct {
	Sess  *session.Session
	Sched *sched.Conn
	Ctl   *sched.Controller
	Mux   *yamux.Session
}

// NewStack builds the stack for a session. client selects the yamux role.
func NewStack(id session.ID, token session.Token, p Params, client bool) (*Stack, error) {
	maxInflight := max(p.MaxInflight, minInflight)
	// Headroom covers acknowledgement granularity (one ack per 4KiB) and the
	// frame being transmitted.
	ctl := sched.NewController(p.Inflight, minInflight, maxInflight, 8<<10, p.Adaptive)
	sess := session.New(id, token, session.Options{
		SendLimit:    ctl.Limit(),
		PingInterval: p.PingInterval,
		DeadAfter:    p.DeadAfter,
		WakeProbe:    10 * time.Second,
		OnSample:     func(s session.Sample) { ctl.Observe(s.RTT, s.Inflight, s.DeliveryRate) },
		Logger:       p.Logger,
	})
	ctl.Bind(sess)
	sc := sched.New(sess, sched.DefaultChunkSize)
	cfg := YamuxConfig(p.TTL)
	var mux *yamux.Session
	var err error
	if client {
		mux, err = yamux.Client(sc, cfg)
	} else {
		mux, err = yamux.Server(sc, cfg)
	}
	if err != nil {
		sess.Abort(err)
		return nil, err
	}
	return &Stack{Sess: sess, Sched: sc, Ctl: ctl, Mux: mux}, nil
}

// Close ends the session, notifying the peer if a link is attached.
func (s *Stack) Close() {
	s.Sess.Close()
	s.Mux.Close()
}

// Abort ends the session without notifying the peer.
func (s *Stack) Abort(err error) {
	s.Sess.Abort(err)
	s.Mux.Close()
}

// YamuxConfig returns a yamux configuration suitable for running over a
// resumable session: yamux must never give up on the connection while the
// link is down, so its keepalive and stream-open timeout are disabled and its
// write timeouts outlast the session TTL.
func YamuxConfig(ttl time.Duration) *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.StreamOpenTimeout = 0
	cfg.ConnectionWriteTimeout = ttl + 5*time.Minute
	cfg.StreamCloseTimeout = max(ttl, 5*time.Minute)
	cfg.LogOutput = io.Discard
	return cfg
}

// Stream adapts a yamux stream for netutil.Splice: yamux's Close is a
// half-close, which is what CloseWrite means.
type Stream struct{ *yamux.Stream }

// CloseWrite sends FIN while keeping the stream readable.
func (s Stream) CloseWrite() error { return s.Stream.Close() }

// Proxy copies between a local connection and a stream until both directions
// end. If the transfer fails the peer is told to reset the stream.
func (s *Stack) Proxy(local net.Conn, st *yamux.Stream, count func(up bool, n int)) error {
	err := netutil.Splice(local, Stream{st}, count)
	if err != nil {
		s.Sched.Reset(st.StreamID())
	} else {
		s.Sched.Forget(st.StreamID())
	}
	return err
}

// OpenStream opens a stream, giving up when ctx ends. (yamux's OpenStream can
// block while the link is down and the send buffer is full.)
func OpenStream(ctx context.Context, mux *yamux.Session) (*yamux.Stream, error) {
	type result struct {
		st  *yamux.Stream
		err error
	}
	ch := make(chan result, 1)
	go func() {
		st, err := mux.OpenStream()
		ch <- result{st, err}
	}()
	select {
	case r := <-ch:
		return r.st, r.err
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.st != nil {
				r.st.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// Request opens a stream, sends req and waits for the response. On success
// the stream is ready for data.
func (s *Stack) Request(ctx context.Context, req proto.Request, class sched.Class) (*yamux.Stream, error) {
	st, err := OpenStream(ctx, s.Mux)
	if err != nil {
		return nil, err
	}
	s.Sched.SetClass(st.StreamID(), class)
	if dl, ok := ctx.Deadline(); ok {
		st.SetDeadline(dl)
	}
	stop := context.AfterFunc(ctx, func() { st.SetDeadline(time.Now()) })
	defer stop()
	if err := proto.WriteMsg(st, req); err != nil {
		s.Sched.Reset(st.StreamID())
		st.Close()
		return nil, err
	}
	var resp proto.Response
	if err := proto.ReadMsg(st, &resp); err != nil {
		s.Sched.Reset(st.StreamID())
		st.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if err := resp.Err(); err != nil {
		s.Sched.Forget(st.StreamID())
		st.Close()
		return nil, err
	}
	st.SetDeadline(time.Time{})
	return st, nil
}

// Reply writes a response on a stream.
func Reply(st *yamux.Stream, err error) error {
	if err == nil {
		return proto.WriteMsg(st, proto.Response{OK: true})
	}
	return proto.WriteMsg(st, proto.Response{Code: DialErrorCode(err), Error: err.Error()})
}

// DialErrorCode classifies a dial error for the peer.
func DialErrorCode(err error) string {
	var dnsErr *net.DNSError
	var netErr net.Error
	var remote *proto.RemoteError
	switch {
	case errors.As(err, &remote):
		return remote.Code
	case errors.As(err, &dnsErr):
		return proto.CodeDNS
	case errors.Is(err, syscall.ECONNREFUSED):
		return proto.CodeRefused
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return proto.CodeUnreachable
	case errors.As(err, &netErr) && netErr.Timeout():
		return proto.CodeTimeout
	}
	return proto.CodeError
}
