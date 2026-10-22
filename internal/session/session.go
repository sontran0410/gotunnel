// Package session implements a byte stream that survives link failures.
//
// A Session is an io.ReadWriteCloser shared by the client and the agent. It
// runs over a "link" (the ssh pipe). Every payload byte has an implicit
// sequence number: its offset in the stream. The receiver periodically
// acknowledges the offset it has received; the sender keeps unacknowledged
// bytes in a replay buffer. When a link dies, both sides keep their state;
// after a new link is attached each side resends from the offset the other
// side reports. Applications above the session only observe a stall.
package session

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"gotunnel/internal/proto"
)

// ID identifies a session.
type ID [16]byte

// NewID returns a random session ID.
func NewID() ID {
	var id ID
	_, _ = rand.Read(id[:])
	return id
}

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Short returns an abbreviated form for logs.
func (id ID) Short() string { return hex.EncodeToString(id[:4]) }

// ParseID parses a hex session ID.
func ParseID(s string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(id) {
		return id, errors.New("session: malformed id")
	}
	copy(id[:], b)
	return id, nil
}

// Token authenticates a session resume.
type Token [32]byte

// NewToken returns a random token.
func NewToken() Token {
	var t Token
	_, _ = rand.Read(t[:])
	return t
}

func (t Token) String() string { return hex.EncodeToString(t[:]) }

// Equal compares tokens in constant time.
func (t Token) Equal(o Token) bool { return subtle.ConstantTimeCompare(t[:], o[:]) == 1 }

// ParseToken parses a hex token.
func ParseToken(s string) (Token, error) {
	var t Token
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(t) {
		return t, errors.New("session: malformed token")
	}
	copy(t[:], b)
	return t, nil
}

// Errors reported by a session.
var (
	ErrClosed     = errors.New("session closed")
	ErrPeerClosed = errors.New("session closed by peer")
	ErrDeadLink   = errors.New("link dead: nothing received in time")
	ErrReplaced   = errors.New("link replaced by a newer one")
	ErrBadOffset  = errors.New("peer resume offset out of range")
	ErrProtocol   = errors.New("protocol error on link")
)

// Options tunes a session. Zero values select defaults.
type Options struct {
	// SendLimit caps unacknowledged bytes held for replay; Write blocks while
	// the cap is reached. It can be changed later with SetSendLimit.
	SendLimit int
	// PingInterval is how often to ping while attached (0 disables).
	PingInterval time.Duration
	// DeadAfter detaches the link if nothing arrives for this long (0 disables).
	DeadAfter time.Duration
	// WakeProbe: after a detected system sleep the link is detached unless
	// something arrives within this time (0 disables).
	WakeProbe time.Duration
	// AckBytes and AckDelay control acknowledgement frequency.
	AckBytes int
	AckDelay time.Duration
	// MaxChunk is the largest DATA frame payload.
	MaxChunk int
	// OnSample, if set, receives a measurement for every acknowledged DATA
	// frame. It is called from the link reader goroutine without locks held.
	OnSample func(Sample)
	// Logger receives link events (default: discard).
	Logger *slog.Logger
}

func (o *Options) defaults() {
	if o.SendLimit <= 0 {
		o.SendLimit = 8 << 20
	}
	if o.AckBytes <= 0 {
		o.AckBytes = 4 << 10
	}
	if o.AckDelay <= 0 {
		o.AckDelay = 25 * time.Millisecond
	}
	if o.MaxChunk <= 0 {
		// Small frames keep RTT samples close to the path RTT on slow links
		// (a frame's own transmission time is part of its sample).
		o.MaxChunk = 4 << 10
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// Session is a resumable byte stream. See the package documentation.
type Session struct {
	id    ID
	token Token
	opts  Options
	log   *slog.Logger
	epoch time.Time // reference point for ping nonces
	done  chan struct{}

	mu    sync.Mutex
	wcond sync.Cond // send space freed or state changed
	rcond sync.Cond // inbound data arrived or state changed

	limit    int
	send     ring   // unacknowledged outbound bytes: offsets [sendBase, sendBase+send.Len())
	sendBase uint64 // offset of the first unacknowledged byte
	wire     uint64 // offset up to which bytes were written to the current link
	samples  []rttSample

	recv       ring   // inbound bytes not yet read by the application
	recvOffset uint64 // total inbound bytes received
	ackedOut   uint64 // last offset acknowledged to the peer
	ackDue     time.Time

	link       *link
	attaches   int
	closed     bool
	closeErr   error
	lastRecv   time.Time
	detachedAt time.Time
	detachErr  error

	rtt, minRTT       time.Duration
	bytesIn, bytesOut uint64
}

type rttSample struct {
	end       uint64 // stream offset just after the sampled chunk
	at        time.Time
	inflight  int
	delivered uint64 // bytes acknowledged when the chunk was sent
}

// Sample is a measurement taken when a DATA frame is acknowledged.
type Sample struct {
	RTT time.Duration
	// Inflight is the number of unacknowledged bytes when the frame was sent.
	Inflight int
	// DeliveryRate is the rate (bytes/s) at which the peer acknowledged data
	// while the frame was in flight.
	DeliveryRate float64
}

// link is one attachment of the session to a transport.
type link struct {
	r io.Reader
	w io.Writer
	c io.Closer

	done chan struct{} // closed when the link is detached
	kick chan struct{} // wakes the writer

	// Guarded by Session.mu.
	pongs      []uint64
	pingDue    bool
	byeQueued  bool
	byeSent    chan struct{}
	probeStart time.Time
	probeUntil time.Time
}

var closedChan = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

// New creates a detached session.
func New(id ID, token Token, opts Options) *Session {
	opts.defaults()
	s := &Session{
		id:         id,
		token:      token,
		opts:       opts,
		log:        opts.Logger.With("session", id.Short()),
		epoch:      time.Now(),
		done:       make(chan struct{}),
		limit:      opts.SendLimit,
		detachedAt: time.Now(),
	}
	s.wcond.L = &s.mu
	s.rcond.L = &s.mu
	return s
}

// ID returns the session ID.
func (s *Session) ID() ID { return s.id }

// Token returns the resume token.
func (s *Session) Token() Token { return s.token }

// Done is closed when the session is closed.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err returns why the session closed, or nil while it is open.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// RecvOffset returns the number of payload bytes received so far.
func (s *Session) RecvOffset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recvOffset
}

// Unacked returns the number of bytes waiting for acknowledgement.
func (s *Session) Unacked() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.send.Len()
}

// SetSendLimit changes the maximum number of unacknowledged bytes.
func (s *Session) SetSendLimit(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = max(n, 1)
	s.wcond.Broadcast()
}

// DetachedSince returns when the session lost its last link (zero if attached).
func (s *Session) DetachedSince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.link != nil {
		return time.Time{}
	}
	return s.detachedAt
}

// LinkDone returns a channel closed when the current link detaches. If no
// link is attached the returned channel is already closed.
func (s *Session) LinkDone() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.link == nil {
		return closedChan
	}
	return s.link.done
}

// Write queues p for delivery. It blocks while the replay buffer is full,
// including while no link is attached.
func (s *Session) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	written := 0
	for len(p) > 0 {
		for !s.closed && s.send.Len() >= s.limit {
			s.wcond.Wait()
		}
		if s.closed {
			return written, s.closeErr
		}
		n := min(len(p), s.limit-s.send.Len())
		s.send.Write(p[:n])
		p = p[n:]
		written += n
		s.bytesOut += uint64(n)
		s.kickLocked()
	}
	return written, nil
}

// Read reads received payload bytes. It returns io.EOF once the session is
// closed and all received data has been read.
func (s *Session) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.recv.Len() == 0 {
		if s.closed {
			return 0, io.EOF
		}
		s.rcond.Wait()
	}
	return s.recv.Read(p), nil
}

// Attach starts using a link. peerRecv is the number of bytes the peer has
// received (from the handshake); bytes after it are resent. Any current link
// is detached first. On error the caller still owns c.
func (s *Session) Attach(r io.Reader, w io.Writer, c io.Closer, peerRecv uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attachLocked(r, w, c, peerRecv)
}

// Resume attaches a link for a returning peer, atomically with respect to
// other links: it detaches the current link, calls welcome with the number of
// bytes received so far (the caller must send it to the peer before any
// session frames), then attaches the new link.
func (s *Session) Resume(r io.Reader, w io.Writer, c io.Closer, peerRecv uint64, welcome func(recv uint64) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	if s.link != nil {
		s.detachLocked(s.link, ErrReplaced)
	}
	if err := welcome(s.recvOffset); err != nil {
		return err
	}
	return s.attachLocked(r, w, c, peerRecv)
}

func (s *Session) attachLocked(r io.Reader, w io.Writer, c io.Closer, peerRecv uint64) error {
	if s.closed {
		return s.closeErr
	}
	if s.link != nil {
		s.detachLocked(s.link, ErrReplaced)
	}
	sendNext := s.sendBase + uint64(s.send.Len())
	if peerRecv < s.sendBase || peerRecv > sendNext {
		return fmt.Errorf("%w: peer has %d, we hold [%d, %d]", ErrBadOffset, peerRecv, s.sendBase, sendNext)
	}
	s.send.Discard(int(peerRecv - s.sendBase))
	s.sendBase = peerRecv
	s.wire = peerRecv
	s.samples = s.samples[:0]
	// The handshake told the peer how much we have received.
	s.ackedOut = s.recvOffset
	s.ackDue = time.Time{}

	l := &link{
		r:       r,
		w:       w,
		c:       c,
		done:    make(chan struct{}),
		kick:    make(chan struct{}, 1),
		byeSent: make(chan struct{}),
	}
	s.link = l
	s.attaches++
	s.lastRecv = time.Now()
	s.wcond.Broadcast()
	go s.readLoop(l)
	go s.writeLoop(l)
	go s.monitor(l)
	s.kickLocked()
	s.log.Debug("link attached", "resend_from", peerRecv, "pending", sendNext-peerRecv, "attach", s.attaches)
	return nil
}

// DetachLink drops the current link, if any; the session stays usable.
func (s *Session) DetachLink(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.link != nil {
		s.detachLocked(s.link, err)
	}
}

func (s *Session) detach(l *link, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.detachLocked(l, err)
}

func (s *Session) detachLocked(l *link, err error) {
	if s.link != l {
		return
	}
	s.link = nil
	s.detachedAt = time.Now()
	s.detachErr = err
	close(l.done)
	go l.c.Close()
	if !s.closed {
		s.log.Info("link detached", "err", err)
	}
}

// Close sends BYE to the peer (if a link is attached) and closes the session.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.markClosedLocked(ErrClosed)
	if l := s.link; l != nil {
		l.byeQueued = true
		s.kickLocked()
		s.mu.Unlock()
		select {
		case <-l.byeSent:
		case <-l.done:
		case <-time.After(2 * time.Second):
		}
		s.mu.Lock()
		s.detachLocked(l, ErrClosed)
	}
	s.mu.Unlock()
	return nil
}

// Abort closes the session without notifying the peer.
func (s *Session) Abort(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked(err)
}

func (s *Session) closeLocked(err error) {
	if s.closed {
		return
	}
	s.markClosedLocked(err)
	if s.link != nil {
		s.detachLocked(s.link, err)
	}
}

func (s *Session) markClosedLocked(err error) {
	s.closed = true
	s.closeErr = err
	close(s.done)
	s.wcond.Broadcast()
	s.rcond.Broadcast()
}

func (s *Session) kickLocked() {
	if s.link == nil {
		return
	}
	select {
	case s.link.kick <- struct{}{}:
	default:
	}
}

func (s *Session) readLoop(l *link) {
	buf := make([]byte, 64<<10)
	for {
		typ, payload, err := proto.ReadFrame(l.r, buf)
		if err != nil {
			s.detach(l, fmt.Errorf("read: %w", err))
			return
		}
		var obs []Sample
		now := time.Now()

		s.mu.Lock()
		if s.link != l {
			s.mu.Unlock()
			return
		}
		s.lastRecv = now
		switch typ {
		case proto.FrameData:
			s.recv.Write(payload)
			s.recvOffset += uint64(len(payload))
			s.bytesIn += uint64(len(payload))
			s.rcond.Broadcast()
			if s.ackDue.IsZero() {
				s.ackDue = now.Add(s.opts.AckDelay)
			}
			if s.recvOffset-s.ackedOut >= uint64(s.opts.AckBytes) {
				s.ackDue = now
			}
			s.kickLocked()
		case proto.FrameAck:
			off, err := proto.U64(payload)
			if err != nil || off < s.sendBase || off > s.wire {
				s.detachLocked(l, fmt.Errorf("%w: bad ack %d (base %d, wire %d)", ErrProtocol, off, s.sendBase, s.wire))
				s.mu.Unlock()
				return
			}
			s.send.Discard(int(off - s.sendBase))
			s.sendBase = off
			s.wcond.Broadcast()
			for len(s.samples) > 0 && s.samples[0].end <= off {
				smp := s.samples[0]
				s.samples = s.samples[1:]
				rtt := now.Sub(smp.at)
				obs = append(obs, Sample{
					RTT:          rtt,
					Inflight:     smp.inflight,
					DeliveryRate: float64(off-smp.delivered) / max(rtt, time.Millisecond).Seconds(),
				})
				s.noteRTTLocked(rtt)
			}
		case proto.FramePing:
			nonce, err := proto.U64(payload)
			if err == nil && len(l.pongs) < 16 {
				l.pongs = append(l.pongs, nonce)
				s.kickLocked()
			}
		case proto.FramePong:
			if nonce, err := proto.U64(payload); err == nil {
				sent := s.epoch.Add(time.Duration(nonce))
				if rtt := now.Sub(sent); rtt >= 0 {
					s.noteRTTLocked(rtt)
				}
			}
		case proto.FrameBye:
			s.log.Info("session closed by peer")
			s.closeLocked(ErrPeerClosed)
			s.mu.Unlock()
			return
		default:
			s.detachLocked(l, fmt.Errorf("%w: unexpected frame type 0x%02x", ErrProtocol, typ))
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		if cb := s.opts.OnSample; cb != nil {
			for _, o := range obs {
				cb(o)
			}
		}
	}
}

func (s *Session) noteRTTLocked(rtt time.Duration) {
	s.rtt = rtt
	if s.minRTT == 0 || rtt < s.minRTT {
		s.minRTT = rtt
	}
}

func (s *Session) writeLoop(l *link) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	out := make([]byte, 0, 96<<10)
	for {
		s.mu.Lock()
		if s.link != l {
			s.mu.Unlock()
			return
		}
		if l.byeQueued {
			s.mu.Unlock()
			_ = proto.WriteFrame(l.w, proto.FrameBye, nil)
			close(l.byeSent)
			return
		}
		var wait time.Duration
		out, wait = s.fillLocked(l, out[:0])
		s.mu.Unlock()

		if len(out) > 0 {
			if _, err := l.w.Write(out); err != nil {
				s.detach(l, fmt.Errorf("write: %w", err))
				return
			}
			continue
		}
		if wait > 0 {
			timer.Reset(wait)
		}
		select {
		case <-l.kick:
		case <-timer.C:
		case <-l.done:
			return
		}
		timer.Stop()
	}
}

// fillLocked appends the frames that are ready to be sent and returns how long
// to wait before a delayed acknowledgement becomes due (0: nothing pending).
func (s *Session) fillLocked(l *link, out []byte) ([]byte, time.Duration) {
	now := time.Now()
	for _, n := range l.pongs {
		out = proto.AppendU64Frame(out, proto.FramePong, n)
	}
	l.pongs = l.pongs[:0]
	if l.pingDue {
		out = proto.AppendU64Frame(out, proto.FramePing, uint64(now.Sub(s.epoch)))
		l.pingDue = false
	}

	sendNext := s.sendBase + uint64(s.send.Len())
	for s.wire < sendNext && len(out) < 64<<10 {
		n := min(int(sendNext-s.wire), s.opts.MaxChunk)
		off := len(out)
		out = append(out, make([]byte, proto.HeaderSize+n)...)
		proto.PutHeader(out[off:], proto.FrameData, n)
		s.send.CopyAt(out[off+proto.HeaderSize:], int(s.wire-s.sendBase))
		s.wire += uint64(n)
		if len(s.samples) < 256 {
			s.samples = append(s.samples, rttSample{end: s.wire, at: now, inflight: int(s.wire - s.sendBase), delivered: s.sendBase})
		}
	}

	var wait time.Duration
	if s.recvOffset > s.ackedOut {
		// Acknowledge when due, or piggyback on anything else being sent.
		if len(out) > 0 || !now.Before(s.ackDue) {
			out = proto.AppendU64Frame(out, proto.FrameAck, s.recvOffset)
			s.ackedOut = s.recvOffset
			s.ackDue = time.Time{}
		} else {
			wait = s.ackDue.Sub(now)
		}
	}
	return out, wait
}

// monitor sends pings and detects dead links and system sleep.
func (s *Session) monitor(l *link) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastPing := time.Now()
	prev := time.Now()
	for {
		select {
		case <-l.done:
			return
		case <-tick.C:
		}
		now := time.Now()
		// The monotonic clock stops during system sleep while the wall clock
		// does not; a large difference means we just woke up.
		slept := now.Round(0).Sub(prev.Round(0)) - now.Sub(prev)
		prev = now

		s.mu.Lock()
		if s.link != l {
			s.mu.Unlock()
			return
		}
		if slept > 3*time.Second && s.opts.WakeProbe > 0 && l.probeUntil.IsZero() {
			s.log.Info("system sleep detected, probing link", "slept", slept.Round(time.Second))
			l.probeStart = now
			l.probeUntil = now.Add(s.opts.WakeProbe)
			l.pingDue = true
			lastPing = now
			s.kickLocked()
		}
		if !l.probeUntil.IsZero() {
			switch {
			case s.lastRecv.After(l.probeStart):
				l.probeUntil = time.Time{}
			case now.After(l.probeUntil):
				s.detachLocked(l, fmt.Errorf("%w (after system sleep)", ErrDeadLink))
				s.mu.Unlock()
				return
			}
		}
		if s.opts.DeadAfter > 0 && now.Sub(s.lastRecv) > s.opts.DeadAfter {
			s.detachLocked(l, ErrDeadLink)
			s.mu.Unlock()
			return
		}
		if s.opts.PingInterval > 0 && now.Sub(lastPing) >= s.opts.PingInterval {
			l.pingDue = true
			lastPing = now
			s.kickLocked()
		}
		s.mu.Unlock()
	}
}

// Stats is a snapshot of session counters.
type Stats struct {
	ID         string        `json:"id"`
	Attached   bool          `json:"attached"`
	DetachedAt time.Time     `json:"detached_at,omitempty"`
	DetachErr  string        `json:"detach_err,omitempty"`
	Attaches   int           `json:"attaches"`
	RTT        time.Duration `json:"rtt"`
	MinRTT     time.Duration `json:"min_rtt"`
	Unacked    int           `json:"unacked"`
	Limit      int           `json:"limit"`
	BytesIn    uint64        `json:"bytes_in"`
	BytesOut   uint64        `json:"bytes_out"`
}

// Stats returns a snapshot of the session state.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{
		ID:       s.id.String(),
		Attached: s.link != nil,
		Attaches: s.attaches,
		RTT:      s.rtt,
		MinRTT:   s.minRTT,
		Unacked:  s.send.Len(),
		Limit:    s.limit,
		BytesIn:  s.bytesIn,
		BytesOut: s.bytesOut,
	}
	if s.link == nil {
		st.DetachedAt = s.detachedAt
	}
	if s.detachErr != nil {
		st.DetachErr = s.detachErr.Error()
	}
	return st
}
