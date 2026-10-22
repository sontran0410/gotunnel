package netutil

import (
	"io"
	"net"
	"sync"
	"time"
)

// FlakyConn fails after Budget bytes have been written, possibly in the
// middle of a Write. It is used by tests to break links at random points.
type FlakyConn struct {
	net.Conn
	mu     sync.Mutex
	Budget int
}

func (f *FlakyConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(p) <= f.Budget {
		f.Budget -= len(p)
		return f.Conn.Write(p)
	}
	n, _ := f.Conn.Write(p[:f.Budget])
	f.Budget = 0
	f.Conn.Close()
	return n, io.ErrClosedPipe
}

// SlowLink is an in-memory duplex link with limited bandwidth and latency in
// each direction and a large buffer, similar to a slow WAN path behind ssh.
// Writes never block (unless the buffer is full); bytes become readable on
// the other end after the serialization delay plus the latency.
type SlowLink struct {
	A, B net.Conn
}

// NewSlowLink returns two connected ends. bytesPerSec limits throughput in
// each direction; latency is added one way.
func NewSlowLink(bytesPerSec int, latency time.Duration) *SlowLink {
	ab := newSlowPipe(bytesPerSec, latency)
	ba := newSlowPipe(bytesPerSec, latency)
	return &SlowLink{
		A: &slowEnd{r: ba, w: ab},
		B: &slowEnd{r: ab, w: ba},
	}
}

type chunk struct {
	data  []byte
	ready time.Time
}

type slowPipe struct {
	mu       sync.Mutex
	cond     sync.Cond
	rate     int
	latency  time.Duration
	queue    []chunk
	nextFree time.Time // when the "wire" finishes sending what is queued
	closed   bool
}

func newSlowPipe(rate int, latency time.Duration) *slowPipe {
	p := &slowPipe{rate: rate, latency: latency}
	p.cond.L = &p.mu
	return p
}

func (p *slowPipe) write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, io.ErrClosedPipe
	}
	now := time.Now()
	start := now
	if p.nextFree.After(now) {
		start = p.nextFree
	}
	tx := time.Duration(float64(len(b)) / float64(p.rate) * float64(time.Second))
	p.nextFree = start.Add(tx)
	p.queue = append(p.queue, chunk{data: append([]byte(nil), b...), ready: p.nextFree.Add(p.latency)})
	p.cond.Broadcast()
	return len(b), nil
}

func (p *slowPipe) read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return 0, io.EOF
		}
		if len(p.queue) > 0 {
			wait := time.Until(p.queue[0].ready)
			if wait <= 0 {
				c := &p.queue[0]
				n := copy(b, c.data)
				c.data = c.data[n:]
				if len(c.data) == 0 {
					p.queue = p.queue[1:]
				}
				return n, nil
			}
			p.mu.Unlock()
			time.Sleep(wait)
			p.mu.Lock()
			continue
		}
		p.cond.Wait()
	}
}

func (p *slowPipe) close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
}

type slowEnd struct {
	r, w *slowPipe
}

func (e *slowEnd) Read(b []byte) (int, error)         { return e.r.read(b) }
func (e *slowEnd) Write(b []byte) (int, error)        { return e.w.write(b) }
func (e *slowEnd) Close() error                       { e.r.close(); e.w.close(); return nil }
func (e *slowEnd) LocalAddr() net.Addr                { return pipeAddr{} }
func (e *slowEnd) RemoteAddr() net.Addr               { return pipeAddr{} }
func (e *slowEnd) SetDeadline(t time.Time) error      { return nil }
func (e *slowEnd) SetReadDeadline(t time.Time) error  { return nil }
func (e *slowEnd) SetWriteDeadline(t time.Time) error { return nil }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "slowlink" }
func (pipeAddr) String() string  { return "slowlink" }
