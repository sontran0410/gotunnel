// Package sched sits between yamux and the session and decides which yamux
// frame goes onto the link next.
//
// The session only accepts data while the bytes in flight are below its send
// limit, so frames wait here, where they can still be reordered, instead of
// in ssh's buffers, where they cannot. Frames are queued per stream and
// served by deficit round robin: streams that were idle (keystrokes) are
// served before streams that keep their queue full (downloads), and priority
// classes weight the share each stream gets. Order within a stream is never
// changed.
package sched

import (
	"encoding/binary"
	"io"
	"sync"

	"gotunnel/internal/config"
)

// Class is a stream priority class.
type Class int

// Priority classes.
const (
	ClassInteractive Class = iota
	ClassNormal
	ClassBulk
)

// ParseClass converts a configuration priority to a Class.
func ParseClass(p string) Class {
	switch p {
	case config.PrioInteractive:
		return ClassInteractive
	case config.PrioBulk:
		return ClassBulk
	}
	return ClassNormal
}

// quantum is the number of bytes a stream of each class may send per round.
var quantum = [...]int{
	ClassInteractive: 32 << 10,
	ClassNormal:      16 << 10,
	ClassBulk:        4 << 10,
}

// yamux framing (see github.com/hashicorp/yamux/spec.md).
const (
	yamuxHeaderSize = 12
	yamuxTypeData   = 0
	yamuxTypeWindow = 1
	yamuxFlagSYN    = 1
	yamuxFlagACK    = 2
	yamuxFlagFIN    = 4
	yamuxFlagRST    = 8
)

// DefaultChunkSize is the largest yamux data frame put on the link; larger
// frames are split so other streams can interleave.
const DefaultChunkSize = 4 << 10

// Sink receives scheduled bytes; *session.Session implements it.
type Sink interface {
	io.ReadWriteCloser
}

// Conn is the connection handed to yamux.
type Conn struct {
	sink      Sink
	chunkSize int

	mu      sync.Mutex
	cond    sync.Cond
	closed  bool
	err     error
	hdr     [yamuxHeaderSize]byte
	hdrN    int
	frame   []byte // frame being assembled (header + body)
	frameN  int
	ctrl    [][]byte // session-level frames (stream 0)
	flows   map[uint32]*flow
	classes map[uint32]Class
	newQ    []*flow
	oldQ    []*flow
	queued  int
}

// flow is the queue of one stream. Drained flows without an explicit class
// are deleted; flows with a class live until Forget.
type flow struct {
	id      uint32
	class   Class
	q       [][]byte
	deficit int
	active  bool // in newQ or oldQ
}

// New wraps sink and starts the pump that feeds it.
func New(sink Sink, chunkSize int) *Conn {
	c := newConn(sink, chunkSize)
	go c.pump()
	return c
}

func newConn(sink Sink, chunkSize int) *Conn {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	c := &Conn{
		sink:      sink,
		chunkSize: chunkSize,
		flows:     map[uint32]*flow{},
		classes:   map[uint32]Class{},
	}
	c.cond.L = &c.mu
	return c
}

// Read reads from the sink.
func (c *Conn) Read(p []byte) (int, error) { return c.sink.Read(p) }

// Write accepts yamux output. It never blocks on the link: frames are parsed
// and queued, and the pump moves them to the sink.
func (c *Conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	n := len(p)
	for len(p) > 0 {
		if c.frame == nil {
			k := copy(c.hdr[c.hdrN:], p)
			c.hdrN += k
			p = p[k:]
			if c.hdrN < yamuxHeaderSize {
				break
			}
			bodyLen := 0
			if c.hdr[1] == yamuxTypeData {
				bodyLen = int(binary.BigEndian.Uint32(c.hdr[8:12]))
			}
			c.frame = make([]byte, yamuxHeaderSize+bodyLen)
			copy(c.frame, c.hdr[:])
			c.frameN = yamuxHeaderSize
		}
		k := copy(c.frame[c.frameN:], p)
		c.frameN += k
		p = p[k:]
		if c.frameN == len(c.frame) {
			c.enqueueFrame(c.frame)
			c.frame, c.frameN, c.hdrN = nil, 0, 0
		}
	}
	return n, nil
}

// enqueueFrame queues a complete yamux frame, splitting large data frames.
func (c *Conn) enqueueFrame(fr []byte) {
	id := binary.BigEndian.Uint32(fr[4:8])
	if id == 0 {
		c.ctrl = append(c.ctrl, fr)
		c.queued += len(fr)
		c.cond.Signal()
		return
	}
	body := fr[yamuxHeaderSize:]
	if fr[1] != yamuxTypeData || len(body) <= c.chunkSize {
		c.enqueue(id, fr)
		return
	}
	// SYN/ACK must arrive with the first piece and FIN/RST with the last.
	flags := binary.BigEndian.Uint16(fr[2:4])
	for off := 0; off < len(body); off += c.chunkSize {
		end := min(off+c.chunkSize, len(body))
		var f uint16
		if off == 0 {
			f |= flags & (yamuxFlagSYN | yamuxFlagACK)
		}
		if end == len(body) {
			f |= flags & (yamuxFlagFIN | yamuxFlagRST)
		}
		piece := make([]byte, yamuxHeaderSize+end-off)
		copy(piece, fr[:yamuxHeaderSize])
		binary.BigEndian.PutUint16(piece[2:4], f)
		binary.BigEndian.PutUint32(piece[8:12], uint32(end-off))
		copy(piece[yamuxHeaderSize:], body[off:end])
		c.enqueue(id, piece)
	}
}

func (c *Conn) enqueue(id uint32, fr []byte) {
	f := c.flows[id]
	if f == nil {
		cl, ok := c.classes[id]
		if !ok {
			cl = ClassNormal
		}
		f = &flow{id: id, class: cl}
		c.flows[id] = f
	}
	f.q = append(f.q, fr)
	c.queued += len(fr)
	if !f.active {
		f.active = true
		f.deficit = quantum[f.class]
		c.newQ = append(c.newQ, f)
	}
	c.cond.Signal()
}

// dequeue returns the next frame to send, or nil if nothing is queued.
func (c *Conn) dequeue() []byte {
	if len(c.ctrl) > 0 {
		fr := c.ctrl[0]
		c.ctrl[0] = nil
		c.ctrl = c.ctrl[1:]
		c.queued -= len(fr)
		return fr
	}
	for {
		var f *flow
		fromNew := false
		switch {
		case len(c.newQ) > 0:
			f, fromNew = c.newQ[0], true
		case len(c.oldQ) > 0:
			f = c.oldQ[0]
		default:
			return nil
		}
		if len(f.q) == 0 {
			c.popFront(fromNew)
			f.active = false
			if _, ok := c.classes[f.id]; !ok {
				delete(c.flows, f.id)
			}
			continue
		}
		if f.deficit <= 0 {
			// Used up its share this round: go to the back of the old flows.
			f.deficit += quantum[f.class]
			c.popFront(fromNew)
			c.oldQ = append(c.oldQ, f)
			continue
		}
		fr := f.q[0]
		f.q[0] = nil
		f.q = f.q[1:]
		f.deficit -= len(fr)
		c.queued -= len(fr)
		return fr
	}
}

func (c *Conn) popFront(fromNew bool) {
	if fromNew {
		c.newQ[0] = nil
		c.newQ = c.newQ[1:]
	} else {
		c.oldQ[0] = nil
		c.oldQ = c.oldQ[1:]
	}
}

func (c *Conn) pump() {
	for {
		c.mu.Lock()
		var fr []byte
		for !c.closed {
			if fr = c.dequeue(); fr != nil {
				break
			}
			c.cond.Wait()
		}
		if c.closed {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		if _, err := c.sink.Write(fr); err != nil {
			c.mu.Lock()
			c.closed = true
			c.err = err
			c.mu.Unlock()
			return
		}
	}
}

// SetClass assigns a priority class to a stream.
func (c *Conn) SetClass(id uint32, cl Class) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.classes[id] = cl
	if f := c.flows[id]; f != nil {
		f.class = cl
	}
}

// Forget releases the state kept for a finished stream.
func (c *Conn) Forget(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.forgetLocked(id)
}

func (c *Conn) forgetLocked(id uint32) {
	delete(c.classes, id)
	if f := c.flows[id]; f != nil && !f.active {
		delete(c.flows, id)
	}
}

// Reset tells the peer to abort a stream (yamux has no public API for it):
// a window update frame with the RST flag is queued behind the stream's
// pending frames.
func (c *Conn) Reset(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	fr := make([]byte, yamuxHeaderSize)
	fr[1] = yamuxTypeWindow
	binary.BigEndian.PutUint16(fr[2:4], yamuxFlagRST)
	binary.BigEndian.PutUint32(fr[4:8], id)
	c.enqueue(id, fr)
	c.forgetLocked(id)
}

// Queued returns the number of bytes waiting in the scheduler.
func (c *Conn) Queued() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queued
}

// Close stops the pump and closes the sink.
func (c *Conn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	return c.sink.Close()
}
