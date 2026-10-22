// Package netutil provides connection helpers shared by client and agent.
package netutil

import (
	"errors"
	"io"
	"net"
	"sync"
)

// CopyBufferSize is the buffer used when copying between connections. It also
// bounds the size of yamux data frames, which keeps scheduling fine-grained.
const CopyBufferSize = 8 << 10

var bufPool = sync.Pool{New: func() any { b := make([]byte, CopyBufferSize); return &b }}

// CloseWriter is implemented by connections that support half-close.
type CloseWriter interface {
	CloseWrite() error
}

// Splice copies data between a and b in both directions until both
// directions reach EOF, propagating half-close. If either direction fails,
// both connections are closed immediately and the first error is returned.
// count, if non-nil, is called with the bytes moved (aToB tells the direction).
func Splice(a, b io.ReadWriteCloser, count func(aToB bool, n int)) error {
	errc := make(chan error, 2)
	go copyHalf(b, a, true, count, errc)
	go copyHalf(a, b, false, count, errc)

	err := <-errc
	if err != nil {
		abort(a)
		abort(b)
		<-errc
		return err
	}
	err = <-errc
	if err != nil {
		abort(a)
		abort(b)
		return err
	}
	a.Close()
	b.Close()
	return nil
}

func copyHalf(dst, src io.ReadWriteCloser, aToB bool, count func(bool, int), errc chan<- error) {
	bp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bp)
	buf := *bp
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			if count != nil && nw > 0 {
				count(aToB, nw)
			}
			if ew != nil {
				errc <- ew
				return
			}
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				errc <- closeWrite(dst)
			} else {
				errc <- er
			}
			return
		}
	}
}

func closeWrite(c io.ReadWriteCloser) error {
	if cw, ok := c.(CloseWriter); ok {
		err := cw.CloseWrite()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
	return c.Close()
}

// abort closes c; TCP connections are reset so the peer application sees the
// failure instead of a clean end of stream.
func abort(c io.ReadWriteCloser) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	c.Close()
}
