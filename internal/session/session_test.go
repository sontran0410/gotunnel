package session

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sontran0410/gotunnel/internal/netutil"
)

func TestRingRandomOps(t *testing.T) {
	var r ring
	var ref []byte
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		switch rng.IntN(3) {
		case 0:
			p := make([]byte, rng.IntN(5000))
			for j := range p {
				p[j] = byte(rng.Uint32())
			}
			r.Write(p)
			ref = append(ref, p...)
		case 1:
			k := rng.IntN(6000)
			r.Discard(k)
			ref = ref[min(k, len(ref)):]
		case 2:
			off := rng.IntN(len(ref) + 1)
			p := make([]byte, rng.IntN(3000))
			n := r.CopyAt(p, off)
			want := ref[off:]
			want = want[:min(len(want), len(p))]
			if n != len(want) || !bytes.Equal(p[:n], want) {
				t.Fatalf("op %d: CopyAt mismatch", i)
			}
		}
		if r.Len() != len(ref) {
			t.Fatalf("op %d: Len = %d, want %d", i, r.Len(), len(ref))
		}
	}
}

func newPair(opts Options) (*Session, *Session) {
	return New(NewID(), NewToken(), opts), New(NewID(), NewToken(), opts)
}

// attachPair connects a and b over a fresh in-memory pipe, resuming from the
// offsets each side has received, as the real handshake does.
func attachPair(t *testing.T, a, b *Session) (net.Conn, net.Conn) {
	t.Helper()
	ca, cb := net.Pipe()
	ra, rb := a.RecvOffset(), b.RecvOffset()
	if err := a.Attach(ca, ca, ca, rb); err != nil {
		t.Fatal(err)
	}
	if err := b.Attach(cb, cb, cb, ra); err != nil {
		t.Fatal(err)
	}
	return ca, cb
}

func randBytes(n int, seed uint64) []byte {
	rng := rand.New(rand.NewPCG(seed, seed+1))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

func readAll(t *testing.T, s *Session, n int) <-chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		buf := make([]byte, n)
		if _, err := io.ReadFull(s, buf); err != nil {
			t.Errorf("read: %v", err)
		}
		ch <- buf
	}()
	return ch
}

func TestTransfer(t *testing.T) {
	a, b := newPair(Options{})
	defer a.Abort(ErrClosed)
	defer b.Abort(ErrClosed)
	attachPair(t, a, b)

	data := randBytes(3<<20, 1)
	got := readAll(t, b, len(data))
	if _, err := a.Write(data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-got, data) {
		t.Fatal("data mismatch")
	}
}

func TestTransferSurvivesLinkFailures(t *testing.T) {
	var rttCalls sync.WaitGroup
	rttCalls.Add(1)
	var once sync.Once
	a, b := newPair(Options{
		SendLimit: 256 << 10,
		OnSample:  func(Sample) { once.Do(rttCalls.Done) },
	})
	defer a.Abort(ErrClosed)
	defer b.Abort(ErrClosed)

	const size = 4 << 20
	dataA, dataB := randBytes(size, 7), randBytes(size, 9)
	gotB, gotA := readAll(t, b, size), readAll(t, a, size)
	go a.Write(dataA)
	go b.Write(dataB)

	var resA, resB []byte
	rng := rand.New(rand.NewPCG(3, 4))
	failures := 0
	for resA == nil || resB == nil {
		// Each link dies after a random number of bytes written by one side,
		// usually in the middle of a frame.
		ca, cb := net.Pipe()
		fa, fb := net.Conn(ca), net.Conn(cb)
		if rng.IntN(2) == 0 {
			fa = &netutil.FlakyConn{Conn: ca, Budget: 1 + rng.IntN(64<<10)}
		} else {
			fb = &netutil.FlakyConn{Conn: cb, Budget: 1 + rng.IntN(64<<10)}
		}
		ra, rb := a.RecvOffset(), b.RecvOffset()
		if err := a.Attach(fa, fa, fa, rb); err != nil {
			t.Fatal(err)
		}
		if err := b.Attach(fb, fb, fb, ra); err != nil {
			t.Fatal(err)
		}
		doneA, doneB := a.LinkDone(), b.LinkDone()
		select {
		case resB = <-gotB:
		case resA = <-gotA:
		case <-doneA:
		case <-doneB:
		}
		// Make sure both sides see the failure before the next handshake.
		ca.Close()
		<-doneA
		<-doneB
		failures++
		if failures > 100000 {
			t.Fatal("transfer did not finish")
		}
	}
	if sha256.Sum256(resB) != sha256.Sum256(dataA) || sha256.Sum256(resA) != sha256.Sum256(dataB) {
		t.Fatal("data corrupted across link failures")
	}
	if failures < 50 {
		t.Fatalf("only %d link failures; test is not exercising resume", failures)
	}
	rttCalls.Wait()
	t.Logf("survived %d link failures", failures)
}

func TestCloseNotifiesPeer(t *testing.T) {
	a, b := newPair(Options{})
	attachPair(t, a, b)
	if _, err := a.Write([]byte("bye soon")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if _, err := io.ReadFull(b, buf); err != nil {
		t.Fatal(err)
	}
	a.Close()
	select {
	case <-b.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("peer was not closed")
	}
	if !errors.Is(b.Err(), ErrPeerClosed) {
		t.Fatalf("peer err = %v", b.Err())
	}
	if n, err := b.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("Read after close = %d, %v", n, err)
	}
	if _, err := a.Write([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write after close = %v", err)
	}
}

// blackhole accepts writes and never delivers anything.
type blackhole struct{ closed chan struct{} }

func (b *blackhole) Read([]byte) (int, error)    { <-b.closed; return 0, io.EOF }
func (b *blackhole) Write(p []byte) (int, error) { return len(p), nil }
func (b *blackhole) Close() error                { close(b.closed); return nil }

func TestDeadLinkDetected(t *testing.T) {
	s := New(NewID(), NewToken(), Options{PingInterval: 500 * time.Millisecond, DeadAfter: 1500 * time.Millisecond})
	defer s.Abort(ErrClosed)
	bh := &blackhole{closed: make(chan struct{})}
	if err := s.Attach(bh, bh, bh, 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.LinkDone():
	case <-time.After(5 * time.Second):
		t.Fatal("dead link not detected")
	}
	if st := s.Stats(); st.Attached || st.DetachErr != ErrDeadLink.Error() {
		t.Fatalf("stats = %+v", st)
	}
}

func TestWriteBlocksAtLimitWhileDetached(t *testing.T) {
	a, b := newPair(Options{SendLimit: 1024})
	defer a.Abort(ErrClosed)
	defer b.Abort(ErrClosed)
	data := randBytes(4096, 5)
	wrote := make(chan error, 1)
	go func() { _, err := a.Write(data); wrote <- err }()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-wrote:
		t.Fatalf("Write returned early: %v", err)
	default:
	}
	if u := a.Unacked(); u != 1024 {
		t.Fatalf("Unacked = %d, want 1024", u)
	}
	got := readAll(t, b, len(data))
	attachPair(t, a, b)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-got, data) {
		t.Fatal("data mismatch")
	}
}

func TestAttachRejectsBadOffset(t *testing.T) {
	s := New(NewID(), NewToken(), Options{})
	c, _ := net.Pipe()
	if err := s.Attach(c, c, c, 10); !errors.Is(err, ErrBadOffset) {
		t.Fatalf("err = %v", err)
	}
}

func TestResumeReportsOffsetBeforeAttach(t *testing.T) {
	a, b := newPair(Options{})
	defer a.Abort(ErrClosed)
	defer b.Abort(ErrClosed)
	attachPair(t, a, b)
	msg := []byte("hello agent")
	if _, err := a.Write(msg); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(b, make([]byte, len(msg))); err != nil {
		t.Fatal(err)
	}
	// b plays the agent: a new link arrives while the old one still looks alive.
	ca, cb := net.Pipe()
	var reported uint64
	err := b.Resume(cb, cb, cb, a.RecvOffset(), func(recv uint64) error {
		reported = recv
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if reported != uint64(len(msg)) {
		t.Fatalf("reported recv = %d, want %d", reported, len(msg))
	}
	<-a.LinkDone() // the old link was closed by b
	if err := a.Attach(ca, ca, ca, reported); err != nil {
		t.Fatal(err)
	}
	got := readAll(t, a, 5)
	if _, err := b.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if string(<-got) != "hello" {
		t.Fatal("data mismatch after resume")
	}
}
