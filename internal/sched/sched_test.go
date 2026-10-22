package sched

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/sontran0410/gotunnel/internal/netutil"
	"github.com/sontran0410/gotunnel/internal/session"
)

func yamuxFrame(typ byte, flags uint16, id uint32, body []byte, length uint32) []byte {
	fr := make([]byte, yamuxHeaderSize+len(body))
	fr[1] = typ
	binary.BigEndian.PutUint16(fr[2:4], flags)
	binary.BigEndian.PutUint32(fr[4:8], id)
	if typ == yamuxTypeData {
		length = uint32(len(body))
	}
	binary.BigEndian.PutUint32(fr[8:12], length)
	copy(fr[yamuxHeaderSize:], body)
	return fr
}

type frameInfo struct {
	typ   byte
	flags uint16
	id    uint32
	body  []byte
}

func parseFrames(t *testing.T, b []byte) []frameInfo {
	t.Helper()
	var out []frameInfo
	for len(b) > 0 {
		if len(b) < yamuxHeaderSize {
			t.Fatalf("truncated header")
		}
		fi := frameInfo{typ: b[1], flags: binary.BigEndian.Uint16(b[2:4]), id: binary.BigEndian.Uint32(b[4:8])}
		n := 0
		if fi.typ == yamuxTypeData {
			n = int(binary.BigEndian.Uint32(b[8:12]))
		}
		fi.body = b[yamuxHeaderSize : yamuxHeaderSize+n]
		out = append(out, fi)
		b = b[yamuxHeaderSize+n:]
	}
	return out
}

// drain pulls every queued frame through dequeue.
func drain(c *Conn) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []byte
	for fr := c.dequeue(); fr != nil; fr = c.dequeue() {
		out = append(out, fr...)
	}
	return out
}

func TestParseSplitAndPreserveStreamOrder(t *testing.T) {
	c := newConn(nil, 1000)
	rng := rand.New(rand.NewPCG(1, 1))
	var input []byte
	want := map[uint32][]byte{}
	for i := 0; i < 200; i++ {
		id := uint32(1 + 2*rng.IntN(5))
		body := make([]byte, rng.IntN(5000))
		for j := range body {
			body[j] = byte(rng.Uint32())
		}
		flags := uint16(0)
		if i < 5 {
			flags = yamuxFlagSYN
		}
		input = append(input, yamuxFrame(yamuxTypeData, flags, id, body, 0)...)
		want[id] = append(want[id], body...)
		if rng.IntN(4) == 0 {
			input = append(input, yamuxFrame(yamuxTypeWindow, 0, id, nil, 4096)...)
		}
	}
	input = append(input, yamuxFrame(2 /* ping */, yamuxFlagSYN, 0, nil, 7)...)
	// Feed in random pieces, as yamux writes header and body separately.
	for len(input) > 0 {
		n := min(len(input), 1+rng.IntN(3000))
		if _, err := c.Write(input[:n]); err != nil {
			t.Fatal(err)
		}
		input = input[n:]
	}
	frames := parseFrames(t, drain(c))
	if frames[0].id != 0 {
		t.Errorf("session-level frame not sent first")
	}
	got := map[uint32][]byte{}
	for _, f := range frames {
		if len(f.body) > 1000 {
			t.Fatalf("data frame of %d bytes not split", len(f.body))
		}
		got[f.id] = append(got[f.id], f.body...)
	}
	for id, w := range want {
		if !bytes.Equal(got[id], w) {
			t.Errorf("stream %d data reordered or corrupted", id)
		}
	}
}

func TestInteractiveFrameJumpsBulkQueue(t *testing.T) {
	c := newConn(nil, DefaultChunkSize)
	c.SetClass(1, ClassBulk)
	c.SetClass(3, ClassInteractive)
	for i := 0; i < 64; i++ { // 512KiB of bulk data
		c.Write(yamuxFrame(yamuxTypeData, 0, 1, make([]byte, DefaultChunkSize), 0))
	}
	// Let the bulk flow use up its new-flow allowance.
	c.mu.Lock()
	c.dequeue()
	c.mu.Unlock()
	c.Write(yamuxFrame(yamuxTypeData, 0, 3, []byte("ls -l\n"), 0))
	frames := parseFrames(t, drain(c))
	pos := -1
	for i, f := range frames {
		if f.id == 3 {
			pos = i
		}
	}
	if pos < 0 || pos > 1 {
		t.Fatalf("interactive frame at position %d of %d", pos, len(frames))
	}
}

func TestWeightedShare(t *testing.T) {
	c := newConn(nil, DefaultChunkSize)
	c.SetClass(1, ClassBulk)
	c.SetClass(3, ClassNormal)
	for i := 0; i < 100; i++ {
		c.Write(yamuxFrame(yamuxTypeData, 0, 1, make([]byte, 4096), 0))
		c.Write(yamuxFrame(yamuxTypeData, 0, 3, make([]byte, 4096), 0))
	}
	frames := parseFrames(t, drain(c))
	var bulk, normal int
	for _, f := range frames[:60] {
		if f.id == 1 {
			bulk++
		} else {
			normal++
		}
	}
	if normal < 2*bulk {
		t.Fatalf("normal %d vs bulk %d frames in the first 60; want normal to dominate", normal, bulk)
	}
}

func TestResetQueuedAfterPendingFrames(t *testing.T) {
	c := newConn(nil, DefaultChunkSize)
	c.Write(yamuxFrame(yamuxTypeData, yamuxFlagSYN, 5, []byte("abc"), 0))
	c.Reset(5)
	frames := parseFrames(t, drain(c))
	if len(frames) != 2 || frames[1].flags != yamuxFlagRST || frames[1].typ != yamuxTypeWindow {
		t.Fatalf("frames = %+v", frames)
	}
	if len(c.flows) != 0 || len(c.classes) != 0 {
		t.Fatalf("state not released: %d flows, %d classes", len(c.flows), len(c.classes))
	}
}

func TestControllerTracksBDP(t *testing.T) {
	const kib = 1 << 10
	ctl := NewController(64*kib, 8*kib, 8<<20, 8*kib, true)
	clock := time.Unix(0, 0)
	ctl.now = func() time.Time { return clock }
	var lim lastLimit
	ctl.Bind(&lim)
	observe := func(n int, rtt time.Duration, rate float64) {
		for i := 0; i < n; i++ {
			clock = clock.Add(10 * time.Millisecond)
			ctl.Observe(rtt, ctl.Limit(), rate)
		}
	}
	// A 100 KiB/s path with 100ms RTT has a BDP of 10 KiB.
	observe(200, 100*time.Millisecond, 100*kib)
	steady := 10*kib*5/4 + 8*kib
	if l := ctl.Limit(); l < steady-kib || l > steady+kib || lim.get() != l {
		t.Fatalf("steady limit = %d (applied %d), want about %d", l, lim.get(), steady)
	}
	// Bandwidth grows: the limit follows, with the startup gain.
	for rate := 100.0 * kib; rate < 1600*kib; rate *= 1.5 {
		observe(10, 100*time.Millisecond, rate)
	}
	if l := ctl.Limit(); l < 160*kib {
		t.Fatalf("limit after bandwidth growth = %d, want >= 160KiB", l)
	}
	// After 30s without a lower RTT the queue is drained to re-measure it.
	start := clock
	for ctl.Limit() != 8*kib {
		observe(1, 150*time.Millisecond, 1600*kib)
		if clock.Sub(start) > 31*time.Second {
			t.Fatal("no RTT probe after 30s")
		}
	}
	if d := clock.Sub(start); d < 30*time.Second {
		t.Fatalf("RTT probe after %v, want 30s", d)
	}
	observe(30, 120*time.Millisecond, 1600*kib)
	if ctl.MinRTT() != 120*time.Millisecond || ctl.Limit() <= 8*kib {
		t.Fatalf("after probe: min RTT %v, limit %d", ctl.MinRTT(), ctl.Limit())
	}

	fixed := NewController(64*kib, 8*kib, 8<<20, 8*kib, false)
	fixed.Observe(time.Second, 64*kib, 1)
	if fixed.Limit() != 64*kib {
		t.Fatal("non-adaptive controller changed its limit")
	}
}

type lastLimit struct {
	mu sync.Mutex
	n  int
}

func (l *lastLimit) SetSendLimit(n int) { l.mu.Lock(); l.n = n; l.mu.Unlock() }
func (l *lastLimit) get() int           { l.mu.Lock(); defer l.mu.Unlock(); return l.n }

// TestStackSurvivesLinkFailures runs yamux over the scheduler over sessions,
// with parallel echo streams, while links keep failing.
func TestStackSurvivesLinkFailures(t *testing.T) {
	sa := session.New(session.NewID(), session.NewToken(), session.Options{SendLimit: 64 << 10})
	sb := session.New(session.NewID(), session.NewToken(), session.Options{SendLimit: 64 << 10})
	ca, cb := New(sa, 0), New(sb, 0)
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.StreamOpenTimeout = 0
	cfg.ConnectionWriteTimeout = time.Hour
	cfg.LogOutput = io.Discard
	client, err := yamux.Client(ca, cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(cb, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()

	go func() { // echo server
		for {
			st, err := server.AcceptStream()
			if err != nil {
				return
			}
			go func() { io.Copy(st, st); st.Close() }()
		}
	}()

	stop := make(chan struct{})
	chaosDone := make(chan struct{})
	failures := 0
	go func() { // link chaos: every link dies after a random number of bytes
		defer close(chaosDone)
		rng := rand.New(rand.NewPCG(5, 6))
		for {
			p1, p2 := net.Pipe()
			f1 := &netutil.FlakyConn{Conn: p1, Budget: 1 + rng.IntN(256<<10)}
			ra, rb := sa.RecvOffset(), sb.RecvOffset()
			if err := sa.Attach(f1, f1, f1, rb); err != nil {
				t.Error(err)
				return
			}
			if err := sb.Attach(p2, p2, p2, ra); err != nil {
				t.Error(err)
				return
			}
			da, db := sa.LinkDone(), sb.LinkDone()
			select {
			case <-stop:
				return
			case <-da:
			case <-db:
			}
			p1.Close()
			<-da
			<-db
			failures++
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := client.OpenStream()
			if err != nil {
				t.Error(err)
				return
			}
			data := bytes.Repeat([]byte(fmt.Sprintf("stream-%d|", i)), 30000)
			go func() { st.Write(data); st.Close() }()
			got, err := io.ReadAll(st)
			if err != nil {
				t.Errorf("stream %d: %v", i, err)
				return
			}
			if !bytes.Equal(got, data) {
				t.Errorf("stream %d: echo mismatch (%d vs %d bytes)", i, len(got), len(data))
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	<-chaosDone
	if failures < 10 {
		t.Fatalf("only %d link failures; test is not exercising resume", failures)
	}
	t.Logf("8 echo streams survived %d link failures", failures)
}
