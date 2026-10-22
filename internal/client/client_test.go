package client_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sontran0410/gotunnel/internal/agent"
	"github.com/sontran0410/gotunnel/internal/client"
	"github.com/sontran0410/gotunnel/internal/config"
	"github.com/sontran0410/gotunnel/internal/proto"
	"github.com/sontran0410/gotunnel/internal/transport"
)

type logWriter struct{ t *testing.T }

func (w logWriter) Write(p []byte) (int, error) {
	w.t.Log(string(bytes.TrimRight(p, "\n")))
	return len(p), nil
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(logWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// fakeAgent is a transport whose links are in-memory pipes served by an
// in-process agentd, standing in for "ssh host gotunnel agent stdio".
type fakeAgent struct {
	mu    sync.Mutex
	srv   *agent.Server
	links []net.Conn
	dials atomic.Int32
	log   *slog.Logger
	pipe  func() (net.Conn, net.Conn) // creates the two ends of a link
}

func newFakeAgent(t *testing.T) *fakeAgent {
	fa := &fakeAgent{log: testLogger(t).With("side", "agent"), pipe: net.Pipe}
	fa.srv = agent.NewServer(agent.Options{Logger: fa.log})
	t.Cleanup(func() { fa.server().Shutdown() })
	return fa
}

func (fa *fakeAgent) server() *agent.Server {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	return fa.srv
}

func (fa *fakeAgent) transport() transport.Transport {
	return transport.Func{Name: "fake", F: func(ctx context.Context) (transport.Link, error) {
		fa.dials.Add(1)
		c1, c2 := fa.pipe()
		srv := fa.server()
		go func() {
			if proto.WritePreamble(c2) == nil {
				srv.HandleConn(c2)
			}
		}()
		fa.mu.Lock()
		fa.links = append(fa.links, c1)
		fa.mu.Unlock()
		return c1, nil
	}}
}

// killLinks breaks every link, as a network failure would.
func (fa *fakeAgent) killLinks() {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	for _, l := range fa.links {
		l.Close()
	}
	fa.links = nil
}

// restart replaces agentd with a fresh one that knows no sessions.
func (fa *fakeAgent) restart(graceful bool) {
	fa.mu.Lock()
	old := fa.srv
	fa.srv = agent.NewServer(agent.Options{Logger: fa.log})
	fa.mu.Unlock()
	if graceful {
		old.Shutdown()
	} else {
		fa.killLinks()
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startEcho runs a TCP echo server that supports half-close.
func startEcho(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				io.Copy(c, c)
				c.(*net.TCPConn).CloseWrite()
				c.Close()
			}()
		}
	}()
	return l.Addr().String()
}

func baseConfig() *config.Config {
	cfg := &config.Config{SSH: config.SSH{Host: "fake"}}
	cfg.Connection.PingInterval = config.Duration(500 * time.Millisecond)
	cfg.Connection.DeadAfter = config.Duration(3 * time.Second)
	cfg.Connection.ConnectTimeout = config.Duration(5 * time.Second)
	cfg.Connection.SessionTTL = config.Duration(time.Minute)
	cfg.Daemon.LogFile = "-"
	return cfg
}

func startClient(t *testing.T, cfg *config.Config, fa *fakeAgent) *client.Client {
	t.Helper()
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c := client.New(cfg, fa.transport(), testLogger(t).With("side", "client"))
	if errs := c.Start(); len(errs) > 0 {
		t.Fatal(errs)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return c
}

// echoRoundTrip sends data through addr and checks it comes back intact.
func echoRoundTrip(t *testing.T, addr string, data []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() {
		conn.Write(data)
		conn.(*net.TCPConn).CloseWrite()
	}()
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read: %v (got %d of %d bytes)", err, len(got), len(data))
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("echo mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func randData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Uint32())
	}
	return b
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestForwardOnDemandAndIdleClose(t *testing.T) {
	fa := newFakeAgent(t)
	echo := startEcho(t)
	listen := freePort(t)
	cfg := baseConfig()
	cfg.Connection.IdleTimeout = config.Duration(time.Second)
	cfg.Forwards = []config.Forward{{Name: "echo", Listen: listen, Target: echo}}
	c := startClient(t, cfg, fa)

	time.Sleep(200 * time.Millisecond)
	if n := fa.dials.Load(); n != 0 {
		t.Fatalf("on-demand forward dialed the transport %d times before use", n)
	}
	echoRoundTrip(t, listen, []byte("hello through the tunnel"))
	echoRoundTrip(t, listen, randData(256<<10))
	if n := fa.dials.Load(); n != 1 {
		t.Fatalf("dials = %d, want 1 (one transport for all connections)", n)
	}
	waitFor(t, "idle close", func() bool { return c.Status().Transport.State == client.StateIdle })
	echoRoundTrip(t, listen, []byte("again"))
	if n := fa.dials.Load(); n != 2 {
		t.Fatalf("dials = %d, want 2", n)
	}
}

func TestConnectionsSurviveLinkFailures(t *testing.T) {
	fa := newFakeAgent(t)
	echo := startEcho(t)
	listen := freePort(t)
	cfg := baseConfig()
	cfg.Forwards = []config.Forward{{Name: "echo", Listen: listen, Target: echo, Mode: config.ModeAlways}}
	c := startClient(t, cfg, fa)
	waitFor(t, "connected", func() bool { return c.Status().Transport.State == client.StateConnected })

	// A long-lived connection keeps working across many link failures.
	conn, err := net.Dial("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	data := randData(4 << 20)
	go func() { // paced so the transfer spans many link failures
		for off := 0; off < len(data); off += 16 << 10 {
			conn.Write(data[off:min(off+16<<10, len(data))])
			time.Sleep(2 * time.Millisecond)
		}
		conn.(*net.TCPConn).CloseWrite()
	}()
	stop := make(chan struct{})
	go func() {
		for {
			// Break the link shortly after every successful (re)connect.
			for c.Status().Transport.State != client.StateConnected {
				time.Sleep(2 * time.Millisecond)
			}
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				fa.killLinks()
			}
		}
	}()
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	got, err := io.ReadAll(conn)
	close(stop)
	if err != nil {
		t.Fatalf("read: %v after %d bytes", err, len(got))
	}
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatalf("data corrupted: got %d bytes", len(got))
	}
	st := c.Status().Transport
	if st.Sessions != 1 || st.Resumes < 10 {
		t.Fatalf("sessions = %d, resumes = %d; want 1 session resumed several times", st.Sessions, st.Resumes)
	}
	t.Logf("4 MiB echoed across %d resumes", st.Resumes)
}

func TestAgentRestartStartsNewSession(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(strconv.FormatBool(graceful), func(t *testing.T) {
			fa := newFakeAgent(t)
			echo := startEcho(t)
			listen := freePort(t)
			cfg := baseConfig()
			cfg.Forwards = []config.Forward{{Name: "echo", Listen: listen, Target: echo, Mode: config.ModeAlways}}
			c := startClient(t, cfg, fa)
			waitFor(t, "connected", func() bool { return c.Status().Transport.State == client.StateConnected })

			old, err := net.Dial("tcp", listen)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			old.Write([]byte("ping"))
			buf := make([]byte, 4)
			io.ReadFull(old, buf)

			fa.restart(graceful)
			// The old connection is reset; new ones work on a new session.
			old.SetReadDeadline(time.Now().Add(15 * time.Second))
			if _, err := old.Read(buf); err == nil {
				t.Fatal("old connection still open after agent restart")
			}
			waitFor(t, "new session", func() bool { return c.Status().Transport.Sessions == 2 })
			echoRoundTrip(t, listen, []byte("fresh session"))
		})
	}
}

func socksConnect(t *testing.T, proxy, target string) (net.Conn, byte) {
	t.Helper()
	c, err := net.Dial("tcp", proxy)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(15 * time.Second))
	c.Write([]byte{5, 1, 0})
	resp := make([]byte, 2)
	if _, err := io.ReadFull(c, resp); err != nil || resp[1] != 0 {
		t.Fatalf("socks greeting: %v %v", resp, err)
	}
	host, portStr, _ := net.SplitHostPort(target)
	port, _ := strconv.Atoi(portStr)
	req := append([]byte{5, 1, 0, 3, byte(len(host))}, host...)
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	c.Write(req)
	reply := make([]byte, 10)
	if _, err := io.ReadFull(c, reply); err != nil {
		t.Fatalf("socks reply: %v", err)
	}
	c.SetDeadline(time.Time{})
	return c, reply[1]
}

func TestSocks(t *testing.T) {
	fa := newFakeAgent(t)
	echo := startEcho(t)
	proxy := freePort(t)
	cfg := baseConfig()
	cfg.Socks = []config.Socks{{Listen: proxy}}
	startClient(t, cfg, fa)

	_, port, _ := net.SplitHostPort(echo)
	c, code := socksConnect(t, proxy, net.JoinHostPort("localhost", port))
	if code != 0 {
		t.Fatalf("socks reply code %d", code)
	}
	c.Write([]byte("via socks"))
	buf := make([]byte, 9)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "via socks" {
		t.Fatalf("got %q, %v", buf, err)
	}
	c.Close()

	closed := freePort(t) // nothing listens here
	c, code = socksConnect(t, proxy, closed)
	c.Close()
	if code != 0x05 {
		t.Fatalf("reply code for refused connection = %d, want 5", code)
	}
}

func TestReverseForward(t *testing.T) {
	fa := newFakeAgent(t)
	localService := startEcho(t)
	remote := freePort(t)
	cfg := baseConfig()
	cfg.Reverse = []config.Reverse{{Name: "svc", RemoteListen: remote, Target: localService}}
	c := startClient(t, cfg, fa)
	waitFor(t, "reverse listener", func() bool {
		st := c.Status()
		return len(st.Reverse) == 1 && st.Reverse[0].State == "listening"
	})
	echoRoundTrip(t, remote, []byte("from the destination back to the client"))

	// The listener survives a link failure.
	fa.killLinks()
	waitFor(t, "resume", func() bool { return c.Status().Transport.Resumes >= 1 })
	echoRoundTrip(t, remote, []byte("still there"))

	if err := c.RemoveReverse("svc"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "remote listener closed", func() bool {
		conn, err := net.DialTimeout("tcp", remote, time.Second)
		if err == nil {
			conn.Close()
		}
		return err != nil
	})
}

func TestRuntimeForwardChanges(t *testing.T) {
	fa := newFakeAgent(t)
	echo := startEcho(t)
	listen := freePort(t)
	c := startClient(t, baseConfig(), fa)

	if err := c.AddForward(config.Forward{Name: "late", Listen: listen, Target: echo}); err != nil {
		t.Fatal(err)
	}
	echoRoundTrip(t, listen, []byte("added at runtime"))
	if err := c.AddForward(config.Forward{Name: "dup", Listen: listen, Target: echo}); err == nil {
		t.Fatal("duplicate listen address accepted")
	}
	if err := c.RemoveForward("late"); err != nil {
		t.Fatal(err)
	}
	if conn, err := net.DialTimeout("tcp", listen, time.Second); err == nil {
		conn.Close()
		t.Fatal("listener still open after removal")
	}
}

func TestDialFailureClosesLocalConnection(t *testing.T) {
	fa := newFakeAgent(t)
	listen := freePort(t)
	cfg := baseConfig()
	cfg.Forwards = []config.Forward{{Name: "dead", Listen: listen, Target: freePort(t)}}
	startClient(t, cfg, fa)
	conn, err := net.Dial("tcp", listen)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the connection to be closed")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection was not closed after the remote dial failed")
	}
}
