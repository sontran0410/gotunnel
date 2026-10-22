// Package e2e drives the real gotunnel binary: install-agent, the background
// client daemon, the relay and an auto-started agentd. A fake ssh runs the
// remote command locally with a separate "destination" home directory, so
// the test needs neither sshd nor network access.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sontran0410/gotunnel/internal/client"
	"github.com/sontran0410/gotunnel/internal/proto"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "gotunnel-e2e")
	if err != nil {
		panic(err)
	}
	bin = filepath.Join(dir, "gotunnel")
	if out, err := exec.Command("go", "build", "-race", "-o", bin, "github.com/sontran0410/gotunnel/cmd/gotunnel").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

const fakeSSH = `#!/bin/sh
# Fake ssh: runs the remote command locally, as if on the destination.
for a; do
  case "$a" in
    -V) echo "fake-ssh" >&2; exit 0 ;;
    -G) printf 'hostname fake\nport 22\n'; exit 0 ;;
  esac
done
for last; do :; done
export HOME="$FAKE_REMOTE_HOME"
export GOTUNNEL_HOME="$FAKE_REMOTE_HOME/.gotunnel"
export GOTUNNEL_RUNTIME_DIR="$FAKE_REMOTE_RUN"
unset GOTUNNEL_DAEMON_CHILD
echo "Welcome to the destination!"   # shell noise before the agent starts
exec sh -c "$last"
`

type env struct {
	t                      *testing.T
	clientHome, remoteHome string
	clientRun, remoteRun   string
	fakeSSH                string
	cfgPath                string
}

func newEnv(t *testing.T) *env {
	root := t.TempDir()
	e := &env{t: t, clientHome: filepath.Join(root, "client"), remoteHome: filepath.Join(root, "remote")}
	// Short runtime directories keep unix socket paths under the size limit.
	for _, p := range []*string{&e.clientRun, &e.remoteRun} {
		d, err := os.MkdirTemp("/tmp", "gt")
		if err != nil {
			t.Fatal(err)
		}
		*p = d
		t.Cleanup(func() { os.RemoveAll(d) })
	}
	e.fakeSSH = filepath.Join(root, "fake-ssh")
	if err := os.WriteFile(e.fakeSSH, []byte(fakeSSH), 0o755); err != nil {
		t.Fatal(err)
	}
	e.cfgPath = filepath.Join(e.clientHome, ".config", "gotunnel", "config.yaml")
	os.MkdirAll(filepath.Dir(e.cfgPath), 0o755)
	os.MkdirAll(e.remoteHome, 0o755)
	return e
}

func (e *env) commonEnv() []string {
	return append(os.Environ(),
		"FAKE_REMOTE_HOME="+e.remoteHome,
		"FAKE_REMOTE_RUN="+e.remoteRun,
		"XDG_CONFIG_HOME="+filepath.Join(e.clientHome, ".config"),
		"GOTUNNEL_DAEMON_CHILD=",
		"GORACE=atexit_sleep_ms=0", // race-enabled binaries otherwise sleep 1s at exit
	)
}

// run executes the client-side CLI.
func (e *env) run(args ...string) (string, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(e.commonEnv(),
		"HOME="+e.clientHome,
		"GOTUNNEL_HOME="+filepath.Join(e.clientHome, ".gotunnel"),
		"GOTUNNEL_RUNTIME_DIR="+e.clientRun,
	)
	start := time.Now()
	out, err := cmd.CombinedOutput()
	e.t.Logf("gotunnel %s (%s)", strings.Join(args, " "), time.Since(start).Round(time.Millisecond))
	return string(out), err
}

// runRemote executes the installed agent binary on the "destination".
func (e *env) runRemote(args ...string) (string, error) {
	cmd := exec.Command(filepath.Join(e.remoteHome, ".gotunnel", "bin", "gotunnel"), args...)
	cmd.Env = append(e.commonEnv(),
		"HOME="+e.remoteHome,
		"GOTUNNEL_HOME="+filepath.Join(e.remoteHome, ".gotunnel"),
		"GOTUNNEL_RUNTIME_DIR="+e.remoteRun,
	)
	start := time.Now()
	out, err := cmd.CombinedOutput()
	e.t.Logf("remote: gotunnel %s (%s)", strings.Join(args, " "), time.Since(start).Round(time.Millisecond))
	return string(out), err
}

func (e *env) mustRun(args ...string) string {
	e.t.Helper()
	out, err := e.run(args...)
	if err != nil {
		e.t.Fatalf("gotunnel %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func (e *env) status() (*client.Status, error) {
	out, err := e.run("status", "--json")
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err, out)
	}
	var st client.Status
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (e *env) waitStatus(what string, cond func(*client.Status) bool) *client.Status {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		st, err := e.status()
		if err == nil && cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			logs, _ := os.ReadFile(filepath.Join(e.clientHome, ".gotunnel", "logs", "client.log"))
			e.t.Fatalf("timed out waiting for %s (last status err: %v)\nclient log:\n%s", what, err, logs)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *env) agentStatus() *proto.AdminResponse {
	e.t.Helper()
	out, err := e.runRemote("agent", "status", "--json")
	if err != nil {
		e.t.Fatalf("agent status: %v\n%s", err, out)
	}
	var st proto.AdminResponse
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		e.t.Fatal(err)
	}
	return &st
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

func roundTrip(t *testing.T, addr string, data []byte) {
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
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("round trip via %s: got %d of %d bytes, err %v", addr, len(got), len(data), err)
	}
}

func exchange(t *testing.T, c net.Conn, msg string) {
	t.Helper()
	c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != msg {
		t.Fatalf("exchange %q: got %q, %v", msg, buf, err)
	}
}

func TestEndToEnd(t *testing.T) {
	e := newEnv(t)
	echo := startEcho(t)
	localService := startEcho(t)
	fwd, socks, remote := freePort(t), freePort(t), freePort(t)
	writeConfig := func(extraForwards string) {
		cfg := fmt.Sprintf(`ssh:
  host: fake
  ssh_command: %s
connection:
  ping_interval: 1s
  dead_after: 5s
  connect_timeout: 20s
daemon:
  log_level: debug
forwards:
  - name: echo
    listen: %s
    target: %s
    mode: always
    priority: interactive
%ssocks:
  - listen: %s
reverse:
  - name: back
    remote_listen: %s
    target: %s
`, e.fakeSSH, fwd, echo, extraForwards, socks, remote, localService)
		if err := os.WriteFile(e.cfgPath, []byte(cfg), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig("")

	// Install the agent on the "destination" through ssh.
	out := e.mustRun("install-agent")
	if !strings.Contains(out, "installed: gotunnel") {
		t.Fatalf("install-agent output:\n%s", out)
	}

	// doctor connects through ssh and finds the agent.
	out = e.mustRun("doctor")
	if !strings.Contains(out, "same as this client") {
		t.Fatalf("doctor output:\n%s", out)
	}

	// Start the client daemon in the background.
	out = e.mustRun("up", "-d")
	if !strings.Contains(out, "running in the background") {
		t.Fatalf("up -d output:\n%s", out)
	}
	t.Cleanup(func() {
		e.run("down")
		e.runRemote("agent", "stop")
	})
	if out, err := e.run("up", "-d"); err == nil || !strings.Contains(out, "already running") {
		t.Fatalf("second up -d: %v\n%s", err, out)
	}
	e.waitStatus("connected", func(st *client.Status) bool { return st.Transport.State == client.StateConnected })
	roundTrip(t, fwd, bytes.Repeat([]byte("through the tunnel "), 10000))

	// agentd was started on demand by the relay and holds one session.
	ast := e.agentStatus()
	if ast.Managed != "auto" || len(ast.Sessions) != 1 || !ast.Sessions[0].Attached {
		t.Fatalf("agent status = %+v", ast)
	}

	// A long-lived connection survives a link drop (the session resumes).
	conn, err := net.Dial("tcp", fwd)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	exchange(t, conn, "before")
	e.mustRun("reconnect")
	e.waitStatus("resume", func(st *client.Status) bool {
		return st.Transport.Resumes >= 1 && st.Transport.State == client.StateConnected
	})
	exchange(t, conn, "after the link was replaced")

	// SOCKS: host names resolve on the destination.
	_, echoPort, _ := net.SplitHostPort(echo)
	sc, err := net.Dial("tcp", socks)
	if err != nil {
		t.Fatal(err)
	}
	sc.Write([]byte{5, 1, 0})
	io.ReadFull(sc, make([]byte, 2))
	host := "localhost"
	req := append([]byte{5, 1, 0, 3, byte(len(host))}, host...)
	var port int
	fmt.Sscan(echoPort, &port)
	req = append(req, byte(port>>8), byte(port))
	sc.Write(req)
	reply := make([]byte, 10)
	if _, err := io.ReadFull(sc, reply); err != nil || reply[1] != 0 {
		t.Fatalf("socks reply %v %v", reply, err)
	}
	exchange(t, sc, "via socks")
	sc.Close()

	// Reverse forward: the destination reaches a service on the client.
	e.waitStatus("reverse listening", func(st *client.Status) bool {
		return len(st.Reverse) == 1 && st.Reverse[0].State == "listening"
	})
	roundTrip(t, remote, []byte("destination to client"))

	// Runtime changes through the control socket.
	extra := freePort(t)
	e.mustRun("forward", "add", "extra", "--listen", extra, "--target", echo)
	roundTrip(t, extra, []byte("runtime forward"))
	e.mustRun("forward", "rm", "extra")
	if c, err := net.DialTimeout("tcp", extra, time.Second); err == nil {
		c.Close()
		t.Fatal("removed forward still listening")
	}

	// Reload picks up a forward added to the file.
	reloaded := freePort(t)
	writeConfig(fmt.Sprintf("  - name: reloaded\n    listen: %s\n    target: %s\n", reloaded, echo))
	e.mustRun("reload")
	roundTrip(t, reloaded, []byte("added by reload"))

	// Restarting agentd ends the session; the client opens a new one.
	if out, err := e.runRemote("agent", "stop"); err != nil {
		t.Fatalf("agent stop: %v\n%s", err, out)
	}
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection survived an agent restart")
	}
	e.waitStatus("new session", func(st *client.Status) bool {
		return st.Transport.Sessions == 2 && st.Transport.State == client.StateConnected
	})
	roundTrip(t, fwd, []byte("new session"))

	// down stops the daemon and tells the agent to drop the session.
	out = e.mustRun("down")
	if !strings.Contains(out, "gotunnel stopped") {
		t.Fatalf("down output:\n%s", out)
	}
	if out, err := e.run("status"); err == nil || !strings.Contains(out, "not running") {
		t.Fatalf("status after down: %v\n%s", err, out)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(e.agentStatus().Sessions) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("agent kept the session after the client said goodbye")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
