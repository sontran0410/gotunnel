package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sontran0410/gotunnel/internal/daemon"
	"github.com/sontran0410/gotunnel/internal/paths"
	"github.com/sontran0410/gotunnel/internal/proto"
	"github.com/sontran0410/gotunnel/internal/version"
)

// AutoIdleExit is the idle exit of an agentd started on demand by a relay.
const AutoIdleExit = time.Hour

// ErrNotRunning is returned when agentd is not running.
var ErrNotRunning = errors.New("agentd is not running")

// RunOptions configures "gotunnel agent run".
type RunOptions struct {
	IdleExit  time.Duration
	Managed   string
	LogLevel  string
	LogStderr bool
}

// Run is agentd's main function. It returns when ctx ends, when stopped
// through the admin socket, or after the idle exit.
func Run(ctx context.Context, opts RunOptions) error {
	if err := paths.EnsureDir(paths.RunDir()); err != nil {
		return err
	}
	lock, err := daemon.AcquireLock(paths.AgentLock())
	if err != nil {
		if errors.Is(err, daemon.ErrLocked) {
			pid, _ := daemon.LockHolder(paths.AgentLock())
			return fmt.Errorf("agentd is already running (pid %d)", pid)
		}
		return err
	}
	defer lock.Release()

	log, closer, err := daemon.NewLogger(opts.LogLevel, paths.AgentLog(), opts.LogStderr)
	if err != nil {
		return err
	}
	defer closer.Close()

	sock := paths.AgentSocket()
	os.Remove(sock) // stale socket of a dead agentd; we hold the lock
	l, err := net.Listen("unix", sock)
	if err != nil {
		return err
	}
	if err := os.Chmod(sock, 0o600); err != nil {
		l.Close()
		return err
	}
	srv := NewServer(Options{IdleExit: opts.IdleExit, Managed: opts.Managed, Logger: log})
	log.Info("agentd started", "version", version.Version, "pid", os.Getpid(), "socket", sock,
		"managed", opts.Managed, "idle_exit", opts.IdleExit)
	go func() {
		select {
		case <-ctx.Done():
			srv.Shutdown()
		case <-srv.Done():
		}
	}()
	err = srv.Serve(l)
	log.Info("agentd stopped")
	return err
}

// RunRelay is "gotunnel agent stdio": sshd runs it for every link. It pipes
// the ssh channel (stdin/stdout) to agentd, starting agentd if needed.
func RunRelay(log *slog.Logger) error {
	if err := proto.WritePreamble(os.Stdout); err != nil {
		return err
	}
	conn, err := Connect(true, log)
	if err != nil {
		return err
	}
	defer conn.Close()
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(conn, os.Stdin)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(os.Stdout, conn)
		done <- struct{}{}
	}()
	<-done
	return nil
}

// RunStandalone serves one link on stdin/stdout inside this process, without
// agentd. Sessions cannot survive a broken link in this mode.
func RunStandalone(log *slog.Logger) error {
	if err := proto.WritePreamble(os.Stdout); err != nil {
		return err
	}
	srv := NewServer(Options{Logger: log, Managed: "standalone"})
	c := &stdioConn{closed: make(chan struct{})}
	srv.HandleConn(c)
	<-c.closed
	srv.Shutdown()
	return nil
}

type stdioConn struct {
	closed chan struct{}
	once   sync.Once
}

func (c *stdioConn) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (c *stdioConn) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (c *stdioConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		os.Stdin.Close()
		os.Stdout.Close()
	})
	return nil
}

// Connect dials agentd's socket. With spawn set, an idle agentd running an
// older version is replaced and a missing agentd is started.
func Connect(spawn bool, log *slog.Logger) (net.Conn, error) {
	sock := paths.AgentSocket()
	if spawn {
		replaceOutdated(log)
	}
	c, err := net.Dial("unix", sock)
	if err == nil || !spawn {
		return c, err
	}
	if _, err := Start(AutoIdleExit, "auto"); err != nil && !errors.Is(err, ErrRunning) {
		return nil, err
	}
	return net.Dial("unix", sock)
}

// replaceOutdated restarts an agentd of another version with the same
// settings if it has no sessions, so a freshly installed binary takes over.
// Service-managed agentds are left to their service manager.
func replaceOutdated(log *slog.Logger) {
	st, err := Admin(proto.AdminStatus)
	if err != nil || st.Version == version.Version {
		return
	}
	if len(st.Sessions) > 0 || st.Managed == "systemd" || st.Managed == "launchd" {
		log.Info("agentd runs another version; keeping it", "running", st.Version, "installed", version.Version)
		return
	}
	if _, err := Admin(proto.AdminStop); err != nil || !waitStopped(st.PID, 5*time.Second) {
		return
	}
	idle, _ := time.ParseDuration(st.IdleExit)
	if _, err := Start(idle, st.Managed); err != nil {
		log.Warn("restarting agentd failed", "err", err)
	}
}

// ErrRunning is returned by Start when agentd is already running.
var ErrRunning = errors.New("agentd is already running")

// Start launches agentd in the background and waits until it answers.
func Start(idleExit time.Duration, managed string) (pid int, err error) {
	if err := paths.EnsureDir(paths.RunDir()); err != nil {
		return 0, err
	}
	if err := paths.EnsureDir(paths.LogDir()); err != nil {
		return 0, err
	}
	if st, err := Admin(proto.AdminStatus); err == nil {
		return st.PID, ErrRunning
	}
	args := []string{"agent", "run", "--idle-exit", idleExit.String(), "--managed", managed}
	pid, exited, err := daemon.Spawn(args, paths.AgentOut())
	if err != nil {
		return 0, err
	}
	err = daemon.WaitReady(func() error { _, err := Admin(proto.AdminStatus); return err }, exited, 10*time.Second)
	if err != nil {
		// Another relay may have won the race to start agentd.
		if st, e := Admin(proto.AdminStatus); e == nil {
			return st.PID, nil
		}
		return 0, fmt.Errorf("%w\n%s", err, daemon.TailLines(paths.AgentOut(), 10))
	}
	return pid, nil
}

// Stop asks agentd to shut down and waits for that process to exit. (A relay
// may start a new agentd right away when a client reconnects.)
func Stop(timeout time.Duration) error {
	st, err := Admin(proto.AdminStop)
	if err != nil {
		return ErrNotRunning
	}
	if !waitStopped(st.PID, timeout) {
		return fmt.Errorf("agentd (pid %d) did not stop within %s", st.PID, timeout)
	}
	return nil
}

// waitStopped waits until the agentd with the given pid no longer holds the lock.
func waitStopped(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if holder, held := daemon.LockHolder(paths.AgentLock()); !held || holder != pid {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// Admin sends an admin command to agentd.
func Admin(cmd string) (*proto.AdminResponse, error) {
	c, err := net.DialTimeout("unix", paths.AgentSocket(), 2*time.Second)
	if err != nil {
		return nil, ErrNotRunning
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if err := proto.WriteJSONFrame(c, proto.FrameAdmin, proto.AdminRequest{Cmd: cmd}); err != nil {
		return nil, err
	}
	typ, payload, err := proto.ReadFrame(c, nil)
	if err != nil {
		return nil, err
	}
	if typ != proto.FrameAdminResp {
		return nil, fmt.Errorf("unexpected admin reply type 0x%02x", typ)
	}
	var resp proto.AdminResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return &resp, nil
}
