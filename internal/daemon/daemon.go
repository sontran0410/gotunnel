// Package daemon provides what both gotunnel daemons need to run in the
// background: a single-instance lock, self-daemonization, log files with
// rotation and helpers to read them.
package daemon

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ErrLocked is returned when another instance holds the lock.
var ErrLocked = errors.New("another instance is already running")

// EnvChild is set in the environment of processes started by Spawn.
const EnvChild = "GOTUNNEL_DAEMON_CHILD"

// Lock is an exclusive advisory lock held for the life of the process. The
// kernel releases it when the process dies, so stale locks cannot happen.
type Lock struct{ f *os.File }

// AcquireLock takes the lock at path and records our PID in it.
func AcquireLock(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	return &Lock{f: f}, nil
}

// Release drops the lock.
func (l *Lock) Release() {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	l.f.Close()
}

// LockHolder returns the PID recorded in the lock file at path, if the lock
// is currently held.
func LockHolder(path string) (pid int, held bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return 0, false
	}
	data, _ := io.ReadAll(f)
	pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
	return pid, true
}

// Spawn starts the current executable with args as a detached background
// process (new session, stdin /dev/null, stdout/stderr appended to outPath).
// The returned channel receives the process exit status.
func Spawn(args []string, outPath string) (pid int, exited <-chan error, err error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, nil, err
	}
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, nil, err
	}
	defer out.Close()
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		return 0, nil, err
	}
	defer devnull.Close()

	cmd := exec.Command(exe, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = devnull, out, out
	cmd.Env = append(os.Environ(), EnvChild+"=1")
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, nil, err
	}
	ch := make(chan error, 1)
	go func() { ch <- cmd.Wait() }()
	return cmd.Process.Pid, ch, nil
}

// WaitReady polls ready until it succeeds, the spawned process exits, or the
// timeout passes.
func WaitReady(ready func() error, exited <-chan error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := ready()
		if err == nil {
			return nil
		}
		select {
		case werr := <-exited:
			if werr == nil {
				return errors.New("background process exited")
			}
			return fmt.Errorf("background process exited: %v", werr)
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("background process not ready after %s: %v", timeout, err)
		}
	}
}

// IsChild reports whether this process was started by Spawn.
func IsChild() bool { return os.Getenv(EnvChild) == "1" }

// ManagedBy guesses what started this process.
func ManagedBy() string {
	switch {
	case os.Getenv("INVOCATION_ID") != "":
		return "systemd"
	case os.Getenv("XPC_SERVICE_NAME") != "" && os.Getenv("XPC_SERVICE_NAME") != "0":
		return "launchd"
	case IsChild():
		return "background"
	}
	return "foreground"
}

// IsTerminal reports whether f is a character device (a terminal).
func IsTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// NewLogger creates a logger writing to a rotating file and, optionally,
// to stderr. The returned closer flushes and closes the file.
func NewLogger(level, file string, stderr bool) (*slog.Logger, io.Closer, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	var writers []io.Writer
	var closer io.Closer = nopCloser{}
	if file != "" {
		rf, err := OpenRotating(file, 10<<20, 3)
		if err != nil {
			return nil, nil, err
		}
		writers = append(writers, rf)
		closer = rf
	}
	if stderr || len(writers) == 0 {
		writers = append(writers, os.Stderr)
	}
	h := slog.NewTextHandler(io.MultiWriter(writers...), &slog.HandlerOptions{Level: lvl})
	return slog.New(h), closer, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// TailLines returns the last n lines of the file at path.
func TailLines(path string, n int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
