package transport

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// SSH opens links by running the system ssh client, which already knows the
// jump chain from ~/.ssh/config, and starting the agent relay remotely.
type SSH struct {
	Command       string   // ssh binary
	Host          string   // alias from ~/.ssh/config
	ExtraArgs     []string // extra ssh options placed before the host
	Compression   bool     // ssh -C
	RemoteCommand string   // command that starts the relay on the destination
	Log           *slog.Logger
}

// Args returns the ssh command line (without the binary).
func (t *SSH) Args() []string {
	args := []string{
		"-T", "-e", "none", // no tty: the pipe must stay binary-clean
		"-o", "BatchMode=yes", // never prompt: a daemon has nobody to ask
		"-o", "ControlPath=none", // we own the lifetime of this connection
		"-o", "ClearAllForwardings=yes", // ignore LocalForward/RemoteForward from the alias
		"-o", "RemoteCommand=none",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
	}
	if t.Compression {
		args = append(args, "-C")
	}
	args = append(args, t.ExtraArgs...)
	return append(args, "--", t.Host, t.RemoteCommand)
}

func (t *SSH) String() string { return "ssh " + t.Host }

// Dial starts ssh and returns its stdio as a link.
func (t *SSH) Dial(ctx context.Context) (Link, error) {
	log := t.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		inR.Close()
		inW.Close()
		return nil, err
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		inR.Close()
		inW.Close()
		outR.Close()
		outW.Close()
		return nil, err
	}

	cmd := exec.Command(t.Command, t.Args()...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, outW, errW
	// A process group lets us kill ssh together with its ProxyCommand
	// children (openssl s_client keeps running after its stdin closes).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	log.Debug("starting ssh", "args", strings.Join(cmd.Args, " "))
	startErr := cmd.Start()
	inR.Close()
	outW.Close()
	errW.Close()
	if startErr != nil {
		inW.Close()
		outR.Close()
		errR.Close()
		return nil, fmt.Errorf("start %s: %w", t.Command, startErr)
	}

	l := &sshLink{
		cmd:        cmd,
		stdin:      inW,
		stdout:     outR,
		exited:     make(chan struct{}),
		stderrDone: make(chan struct{}),
	}
	go l.readStderr(errR, log)
	go func() {
		l.waitErr = cmd.Wait()
		close(l.exited)
	}()
	return l, nil
}

type sshLink struct {
	cmd        *exec.Cmd
	stdin      *os.File
	stdout     *os.File
	exited     chan struct{}
	waitErr    error
	stderrDone chan struct{}

	mu     sync.Mutex
	stderr []string

	closeOnce sync.Once
}

func (l *sshLink) Read(p []byte) (int, error)  { return l.stdout.Read(p) }
func (l *sshLink) Write(p []byte) (int, error) { return l.stdin.Write(p) }

// Close terminates ssh and everything it started. Closing stdin first lets
// ssh deliver what was already written (such as a final BYE) and end the
// session by itself; a dead link gets killed after a short grace period.
func (l *sshLink) Close() error {
	l.closeOnce.Do(func() {
		l.stdin.Close()
		pgid := l.cmd.Process.Pid
		select {
		case <-l.exited:
		case <-time.After(time.Second):
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
			select {
			case <-l.exited:
			case <-time.After(2 * time.Second):
			}
		}
		// ProxyCommand children (openssl s_client) may outlive ssh.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		l.stdout.Close()
	})
	return nil
}

func (l *sshLink) readStderr(r *os.File, log *slog.Logger) {
	defer close(l.stderrDone)
	defer r.Close()
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		log.Warn("ssh: " + line)
		l.mu.Lock()
		if len(l.stderr) >= 20 {
			l.stderr = l.stderr[1:]
		}
		l.stderr = append(l.stderr, line)
		l.mu.Unlock()
	}
}

// Diagnose describes how ssh ended: its exit status and last error output.
// It waits briefly for ssh to exit so the output is complete.
func (l *sshLink) Diagnose() string {
	select {
	case <-l.exited:
	case <-time.After(time.Second):
	}
	select {
	case <-l.stderrDone:
	case <-time.After(500 * time.Millisecond):
	}
	var parts []string
	select {
	case <-l.exited:
		if l.waitErr != nil {
			parts = append(parts, "ssh "+l.waitErr.Error())
		} else {
			parts = append(parts, "ssh exited")
		}
	default:
	}
	l.mu.Lock()
	if n := len(l.stderr); n > 0 {
		parts = append(parts, strings.Join(l.stderr[max(0, n-3):], " | "))
	}
	l.mu.Unlock()
	return strings.Join(parts, ": ")
}
