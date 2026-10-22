package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"gotunnel/internal/client"
	"gotunnel/internal/config"
	"gotunnel/internal/service"
	"gotunnel/internal/version"
)

type doctor struct{ failures int }

func (d *doctor) line(level, topic, msg string) {
	fmt.Printf("  %-5s %-9s %s\n", level, topic, msg)
}
func (d *doctor) ok(topic, msg string)   { d.line("ok", topic, msg) }
func (d *doctor) info(topic, msg string) { d.line("info", topic, msg) }
func (d *doctor) warn(topic, msg string) { d.line("warn", topic, msg) }
func (d *doctor) fail(topic, msg string) {
	d.failures++
	d.line("FAIL", topic, msg)
}

func cmdDoctor(args []string) error {
	fs := newFlags("doctor")
	cfgPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	d := &doctor{}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		d.fail("config", err.Error())
		return exitCode(1)
	}
	d.ok("config", fmt.Sprintf("%s (%d forwards, %d socks, %d reverse)", cfg.Path, len(cfg.Forwards), len(cfg.Socks), len(cfg.Reverse)))

	sshPath, err := exec.LookPath(cfg.SSH.Command)
	if err != nil {
		d.fail("ssh", fmt.Sprintf("%q not found in PATH", cfg.SSH.Command))
	} else {
		out, _ := exec.Command(sshPath, "-V").CombinedOutput()
		d.ok("ssh", fmt.Sprintf("%s (%s)", sshPath, strings.TrimSpace(string(out))))
		d.route(cfg, sshPath)
	}

	st, err := fetchStatus()
	running := err == nil
	switch {
	case !running:
		d.info("daemon", "not running (start it with 'gotunnel up -d' or 'gotunnel service install')")
	case st.Transport.State == client.StateConnected:
		d.ok("daemon", fmt.Sprintf("running (pid %d, %s), transport connected, rtt %s", st.PID, st.Managed, millis(st.Transport.RTTMillis)))
	default:
		msg := fmt.Sprintf("running (pid %d, %s), transport %s", st.PID, st.Managed, st.Transport.State)
		if st.Transport.LastError != "" {
			msg += ": " + st.Transport.LastError
		}
		d.warn("daemon", msg)
	}

	d.listeners(cfg, st)

	m := service.Detect()
	s, _ := m.Status("client")
	d.info("service", m.Kind()+": "+s)

	if running && st.Transport.State == client.StateConnected {
		d.versionCheck(st.Transport.AgentVersion)
	} else if sshPath != "" {
		d.agentCheck(cfg)
	}

	if d.failures > 0 {
		return exitCode(1)
	}
	return nil
}

// route shows the jump chain ssh will use and checks ProxyCommand programs.
func (d *doctor) route(cfg *config.Config, sshPath string) {
	visited := map[string]bool{}
	var chain []string
	var walk func(name string, depth int)
	walk = func(name string, depth int) {
		if depth > 10 || visited[name] {
			return
		}
		visited[name] = true
		h, err := resolveHost(cfg, sshPath, name)
		if err != nil {
			d.warn("route", fmt.Sprintf("ssh -G %s: %v", name, err))
			return
		}
		if pj := h["proxyjump"]; pj != "" && pj != "none" {
			for i, j := range strings.Split(pj, ",") {
				j = jumpHostName(j)
				if i == 0 {
					walk(j, depth+1)
				} else {
					chain = append(chain, j)
				}
			}
		}
		label := name
		if hn := h["hostname"]; hn != "" && hn != name {
			label += " (" + hn + ")"
		}
		if pc := h["proxycommand"]; pc != "" && pc != "none" {
			label += " [via " + strings.Fields(pc)[0] + "]"
			d.proxyCommand(name, pc)
		}
		chain = append(chain, label)
	}
	walk(cfg.SSH.Host, 0)
	if len(chain) > 0 {
		d.info("route", "this machine → "+strings.Join(chain, " → "))
	}
}

func resolveHost(cfg *config.Config, sshPath, name string) (map[string]string, error) {
	args := append(append([]string{}, cfg.SSH.ExtraArgs...), "-G", name)
	out, err := exec.Command(sshPath, args...).Output()
	if err != nil {
		return nil, err
	}
	h := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), " ")
		if ok {
			if _, dup := h[k]; !dup {
				h[k] = v
			}
		}
	}
	return h, nil
}

// jumpHostName strips user@ and :port from a ProxyJump entry.
func jumpHostName(j string) string {
	j = strings.TrimSpace(j)
	j = strings.TrimPrefix(j, "ssh://")
	if i := strings.LastIndex(j, "@"); i >= 0 {
		j = j[i+1:]
	}
	if h, _, err := net.SplitHostPort(j); err == nil {
		return h
	}
	return j
}

func (d *doctor) proxyCommand(host, pc string) {
	prog := strings.Fields(pc)[0]
	path, err := exec.LookPath(prog)
	if err != nil {
		d.fail("proxy", fmt.Sprintf("%s uses ProxyCommand %q but %s is not in PATH", host, pc, prog))
		return
	}
	d.ok("proxy", fmt.Sprintf("%s: %s (%s)", host, pc, path))
	if runtime.GOOS == "darwin" {
		switch filepath.Dir(path) {
		case "/usr/bin", "/bin", "/usr/sbin", "/sbin":
		default:
			d.info("path", fmt.Sprintf("%s is outside launchd's default PATH; 'gotunnel service install' records your PATH so the service finds it", path))
		}
	}
}

func (d *doctor) listeners(cfg *config.Config, st *client.Status) {
	byListen := map[string]client.ListenStatus{}
	if st != nil {
		for _, l := range append(append([]client.ListenStatus{}, st.Forwards...), st.Socks...) {
			byListen[l.Listen] = l
		}
	}
	check := func(kind, name, addr string) {
		topic := "listen"
		if l, ok := byListen[addr]; ok {
			if l.Listening {
				d.ok(topic, fmt.Sprintf("%s %s on %s", kind, name, addr))
			} else {
				d.fail(topic, fmt.Sprintf("%s %s cannot listen on %s: %s", kind, name, addr, l.Error))
			}
			return
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			d.fail(topic, fmt.Sprintf("%s %s: %s is in use by another program (%v)", kind, name, addr, err))
			return
		}
		ln.Close()
		d.ok(topic, fmt.Sprintf("%s %s: %s is free", kind, name, addr))
	}
	for _, f := range cfg.Forwards {
		check("forward", f.Name, f.Listen)
	}
	for _, s := range cfg.Socks {
		check("socks", s.Name, s.Listen)
	}
}

func (d *doctor) versionCheck(agentVersion string) {
	if agentVersion == version.Version {
		d.ok("agent", "version "+agentVersion+" (same as this client)")
	} else {
		d.warn("agent", fmt.Sprintf("version %s differs from this client (%s); run 'gotunnel install-agent'", agentVersion, version.Version))
	}
}

// agentCheck connects through ssh exactly like the daemon would and asks the
// installed agent for its version.
func (d *doctor) agentCheck(cfg *config.Config) {
	fmt.Printf("  ...   agent     connecting through ssh to %s (this can take a while)\n", cfg.SSH.Host)
	t := sshTransport(cfg, nil)
	t.RemoteCommand = remoteVersionCommand(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Connection.ConnectTimeout.D())
	defer cancel()
	cmd := exec.CommandContext(ctx, t.Command, t.Args()...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	err := cmd.Run()
	took := time.Since(start).Round(100 * time.Millisecond)
	out := strings.TrimSpace(stdout.String())
	errText := strings.TrimSpace(stderr.String())

	var exitErr *exec.ExitError
	switch {
	case ctx.Err() != nil:
		d.fail("ssh", fmt.Sprintf("no answer within %s; %s", cfg.Connection.ConnectTimeout, sshHint(errText)))
	case err == nil && strings.HasPrefix(lastLine(out), "gotunnel "):
		d.ok("ssh", fmt.Sprintf("connected to %s in %s", cfg.SSH.Host, took))
		v := strings.Fields(lastLine(out))
		d.versionCheck(v[1])
	case errors.As(err, &exitErr) && exitErr.ExitCode() == 127:
		d.ok("ssh", fmt.Sprintf("connected to %s in %s", cfg.SSH.Host, took))
		d.fail("agent", "not installed on the destination; run 'gotunnel install-agent'")
	case errors.As(err, &exitErr) && exitErr.ExitCode() == 255:
		d.fail("ssh", sshHint(errText))
	default:
		d.fail("agent", fmt.Sprintf("unexpected answer: %v %s %s", err, out, errText))
	}
}

func sshHint(stderr string) string {
	last := lastLine(stderr)
	switch {
	case strings.Contains(stderr, "Permission denied"):
		return last + " — the key was refused or is not available without a passphrase; load it with ssh-add (or use the macOS keychain) since a daemon cannot type passphrases"
	case strings.Contains(stderr, "Host key verification failed"):
		return last + " — connect once by hand with 'ssh <host>' to accept the host key"
	case strings.Contains(stderr, "Could not resolve hostname"):
		return last + " — check HostName in ~/.ssh/config"
	case strings.Contains(stderr, "timed out"):
		return last + " — a hop is unreachable or very slow; try 'ssh -v <host>'"
	case last == "":
		return "ssh failed without output; try 'ssh -v <host>' by hand"
	}
	return last
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "\n"); i >= 0 {
		return strings.TrimSpace(s[i+1:])
	}
	return s
}
