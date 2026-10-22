package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/sontran0410/gotunnel/internal/agent"
	"github.com/sontran0410/gotunnel/internal/daemon"
	"github.com/sontran0410/gotunnel/internal/paths"
	"github.com/sontran0410/gotunnel/internal/proto"
	"github.com/sontran0410/gotunnel/internal/service"
)

func cmdAgent(args []string) error {
	if len(args) == 0 {
		return usageError("usage: gotunnel agent start|stop|status|logs|run|service|stdio")
	}
	sub, args := args[0], args[1:]
	switch sub {
	case "stdio":
		fs := newFlags("agent stdio")
		standalone := fs.Bool("standalone", false, "serve the link in this process (no agentd, no resume)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		// stdout carries the protocol; diagnostics go to stderr, which the
		// client shows in its log as "ssh: ..." lines.
		log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
		if *standalone {
			return agent.RunStandalone(log)
		}
		return agent.RunRelay(log)

	case "run":
		fs := newFlags("agent run")
		idle := fs.Duration("idle-exit", 0, "exit after having no sessions for this long (0: never)")
		managed := fs.String("managed", daemon.ManagedBy(), "how agentd was started (shown in status)")
		level := fs.String("log-level", "info", "debug, info, warn or error")
		if err := fs.Parse(args); err != nil {
			return err
		}
		signal.Ignore(syscall.SIGHUP)
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		return agent.Run(ctx, agent.RunOptions{
			IdleExit:  *idle,
			Managed:   *managed,
			LogLevel:  *level,
			LogStderr: daemon.IsTerminal(os.Stderr),
		})

	case "start":
		fs := newFlags("agent start")
		managed := fs.String("managed", "manual", "how agentd was started (shown in status)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		pid, err := agent.Start(0, *managed)
		if errors.Is(err, agent.ErrRunning) {
			fmt.Printf("agentd is already running (pid %d)\n", pid)
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("agentd started (pid %d); logs: %s\n", pid, paths.AgentLog())
		return nil

	case "stop":
		if err := newFlags("agent stop").Parse(args); err != nil {
			return err
		}
		if err := agent.Stop(10 * time.Second); err != nil {
			return err
		}
		fmt.Println("agentd stopped")
		return nil

	case "status":
		fs := newFlags("agent status")
		asJSON := fs.Bool("json", false, "print JSON")
		if err := fs.Parse(args); err != nil {
			return err
		}
		st, err := agent.Admin(proto.AdminStatus)
		if err != nil {
			fmt.Println("agentd is not running (a client connecting through ssh starts it automatically)")
			m := service.Detect()
			s, _ := m.Status("agent")
			fmt.Printf("service (%s): %s\n", m.Kind(), s)
			return exitCode(3)
		}
		if *asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(st)
		}
		printAgentStatus(st)
		return nil

	case "logs":
		fs := newFlags("agent logs")
		follow := fs.Bool("f", false, "keep printing new lines")
		n := fs.Int("n", 50, "number of lines to show")
		if err := fs.Parse(args); err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return daemon.Follow(ctx, paths.AgentLog(), *n, *follow, os.Stdout)

	case "service":
		return cmdAgentService(args)
	}
	return usageError("unknown agent command %q", sub)
}

func printAgentStatus(st *proto.AdminResponse) {
	idle := "never"
	if st.IdleExit != "" {
		idle = "after " + st.IdleExit + " without sessions"
	}
	fmt.Printf("agentd %s · pid %d (%s) · up %s · idle exit: %s\n", st.Version, st.PID, st.Managed, age(st.Started), idle)
	if len(st.Sessions) == 0 {
		fmt.Println("no sessions")
		return
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SESSION\tSTATE\tAGE\tSTREAMS\tSENT\tRECEIVED\tREVERSE LISTENERS")
	for _, s := range st.Sessions {
		state := "attached"
		if !s.Attached {
			state = fmt.Sprintf("detached %s (ttl %s)", age(s.Detached), s.TTL)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%v\n", s.ID[:8], state, age(s.Created), s.Streams,
			bytesH(s.BytesOut), bytesH(s.BytesIn), s.Listeners)
	}
	tw.Flush()
}

func serviceEnv(withPath bool) map[string]string {
	env := map[string]string{}
	if withPath {
		env["PATH"] = os.Getenv("PATH")
	}
	for _, k := range []string{"GOTUNNEL_HOME", "GOTUNNEL_RUNTIME_DIR", "XDG_CONFIG_HOME"} {
		if v := os.Getenv(k); v != "" {
			env[k] = v
		}
	}
	return env
}

func cmdAgentService(args []string) error {
	if len(args) == 0 {
		return usageError("usage: gotunnel agent service install|uninstall|status")
	}
	m := service.Detect()
	switch args[0] {
	case "install":
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		// A running agentd holds the lock the service instance needs.
		if st, err := agent.Admin(proto.AdminStatus); err == nil && st.Managed != m.Kind() {
			if len(st.Sessions) > 0 {
				fmt.Printf("stopping the running agentd (%d sessions will be lost; clients reconnect automatically)\n", len(st.Sessions))
			}
			if err := agent.Stop(10 * time.Second); err != nil && !errors.Is(err, agent.ErrNotRunning) {
				return err
			}
		}
		for _, d := range []string{paths.RunDir(), paths.LogDir()} {
			if err := paths.EnsureDir(d); err != nil {
				return err
			}
		}
		notes, err := m.Install(service.Spec{
			Name:        "agent",
			Description: "gotunnel agent",
			Args:        []string{exe, "agent", "run", "--idle-exit", "0s", "--managed", m.Kind()},
			CronArgs:    []string{exe, "agent", "start", "--managed", "cron"},
			Env:         serviceEnv(false),
			LogPath:     paths.AgentOut(),
		})
		if err != nil {
			return err
		}
		for _, n := range notes {
			fmt.Println(n)
		}
		if m.Kind() == "cron" { // cron only acts at the next boot
			if _, err := agent.Start(0, "cron"); err != nil && !errors.Is(err, agent.ErrRunning) {
				return err
			}
		}
		if err := daemon.WaitReady(func() error { _, err := agent.Admin(proto.AdminStatus); return err }, nil, 10*time.Second); err != nil {
			return fmt.Errorf("service installed but agentd does not answer: %v", err)
		}
		fmt.Println("agentd is running under", m.Kind())
		return nil
	case "uninstall":
		if err := m.Uninstall("agent"); err != nil {
			return err
		}
		fmt.Printf("%s service removed (agentd still starts on demand when a client connects)\n", m.Kind())
		return nil
	case "status":
		s, err := m.Status("agent")
		if err != nil {
			return err
		}
		fmt.Printf("service (%s): %s\n", m.Kind(), s)
		return nil
	}
	return usageError("unknown agent service command %q", args[0])
}

func cmdService(args []string) error {
	if len(args) == 0 {
		return usageError("usage: gotunnel service install|uninstall|status [-c config]")
	}
	sub := args[0]
	fs := newFlags("service " + sub)
	cfgPath := configFlag(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	m := service.Detect()
	switch sub {
	case "install":
		cfg, err := loadConfig(*cfgPath)
		if err != nil {
			return err
		}
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		// A manually started client holds the lock the service instance needs.
		if st, err := fetchStatus(); err == nil && st.Managed != m.Kind() {
			fmt.Println("stopping the manually started gotunnel first")
			if err := cmdDown(nil); err != nil {
				return err
			}
		}
		for _, d := range []string{paths.RunDir(), paths.LogDir()} {
			if err := paths.EnsureDir(d); err != nil {
				return err
			}
		}
		notes, err := m.Install(service.Spec{
			Name:        "client",
			Description: "gotunnel client",
			Args:        []string{exe, "up", "-c", cfg.Path},
			CronArgs:    []string{exe, "up", "-d", "-c", cfg.Path},
			// launchd and systemd start services with a minimal PATH; keep ours
			// so ProxyCommands such as openssl resolve as in a terminal.
			Env:     serviceEnv(true),
			LogPath: paths.ClientOut(),
		})
		if err != nil {
			return err
		}
		for _, n := range notes {
			fmt.Println(n)
		}
		if m.Kind() == "cron" {
			if err := cmdUp([]string{"-d", "-c", cfg.Path}); err != nil {
				return err
			}
		}
		if err := daemon.WaitReady(func() error { _, err := fetchStatus(); return err }, nil, 15*time.Second); err != nil {
			return fmt.Errorf("service installed but gotunnel does not answer: %v\n%s", err, daemon.TailLines(paths.ClientOut(), 10))
		}
		fmt.Println("gotunnel is running under", m.Kind(), "- check with 'gotunnel status'")
		return nil
	case "uninstall":
		if err := m.Uninstall("client"); err != nil {
			return err
		}
		fmt.Printf("%s service removed\n", m.Kind())
		return nil
	case "status":
		s, err := m.Status("client")
		if err != nil {
			return err
		}
		fmt.Printf("service (%s): %s\n", m.Kind(), s)
		return nil
	}
	return usageError("unknown service command %q", sub)
}
