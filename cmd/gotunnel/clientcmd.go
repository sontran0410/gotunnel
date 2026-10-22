package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sontran0410/gotunnel/internal/client"
	"github.com/sontran0410/gotunnel/internal/config"
	"github.com/sontran0410/gotunnel/internal/control"
	"github.com/sontran0410/gotunnel/internal/daemon"
	"github.com/sontran0410/gotunnel/internal/paths"
	"github.com/sontran0410/gotunnel/internal/service"
	"github.com/sontran0410/gotunnel/internal/transport"
	"github.com/sontran0410/gotunnel/internal/version"
)

func configFlag(fs *flag.FlagSet) *string {
	return fs.String("c", paths.ConfigPath(), "configuration file")
}

func loadConfig(path string) (*config.Config, error) {
	abs, err := filepath.Abs(paths.Expand(path))
	if err != nil {
		return nil, err
	}
	cfg, err := config.Load(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("no configuration file at %s (start from examples/config.yaml)", abs)
	}
	return cfg, err
}

func sshTransport(cfg *config.Config, log *slog.Logger) *transport.SSH {
	return &transport.SSH{
		Command:       cfg.SSH.Command,
		Host:          cfg.SSH.Host,
		ExtraArgs:     cfg.SSH.ExtraArgs,
		Compression:   cfg.SSH.CompressionEnabled(),
		RemoteCommand: cfg.SSH.RemoteCommand,
		Log:           log,
	}
}

func controlClient() *control.Client { return control.NewClient(paths.ControlSocket()) }

func ctxTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func cmdUp(args []string) error {
	fs := newFlags("up")
	cfgPath := configFlag(fs)
	detach := fs.Bool("d", false, "run in the background")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	for _, d := range []string{paths.RunDir(), paths.LogDir()} {
		if err := paths.EnsureDir(d); err != nil {
			return err
		}
	}
	if !*detach || daemon.IsChild() {
		return runClient(cfg)
	}

	if pid, held := daemon.LockHolder(paths.ClientLock()); held {
		return fmt.Errorf("gotunnel is already running (pid %d)", pid)
	}
	pid, exited, err := daemon.Spawn([]string{"up", "-c", cfg.Path}, paths.ClientOut())
	if err != nil {
		return err
	}
	var st client.Status
	ready := func() error {
		ctx, cancel := ctxTimeout(time.Second)
		defer cancel()
		return controlClient().Do(ctx, http.MethodGet, "/status", nil, &st)
	}
	if err := daemon.WaitReady(ready, exited, 15*time.Second); err != nil {
		return fmt.Errorf("%w\n%s", err, daemon.TailLines(paths.ClientOut(), 15))
	}
	fmt.Printf("gotunnel is running in the background (pid %d)\n", pid)
	printListeners(os.Stdout, &st)
	fmt.Println("check with 'gotunnel status'; logs: 'gotunnel logs -f'")
	return nil
}

// runClient runs the client daemon in this process until it is stopped.
func runClient(cfg *config.Config) error {
	lock, err := daemon.AcquireLock(paths.ClientLock())
	if err != nil {
		if errors.Is(err, daemon.ErrLocked) {
			pid, _ := daemon.LockHolder(paths.ClientLock())
			return fmt.Errorf("gotunnel is already running (pid %d)", pid)
		}
		return err
	}
	defer lock.Release()

	log, closer, err := daemon.NewLogger(cfg.Daemon.LogLevel, cfg.Daemon.LogFile, daemon.IsTerminal(os.Stderr))
	if err != nil {
		return err
	}
	defer closer.Close()

	cl := client.New(cfg, sshTransport(cfg, log), log)
	log.Info("gotunnel starting", "version", version.Version, "pid", os.Getpid(), "config", cfg.Path,
		"managed", daemon.ManagedBy(), "host", cfg.SSH.Host)
	cl.Start()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &daemonAPI{cl: cl, cancel: cancel, log: log}
	l, err := control.Listen(paths.ControlSocket())
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	srv := &http.Server{Handler: control.Handler(api), ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(l)

	sigc := make(chan os.Signal, 4)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for s := range sigc {
			if s == syscall.SIGHUP {
				log.Info("SIGHUP: reloading configuration")
				for _, w := range api.Reload() {
					log.Warn("reload: " + w.Error())
				}
				continue
			}
			log.Info("shutting down", "signal", s.String())
			cancel()
			return
		}
	}()

	cl.Run(ctx)
	signal.Stop(sigc)
	srv.Close()
	log.Info("gotunnel stopped")
	return nil
}

// daemonAPI adapts the client to the control API.
type daemonAPI struct {
	cl     *client.Client
	cancel context.CancelFunc
	log    *slog.Logger
}

func (a *daemonAPI) Status() any                       { return a.cl.Status() }
func (a *daemonAPI) AddForward(f config.Forward) error { return a.cl.AddForward(f) }
func (a *daemonAPI) RemoveForward(n string) error      { return a.cl.RemoveForward(n) }
func (a *daemonAPI) AddReverse(r config.Reverse) error { return a.cl.AddReverse(r) }
func (a *daemonAPI) RemoveReverse(n string) error      { return a.cl.RemoveReverse(n) }
func (a *daemonAPI) Reconnect()                        { a.cl.Supervisor().Reconnect() }

func (a *daemonAPI) Reload() []error {
	cfg, err := config.Load(a.cl.Config().Path)
	if err != nil {
		return []error{err}
	}
	return a.cl.Apply(cfg)
}

func (a *daemonAPI) Shutdown() {
	a.log.Info("shutdown requested")
	a.cancel()
}

func cmdDown(args []string) error {
	if err := newFlags("down").Parse(args); err != nil {
		return err
	}
	ctx, cancel := ctxTimeout(10 * time.Second)
	defer cancel()
	if err := controlClient().Do(ctx, http.MethodPost, "/shutdown", nil, nil); err != nil {
		pid, held := daemon.LockHolder(paths.ClientLock())
		if !held {
			return errors.New("gotunnel is not running")
		}
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			return err
		}
	}
	if !waitUnlocked(paths.ClientLock(), 15*time.Second) {
		return errors.New("gotunnel did not stop within 15s")
	}
	fmt.Println("gotunnel stopped")
	if m := service.Detect(); m.Installed("client") {
		fmt.Printf("note: the %s service starts it again at the next login ('gotunnel service uninstall' disables that)\n", m.Kind())
	}
	return nil
}

func waitUnlocked(lockPath string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, held := daemon.LockHolder(lockPath); !held {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func fetchStatus() (*client.Status, error) {
	ctx, cancel := ctxTimeout(10 * time.Second)
	defer cancel()
	var st client.Status
	if err := controlClient().Do(ctx, http.MethodGet, "/status", nil, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func cmdStatus(args []string) error {
	fs := newFlags("status")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := fetchStatus()
	if errors.Is(err, control.ErrNotRunning) {
		fmt.Println("gotunnel is not running")
		m := service.Detect()
		s, _ := m.Status("client")
		fmt.Printf("service (%s): %s\n", m.Kind(), s)
		fmt.Println("start it with 'gotunnel up -d' or 'gotunnel service install'")
		return exitCode(3)
	}
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	}
	printStatus(os.Stdout, st)
	return nil
}

func cmdLogs(args []string) error {
	fs := newFlags("logs")
	cfgPath := configFlag(fs)
	follow := fs.Bool("f", false, "keep printing new lines")
	n := fs.Int("n", 50, "number of lines to show")
	if err := fs.Parse(args); err != nil {
		return err
	}
	path := paths.ClientLog()
	if cfg, err := loadConfig(*cfgPath); err == nil {
		path = cfg.Daemon.LogFile
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return daemon.Follow(ctx, path, *n, *follow, os.Stdout)
}

func cmdReload(args []string) error {
	if err := newFlags("reload").Parse(args); err != nil {
		return err
	}
	ctx, cancel := ctxTimeout(30 * time.Second)
	defer cancel()
	var res control.ReloadResult
	if err := controlClient().Do(ctx, http.MethodPost, "/reload", nil, &res); err != nil {
		return err
	}
	for _, w := range res.Warnings {
		fmt.Println("warning:", w)
	}
	fmt.Println("configuration reloaded")
	return nil
}

func cmdReconnect(args []string) error {
	if err := newFlags("reconnect").Parse(args); err != nil {
		return err
	}
	ctx, cancel := ctxTimeout(10 * time.Second)
	defer cancel()
	if err := controlClient().Do(ctx, http.MethodPost, "/reconnect", nil, nil); err != nil {
		return err
	}
	fmt.Println("link dropped; the session resumes on a new link (see 'gotunnel status')")
	return nil
}

func cmdForward(args []string) error {
	if len(args) == 0 {
		return usageError("usage: gotunnel forward add|rm|ls")
	}
	ctx, cancel := ctxTimeout(30 * time.Second)
	defer cancel()
	switch args[0] {
	case "add":
		fs := newFlags("forward add")
		listen := fs.String("listen", "", "local address, e.g. 127.0.0.1:5432")
		target := fs.String("target", "", "address as seen from the destination, e.g. db.internal:5432")
		mode := fs.String("mode", config.ModeOnDemand, "always or on-demand")
		prio := fs.String("priority", config.PrioNormal, "interactive, normal or bulk")
		name, err := parseNamed(fs, args[1:])
		if err != nil {
			return err
		}
		f := config.Forward{Name: name, Listen: *listen, Target: *target, Mode: *mode, Priority: *prio}
		if err := controlClient().Do(ctx, http.MethodPost, "/forwards", f, nil); err != nil {
			return err
		}
		fmt.Printf("forward %s added: %s -> %s (not saved; add it to the config file to keep it)\n", name, *listen, *target)
	case "rm", "remove":
		if len(args) < 2 {
			return usageError("usage: gotunnel forward rm NAME")
		}
		if err := controlClient().Do(ctx, http.MethodDelete, "/forwards/"+url.PathEscape(args[1]), nil, nil); err != nil {
			return err
		}
		fmt.Printf("forward %s removed (open connections continue)\n", args[1])
	case "ls", "list":
		st, err := fetchStatus()
		if err != nil {
			return err
		}
		printListeners(os.Stdout, st)
	default:
		return usageError("unknown forward command %q", args[0])
	}
	return nil
}

func cmdReverse(args []string) error {
	if len(args) == 0 {
		return usageError("usage: gotunnel reverse add|rm")
	}
	ctx, cancel := ctxTimeout(30 * time.Second)
	defer cancel()
	switch args[0] {
	case "add":
		fs := newFlags("reverse add")
		remote := fs.String("remote-listen", "", "address to listen on at the destination, e.g. 127.0.0.1:8080")
		target := fs.String("target", "", "address on this machine, e.g. 127.0.0.1:3000")
		prio := fs.String("priority", config.PrioNormal, "interactive, normal or bulk")
		name, err := parseNamed(fs, args[1:])
		if err != nil {
			return err
		}
		r := config.Reverse{Name: name, RemoteListen: *remote, Target: *target, Priority: *prio}
		if err := controlClient().Do(ctx, http.MethodPost, "/reverse", r, nil); err != nil {
			return err
		}
		fmt.Printf("reverse %s added: destination %s -> %s (not saved)\n", name, *remote, *target)
	case "rm", "remove":
		if len(args) < 2 {
			return usageError("usage: gotunnel reverse rm NAME")
		}
		if err := controlClient().Do(ctx, http.MethodDelete, "/reverse/"+url.PathEscape(args[1]), nil, nil); err != nil {
			return err
		}
		fmt.Printf("reverse %s removed\n", args[1])
	default:
		return usageError("unknown reverse command %q", args[0])
	}
	return nil
}
