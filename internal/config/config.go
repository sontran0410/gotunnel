// Package config loads and validates the client configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"gotunnel/internal/paths"
)

// Forward modes.
const (
	// ModeAlways keeps the transport up for as long as the forward exists.
	ModeAlways = "always"
	// ModeOnDemand brings the transport up on the first connection and lets it
	// close again after the idle timeout.
	ModeOnDemand = "on-demand"
)

// Stream priorities, used by the bandwidth scheduler.
const (
	PrioInteractive = "interactive"
	PrioNormal      = "normal"
	PrioBulk        = "bulk"
)

// DefaultRemoteCommand is what the client runs on the destination through ssh.
const DefaultRemoteCommand = "~/.gotunnel/bin/gotunnel agent stdio"

// Config is the client configuration.
type Config struct {
	SSH        SSH        `yaml:"ssh"`
	Connection Connection `yaml:"connection"`
	Daemon     Daemon     `yaml:"daemon"`
	Forwards   []Forward  `yaml:"forwards"`
	Socks      []Socks    `yaml:"socks"`
	Reverse    []Reverse  `yaml:"reverse"`

	// Path is the file the configuration was loaded from, if any.
	Path string `yaml:"-"`
}

// SSH describes how to reach the destination.
type SSH struct {
	// Host is an alias from ~/.ssh/config (which already knows the jump chain).
	Host string `yaml:"host"`
	// Command is the ssh binary to run (default "ssh").
	Command string `yaml:"ssh_command"`
	// ExtraArgs are passed to ssh before the host, e.g. ["-F", "/path/cfg"].
	ExtraArgs []string `yaml:"extra_args"`
	// Compression enables ssh -C (default true).
	Compression *bool `yaml:"compression"`
	// RemoteCommand starts the agent relay on the destination.
	RemoteCommand string `yaml:"remote_command"`
}

// CompressionEnabled reports whether ssh compression should be requested.
func (s SSH) CompressionEnabled() bool { return s.Compression == nil || *s.Compression }

// Connection holds transport and session tuning.
type Connection struct {
	ConnectTimeout Duration `yaml:"connect_timeout"`
	PingInterval   Duration `yaml:"ping_interval"`
	DeadAfter      Duration `yaml:"dead_after"`
	IdleTimeout    Duration `yaml:"idle_timeout"`
	SessionTTL     Duration `yaml:"session_ttl"`
	ReplayBuffer   ByteSize `yaml:"replay_buffer"`
	MaxInflight    ByteSize `yaml:"max_inflight"`
	// AdaptiveInflight lets the scheduler tune the in-flight limit from RTT
	// measurements (default true). MaxInflight is then the starting value.
	AdaptiveInflight *bool `yaml:"adaptive_inflight"`
}

// Adaptive reports whether the in-flight limit is tuned automatically.
func (c Connection) Adaptive() bool { return c.AdaptiveInflight == nil || *c.AdaptiveInflight }

// Daemon holds logging settings for the client daemon.
type Daemon struct {
	LogFile  string `yaml:"log_file"`
	LogLevel string `yaml:"log_level"`
}

// Forward is a local port forwarded to a target reachable from the destination.
type Forward struct {
	Name     string `yaml:"name" json:"name"`
	Listen   string `yaml:"listen" json:"listen"`
	Target   string `yaml:"target" json:"target"`
	Mode     string `yaml:"mode" json:"mode"`
	Priority string `yaml:"priority" json:"priority"`
}

// Socks is a local SOCKS5 proxy whose connections exit on the destination.
type Socks struct {
	Name     string `yaml:"name" json:"name"`
	Listen   string `yaml:"listen" json:"listen"`
	Priority string `yaml:"priority" json:"priority"`
}

// Reverse is a port opened on the destination and forwarded back to a target
// reachable from the client machine.
type Reverse struct {
	Name         string `yaml:"name" json:"name"`
	RemoteListen string `yaml:"remote_listen" json:"remote_listen"`
	Target       string `yaml:"target" json:"target"`
	Priority     string `yaml:"priority" json:"priority"`
}

// Load reads, defaults and validates the configuration at path.
func Load(path string) (*Config, error) {
	path = paths.Expand(path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	cfg.Path = path
	return cfg, nil
}

// Parse decodes, defaults and validates a YAML configuration.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	// An empty document decodes as io.EOF; let validation report what is missing.
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ApplyDefaults fills unset fields with their default values.
func (c *Config) ApplyDefaults() {
	if c.SSH.Command == "" {
		c.SSH.Command = "ssh"
	}
	if c.SSH.RemoteCommand == "" {
		c.SSH.RemoteCommand = DefaultRemoteCommand
	}
	cn := &c.Connection
	setDur(&cn.ConnectTimeout, 3*time.Minute)
	setDur(&cn.PingInterval, 10*time.Second)
	setDur(&cn.DeadAfter, 45*time.Second)
	setDur(&cn.IdleTimeout, 15*time.Minute)
	setDur(&cn.SessionTTL, 30*time.Minute)
	if cn.ReplayBuffer == 0 {
		cn.ReplayBuffer = 8 << 20
	}
	if cn.MaxInflight == 0 {
		cn.MaxInflight = 64 << 10
	}
	if c.Daemon.LogFile == "" {
		c.Daemon.LogFile = paths.ClientLog()
	}
	c.Daemon.LogFile = paths.Expand(c.Daemon.LogFile)
	if c.Daemon.LogLevel == "" {
		c.Daemon.LogLevel = "info"
	}
	for i := range c.Forwards {
		DefaultForward(&c.Forwards[i])
	}
	for i := range c.Socks {
		DefaultSocks(&c.Socks[i])
	}
	for i := range c.Reverse {
		DefaultReverse(&c.Reverse[i])
	}
}

func setDur(d *Duration, def time.Duration) {
	if *d == 0 {
		*d = Duration(def)
	}
}

// DefaultForward fills unset fields of f.
func DefaultForward(f *Forward) {
	if f.Mode == "" {
		f.Mode = ModeOnDemand
	}
	if f.Priority == "" {
		f.Priority = PrioNormal
	}
	if f.Name == "" {
		f.Name = "forward-" + portOf(f.Listen)
	}
}

// DefaultSocks fills unset fields of s.
func DefaultSocks(s *Socks) {
	if s.Priority == "" {
		s.Priority = PrioNormal
	}
	if s.Name == "" {
		s.Name = "socks-" + portOf(s.Listen)
	}
}

// DefaultReverse fills unset fields of r.
func DefaultReverse(r *Reverse) {
	if r.Priority == "" {
		r.Priority = PrioNormal
	}
	if r.Name == "" {
		r.Name = "reverse-" + portOf(r.RemoteListen)
	}
}

func portOf(addr string) string {
	if _, p, err := net.SplitHostPort(addr); err == nil {
		return p
	}
	return addr
}

// Validate checks the configuration and reports every problem found.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.SSH.Host == "" {
		add("ssh.host is required (an alias from ~/.ssh/config)")
	}
	cn := c.Connection
	if cn.PingInterval.D() <= 0 {
		add("connection.ping_interval must be positive")
	}
	if cn.DeadAfter.D() <= cn.PingInterval.D() {
		add("connection.dead_after (%s) must be longer than ping_interval (%s)", cn.DeadAfter, cn.PingInterval)
	}
	if cn.ConnectTimeout.D() <= 0 {
		add("connection.connect_timeout must be positive")
	}
	if cn.SessionTTL.D() < time.Minute {
		add("connection.session_ttl must be at least 1m")
	}
	if cn.IdleTimeout.D() < 0 {
		add("connection.idle_timeout must not be negative")
	}
	if cn.ReplayBuffer < 256<<10 {
		add("connection.replay_buffer must be at least 256KiB")
	}
	if cn.MaxInflight < 16<<10 || cn.MaxInflight > cn.ReplayBuffer {
		add("connection.max_inflight must be between 16KiB and replay_buffer")
	}
	switch c.Daemon.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("daemon.log_level must be one of debug, info, warn, error")
	}

	listens := map[string]string{}
	claimListen := func(owner, addr string) {
		if prev, ok := listens[addr]; ok {
			add("%s: listen address %s is already used by %s", owner, addr, prev)
			return
		}
		listens[addr] = owner
	}
	names := map[string]bool{}
	claimName := func(kind, name string) {
		key := kind + "/" + name
		if names[key] {
			add("duplicate %s name %q", kind, name)
		}
		names[key] = true
	}
	for i := range c.Forwards {
		f := &c.Forwards[i]
		errs = append(errs, ValidateForward(f)...)
		claimName("forward", f.Name)
		claimListen("forward "+f.Name, f.Listen)
	}
	for i := range c.Socks {
		s := &c.Socks[i]
		errs = append(errs, ValidateSocks(s)...)
		claimName("socks", s.Name)
		claimListen("socks "+s.Name, s.Listen)
	}
	for i := range c.Reverse {
		r := &c.Reverse[i]
		errs = append(errs, ValidateReverse(r)...)
		claimName("reverse", r.Name)
	}
	return errors.Join(errs...)
}

// ValidateForward checks a single (defaulted) forward.
func ValidateForward(f *Forward) []error {
	var errs []error
	who := "forward " + f.Name
	if err := checkAddr(f.Listen, false); err != nil {
		errs = append(errs, fmt.Errorf("%s: listen: %w", who, err))
	}
	if err := checkAddr(f.Target, true); err != nil {
		errs = append(errs, fmt.Errorf("%s: target: %w", who, err))
	}
	if f.Mode != ModeAlways && f.Mode != ModeOnDemand {
		errs = append(errs, fmt.Errorf("%s: mode must be %q or %q", who, ModeAlways, ModeOnDemand))
	}
	if !ValidPriority(f.Priority) {
		errs = append(errs, fmt.Errorf("%s: priority must be interactive, normal or bulk", who))
	}
	return errs
}

// ValidateSocks checks a single (defaulted) SOCKS entry.
func ValidateSocks(s *Socks) []error {
	var errs []error
	who := "socks " + s.Name
	if err := checkAddr(s.Listen, false); err != nil {
		errs = append(errs, fmt.Errorf("%s: listen: %w", who, err))
	}
	if !ValidPriority(s.Priority) {
		errs = append(errs, fmt.Errorf("%s: priority must be interactive, normal or bulk", who))
	}
	return errs
}

// ValidateReverse checks a single (defaulted) reverse forward.
func ValidateReverse(r *Reverse) []error {
	var errs []error
	who := "reverse " + r.Name
	if err := checkAddr(r.RemoteListen, false); err != nil {
		errs = append(errs, fmt.Errorf("%s: remote_listen: %w", who, err))
	}
	if err := checkAddr(r.Target, true); err != nil {
		errs = append(errs, fmt.Errorf("%s: target: %w", who, err))
	}
	if !ValidPriority(r.Priority) {
		errs = append(errs, fmt.Errorf("%s: priority must be interactive, normal or bulk", who))
	}
	return errs
}

// ValidPriority reports whether p is a known priority class.
func ValidPriority(p string) bool {
	return p == PrioInteractive || p == PrioNormal || p == PrioBulk
}

// checkAddr validates "host:port". Listen addresses may omit the host.
func checkAddr(addr string, needHost bool) error {
	if addr == "" {
		return errors.New("address is required (host:port)")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%q is not host:port", addr)
	}
	if needHost && host == "" {
		return fmt.Errorf("%q has no host", addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("%q has an invalid port", addr)
	}
	return nil
}

// AlwaysUp reports whether the configuration requires a permanent transport.
func (c *Config) AlwaysUp() bool {
	if len(c.Reverse) > 0 {
		return true
	}
	for _, f := range c.Forwards {
		if f.Mode == ModeAlways {
			return true
		}
	}
	return false
}

// Duration is a time.Duration written as "10s", "5m" in YAML.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML parses Go duration strings such as "45s" or "1h30m".
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: expected a duration like 10s or 5m", n.Line)
	}
	v, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("line %d: %q is not a duration (use units, e.g. 10s, 5m, 1h)", n.Line, s)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML writes the duration in Go syntax.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// ByteSize is a size written as "64KiB", "8MiB" or a plain number of bytes.
type ByteSize int64

// UnmarshalYAML parses sizes with K/KB/KiB, M/MB/MiB or G/GB/GiB suffixes
// (all binary multiples).
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: expected a size like 64KiB or 8MiB", n.Line)
	}
	v, err := ParseByteSize(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*b = v
	return nil
}

// MarshalYAML writes the size in bytes.
func (b ByteSize) MarshalYAML() (any, error) { return int64(b), nil }

// ParseByteSize parses a human size such as "64KiB".
func ParseByteSize(s string) (ByteSize, error) {
	t := strings.TrimSpace(s)
	mult := int64(1)
	upper := strings.ToUpper(t)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{
		{"GIB", 1 << 30}, {"GB", 1 << 30}, {"G", 1 << 30},
		{"MIB", 1 << 20}, {"MB", 1 << 20}, {"M", 1 << 20},
		{"KIB", 1 << 10}, {"KB", 1 << 10}, {"K", 1 << 10},
		{"B", 1},
	} {
		if strings.HasSuffix(upper, u.suffix) {
			mult = u.mult
			t = strings.TrimSpace(t[:len(t)-len(u.suffix)])
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a size (e.g. 64KiB, 8MiB)", s)
	}
	return ByteSize(n * mult), nil
}

func (b ByteSize) String() string {
	switch {
	case b >= 1<<20 && b%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", b>>20)
	case b >= 1<<10 && b%(1<<10) == 0:
		return fmt.Sprintf("%dKiB", b>>10)
	}
	return fmt.Sprintf("%dB", int64(b))
}
