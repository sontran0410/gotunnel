package config

import (
	"strings"
	"testing"
	"time"
)

const sample = `
ssh:
  host: prod-dest
  compression: false
connection:
  ping_interval: 5s
  dead_after: 30s
  replay_buffer: 4MiB
  max_inflight: 32KiB
forwards:
  - name: dest-ssh
    listen: 127.0.0.1:2222
    target: 127.0.0.1:22
    mode: always
    priority: interactive
  - listen: 127.0.0.1:5432
    target: db.internal:5432
socks:
  - listen: 127.0.0.1:1080
reverse:
  - name: my-web
    remote_listen: 127.0.0.1:8080
    target: 127.0.0.1:3000
`

func TestParseSample(t *testing.T) {
	cfg, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SSH.Host != "prod-dest" || cfg.SSH.CompressionEnabled() {
		t.Errorf("ssh = %+v", cfg.SSH)
	}
	if cfg.SSH.RemoteCommand != DefaultRemoteCommand || cfg.SSH.Command != "ssh" {
		t.Errorf("ssh defaults not applied: %+v", cfg.SSH)
	}
	cn := cfg.Connection
	if cn.PingInterval.D() != 5*time.Second || cn.DeadAfter.D() != 30*time.Second {
		t.Errorf("durations = %v %v", cn.PingInterval, cn.DeadAfter)
	}
	if cn.SessionTTL.D() != 30*time.Minute || cn.ConnectTimeout.D() != 3*time.Minute {
		t.Errorf("duration defaults = %v %v", cn.SessionTTL, cn.ConnectTimeout)
	}
	if cn.ReplayBuffer != 4<<20 || cn.MaxInflight != 32<<10 || !cn.Adaptive() {
		t.Errorf("sizes = %v %v adaptive=%v", cn.ReplayBuffer, cn.MaxInflight, cn.Adaptive())
	}
	if f := cfg.Forwards[1]; f.Name != "forward-5432" || f.Mode != ModeOnDemand || f.Priority != PrioNormal {
		t.Errorf("forward defaults = %+v", f)
	}
	if cfg.Socks[0].Name != "socks-1080" {
		t.Errorf("socks name = %q", cfg.Socks[0].Name)
	}
	if !cfg.AlwaysUp() {
		t.Error("AlwaysUp = false, want true")
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	_, err := Parse([]byte(`
connection:
  ping_interval: 10s
  dead_after: 5s
forwards:
  - name: a
    listen: 127.0.0.1:99999
    target: nohost
    mode: sometimes
  - name: a
    listen: 127.0.0.1:1080
    target: x:1
socks:
  - listen: 127.0.0.1:1080
    priority: urgent
`))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{
		"ssh.host is required",
		"dead_after",
		"invalid port",
		"not host:port",
		"mode must be",
		`duplicate forward name "a"`,
		"already used by",
		"priority must be",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	_, err := Parse([]byte("ssh:\n  host: x\n  hots: y\n"))
	if err == nil || !strings.Contains(err.Error(), "hots") {
		t.Fatalf("err = %v, want unknown field error", err)
	}
}

func TestEmptyConfig(t *testing.T) {
	_, err := Parse(nil)
	if err == nil || !strings.Contains(err.Error(), "ssh.host") {
		t.Fatalf("err = %v", err)
	}
}

func TestBadDuration(t *testing.T) {
	_, err := Parse([]byte("ssh: {host: x}\nconnection: {ping_interval: 10}\n"))
	if err == nil || !strings.Contains(err.Error(), "not a duration") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{
		"1024": 1024, "64KiB": 64 << 10, "64kb": 64 << 10, "8MiB": 8 << 20, "2M": 2 << 20, "1GiB": 1 << 30, "10B": 10,
	} {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "abc", "-1", "1.5MiB"} {
		if _, err := ParseByteSize(bad); err == nil {
			t.Errorf("ParseByteSize(%q) succeeded", bad)
		}
	}
}

func TestExampleConfig(t *testing.T) {
	cfg, err := Load("../../examples/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Forwards) != 3 || len(cfg.Socks) != 1 || len(cfg.Reverse) != 1 {
		t.Fatalf("unexpected example contents: %+v", cfg)
	}
}
