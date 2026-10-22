// Package paths resolves the on-disk locations gotunnel uses on both the
// client and the agent side.
//
// Everything lives under GOTUNNEL_HOME (default ~/.gotunnel):
//
//	bin/   agent binary installed by "gotunnel install-agent"
//	run/   unix sockets and lock files (mode 0700)
//	logs/  rotating log files
package paths

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// maxSockPath is a conservative limit for unix socket paths (sun_path is 104
// bytes on macOS and 108 on Linux, including the trailing NUL).
const maxSockPath = 100

// Home returns the gotunnel state directory.
func Home() string {
	if h := os.Getenv("GOTUNNEL_HOME"); h != "" {
		return Expand(h)
	}
	return filepath.Join(userHome(), ".gotunnel")
}

// RunDir returns the directory holding sockets and lock files. It falls back
// to /tmp/gotunnel-<uid> when the regular location would produce socket
// paths that are too long.
func RunDir() string {
	if d := os.Getenv("GOTUNNEL_RUNTIME_DIR"); d != "" {
		return Expand(d)
	}
	d := filepath.Join(Home(), "run")
	if len(filepath.Join(d, "agentd.sock")) > maxSockPath {
		d = filepath.Join(os.TempDir(), fmt.Sprintf("gotunnel-%d", os.Getuid()))
	}
	return d
}

// LogDir returns the directory holding log files.
func LogDir() string { return filepath.Join(Home(), "logs") }

// ControlSocket is the client daemon's control API socket.
func ControlSocket() string { return filepath.Join(RunDir(), "ctl.sock") }

// ClientLock is held by the running client daemon.
func ClientLock() string { return filepath.Join(RunDir(), "client.lock") }

// AgentSocket is where agentd accepts links from relays and admin requests.
func AgentSocket() string { return filepath.Join(RunDir(), "agentd.sock") }

// AgentLock is held by the running agentd.
func AgentLock() string { return filepath.Join(RunDir(), "agentd.lock") }

// ClientLog and AgentLog are the rotating log files of the two daemons.
func ClientLog() string { return filepath.Join(LogDir(), "client.log") }
func AgentLog() string  { return filepath.Join(LogDir(), "agentd.log") }

// ClientOut and AgentOut receive the raw stdout/stderr of a daemonized
// process (startup errors, panics).
func ClientOut() string { return filepath.Join(LogDir(), "client.out") }
func AgentOut() string  { return filepath.Join(LogDir(), "agentd.out") }

// ConfigPath returns the default client configuration file.
func ConfigPath() string {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(Expand(d), "gotunnel", "config.yaml")
	}
	return filepath.Join(userHome(), ".config", "gotunnel", "config.yaml")
}

// EnsureDir creates dir (and parents) with mode 0700 if it does not exist.
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

// Expand replaces a leading "~" with the user's home directory.
func Expand(p string) string {
	if p == "~" {
		return userHome()
	}
	if strings.HasPrefix(p, "~/") {
		return filepath.Join(userHome(), p[2:])
	}
	return p
}

func userHome() string {
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return h
	}
	return "."
}
