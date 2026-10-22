// Package service registers gotunnel with the operating system's service
// manager so it starts at login/boot and is restarted if it crashes:
// launchd on macOS, systemd --user on Linux, and a crontab @reboot entry
// where systemd is not available.
package service

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

// Spec describes a service to install.
type Spec struct {
	// Name is a short identifier ("client" or "agent").
	Name        string
	Description string
	// Args is the full command line, starting with the absolute binary path.
	// It must run in the foreground.
	Args []string
	// CronArgs is used instead of Args for the crontab fallback; it must
	// start the daemon in the background and return.
	CronArgs []string
	Env      map[string]string
	// LogPath receives the raw output of the process (launchd only).
	LogPath string
}

// Manager installs services for one service manager.
type Manager interface {
	Kind() string // launchd, systemd or cron
	Install(Spec) (notes []string, err error)
	Uninstall(name string) error
	Status(name string) (string, error)
	Installed(name string) bool
}

// Detect returns the service manager available on this system.
func Detect() Manager {
	if runtime.GOOS == "darwin" {
		return launchd{}
	}
	if systemdUserAvailable() {
		return systemd{}
	}
	return cron{}
}

// --- launchd ---

type launchd struct{}

func (launchd) Kind() string { return "launchd" }

func launchdLabel(name string) string { return "local.gotunnel." + name }

func launchdPlist(name string) string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents", launchdLabel(name)+".plist")
}

func launchdDomain() string { return "gui/" + strconv.Itoa(os.Getuid()) }

func (l launchd) Install(s Spec) ([]string, error) {
	label := launchdLabel(s.Name)
	path := launchdPlist(s.Name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(plist(label, s)), 0o644); err != nil {
		return nil, err
	}
	run("launchctl", "bootout", launchdDomain()+"/"+label) // reload if already loaded
	if out, err := run("launchctl", "bootstrap", launchdDomain(), path); err != nil {
		return nil, fmt.Errorf("launchctl bootstrap: %v: %s", err, out)
	}
	return []string{
		"installed " + path,
		"it starts at login and is restarted if it crashes; 'gotunnel down' stops it until the next login",
	}, nil
}

func (l launchd) Uninstall(name string) error {
	run("launchctl", "bootout", launchdDomain()+"/"+launchdLabel(name))
	err := os.Remove(launchdPlist(name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (l launchd) Installed(name string) bool {
	_, err := os.Stat(launchdPlist(name))
	return err == nil
}

func (l launchd) Status(name string) (string, error) {
	if !l.Installed(name) {
		return "not installed", nil
	}
	out, err := run("launchctl", "print", launchdDomain()+"/"+launchdLabel(name))
	if err != nil {
		return "installed, not loaded", nil
	}
	state, pid := "", ""
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "state = "); ok && state == "" {
			state = v
		}
		if v, ok := strings.CutPrefix(line, "pid = "); ok && pid == "" {
			pid = v
		}
	}
	if pid != "" {
		return fmt.Sprintf("installed (launchd %s), %s, pid %s", launchdLabel(name), state, pid), nil
	}
	return fmt.Sprintf("installed (launchd %s), %s", launchdLabel(name), state), nil
}

func plist(label string, s Spec) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
`)
	kv := func(k, v string) { fmt.Fprintf(&b, "  <key>%s</key>\n  <string>%s</string>\n", k, xmlEscape(v)) }
	kv("Label", label)
	b.WriteString("  <key>ProgramArguments</key>\n  <array>\n")
	for _, a := range s.Args {
		fmt.Fprintf(&b, "    <string>%s</string>\n", xmlEscape(a))
	}
	b.WriteString("  </array>\n")
	b.WriteString("  <key>RunAtLoad</key>\n  <true/>\n")
	// Restart after crashes, but not after a clean "gotunnel down".
	b.WriteString("  <key>KeepAlive</key>\n  <dict>\n    <key>SuccessfulExit</key>\n    <false/>\n  </dict>\n")
	b.WriteString("  <key>ThrottleInterval</key>\n  <integer>10</integer>\n")
	if len(s.Env) > 0 {
		b.WriteString("  <key>EnvironmentVariables</key>\n  <dict>\n")
		for _, k := range sortedKeys(s.Env) {
			fmt.Fprintf(&b, "    <key>%s</key>\n    <string>%s</string>\n", xmlEscape(k), xmlEscape(s.Env[k]))
		}
		b.WriteString("  </dict>\n")
	}
	if s.LogPath != "" {
		kv("StandardOutPath", s.LogPath)
		kv("StandardErrorPath", s.LogPath)
	}
	b.WriteString("</dict>\n</plist>\n")
	return b.String()
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '"':
			b.WriteString("&quot;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// --- systemd --user ---

type systemd struct{}

func (systemd) Kind() string { return "systemd" }

func systemdUnit(name string) string {
	if name == "client" {
		return "gotunnel.service"
	}
	return "gotunnel-" + name + ".service"
}

func systemdPath(name string) string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "systemd", "user", systemdUnit(name))
}

func systemdUserAvailable() bool {
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	_, err := run("systemctl", "--user", "show-environment")
	return err == nil
}

func (systemd) Install(s Spec) ([]string, error) {
	path := systemdPath(s.Name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(unitFile(s)), 0o644); err != nil {
		return nil, err
	}
	if out, err := run("systemctl", "--user", "daemon-reload"); err != nil {
		return nil, fmt.Errorf("systemctl daemon-reload: %v: %s", err, out)
	}
	if out, err := run("systemctl", "--user", "enable", "--now", systemdUnit(s.Name)); err != nil {
		return nil, fmt.Errorf("systemctl enable: %v: %s", err, out)
	}
	notes := []string{"installed " + path}
	// Without lingering, user services stop at logout and do not start at boot.
	if u, err := user.Current(); err == nil {
		if out, err := run("loginctl", "show-user", u.Username, "-p", "Linger"); err == nil && strings.Contains(out, "Linger=yes") {
			notes = append(notes, "lingering is enabled: the service runs without a login session and starts at boot")
		} else if _, err := run("loginctl", "enable-linger", u.Username); err == nil {
			notes = append(notes, "enabled lingering: the service runs without a login session and starts at boot")
		} else {
			notes = append(notes, "could not enable lingering, so the service stops when you log out; ask an admin to run: loginctl enable-linger "+u.Username)
		}
	}
	return notes, nil
}

func unitFile(s Spec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[Unit]\nDescription=%s\nAfter=network-online.target\n\n[Service]\n", s.Description)
	quoted := make([]string, len(s.Args))
	for i, a := range s.Args {
		quoted[i] = systemdQuote(a)
	}
	fmt.Fprintf(&b, "ExecStart=%s\n", strings.Join(quoted, " "))
	for _, k := range sortedKeys(s.Env) {
		fmt.Fprintf(&b, "Environment=%s\n", systemdQuote(k+"="+s.Env[k]))
	}
	b.WriteString("Restart=on-failure\nRestartSec=5\n\n[Install]\nWantedBy=default.target\n")
	return b.String()
}

func systemdQuote(s string) string {
	if !strings.ContainsAny(s, " \t\"'\\$%") {
		return s
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `$`, `$$`, `%`, `%%`)
	return `"` + r.Replace(s) + `"`
}

func (systemd) Uninstall(name string) error {
	run("systemctl", "--user", "disable", "--now", systemdUnit(name))
	err := os.Remove(systemdPath(name))
	run("systemctl", "--user", "daemon-reload")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (systemd) Installed(name string) bool {
	_, err := os.Stat(systemdPath(name))
	return err == nil
}

func (s systemd) Status(name string) (string, error) {
	if !s.Installed(name) {
		return "not installed", nil
	}
	active, _ := run("systemctl", "--user", "is-active", systemdUnit(name))
	pid, _ := run("systemctl", "--user", "show", "-p", "MainPID", "--value", systemdUnit(name))
	st := fmt.Sprintf("installed (systemd --user %s), %s", systemdUnit(name), strings.TrimSpace(active))
	if p := strings.TrimSpace(pid); p != "" && p != "0" {
		st += ", pid " + p
	}
	return st, nil
}

// --- crontab @reboot fallback ---

type cron struct{}

func (cron) Kind() string { return "cron" }

func cronMarker(name string) string { return "# gotunnel:" + name }

func crontab() (string, error) {
	out, err := run("crontab", "-l")
	if err != nil {
		if strings.Contains(out, "no crontab") {
			return "", nil
		}
		return "", fmt.Errorf("crontab -l: %v: %s", err, out)
	}
	return out, nil
}

func writeCrontab(content string) error {
	cmd := exec.Command("crontab", "-")
	cmd.Stdin = strings.NewReader(content)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("crontab: %v: %s", err, out)
	}
	return nil
}

func withoutMarker(tab, name string) string {
	var keep []string
	for _, line := range strings.Split(tab, "\n") {
		if line != "" && !strings.HasSuffix(line, cronMarker(name)) {
			keep = append(keep, line)
		}
	}
	if len(keep) == 0 {
		return ""
	}
	return strings.Join(keep, "\n") + "\n"
}

func (cron) Install(s Spec) ([]string, error) {
	if _, err := exec.LookPath("crontab"); err != nil {
		return nil, errors.New("neither systemd --user nor crontab is available")
	}
	tab, err := crontab()
	if err != nil {
		return nil, err
	}
	args := s.CronArgs
	if len(args) == 0 {
		args = s.Args
	}
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = shellQuote(a)
	}
	var env []string
	for _, k := range sortedKeys(s.Env) {
		env = append(env, k+"="+shellQuote(s.Env[k]))
	}
	line := strings.TrimSpace(strings.Join(env, " ")+" "+strings.Join(quoted, " ")) + " " + cronMarker(s.Name)
	if err := writeCrontab(withoutMarker(tab, s.Name) + "@reboot " + line + "\n"); err != nil {
		return nil, err
	}
	return []string{"added a crontab @reboot entry (systemd --user is not available here)"}, nil
}

func (cron) Uninstall(name string) error {
	tab, err := crontab()
	if err != nil {
		return err
	}
	return writeCrontab(withoutMarker(tab, name))
}

func (cron) Installed(name string) bool {
	tab, err := crontab()
	return err == nil && strings.Contains(tab, cronMarker(name))
}

func (c cron) Status(name string) (string, error) {
	if c.Installed(name) {
		return "installed (crontab @reboot)", nil
	}
	return "not installed", nil
}

func shellQuote(s string) string {
	if s != "" && !strings.ContainsAny(s, " \t\n\"'\\$`!*?[]{}()<>|&;#~") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func sortedKeys(m map[string]string) []string { return slices.Sorted(maps.Keys(m)) }

func run(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}
