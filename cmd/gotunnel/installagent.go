package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"debug/elf"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sontran0410/gotunnel/internal/agentbin"
	"github.com/sontran0410/gotunnel/internal/config"
	"github.com/sontran0410/gotunnel/internal/daemon"
)

// installScript runs on the destination. It prints the platform, reads the
// payload size and the gzipped binary from stdin, installs it atomically and
// prints the installed version. One ssh round trip does everything, because
// each ssh connection through the jump chain is slow.
const installScript = `set -e
echo ` + installMarker + `
uname -sm
target=%s
dir=$(dirname "$target")
mkdir -p "$dir"
tmp="$dir/.gotunnel-upload.$$"
trap 'rm -f "$tmp"' EXIT
IFS= read -r size
head -c "$size" | gunzip -c > "$tmp"
chmod 755 "$tmp"
mv -f "$tmp" "$target"
trap - EXIT
"$target" version
`

// installMarker precedes the script's output, so text printed by the remote
// user's shell startup files can be skipped.
const installMarker = "GOTUNNEL-INSTALL/1"

// readPlatform skips remote output up to the marker and returns the line
// after it.
func readPlatform(r *bufio.Reader) string {
	for skipped := 0; skipped < 1000; skipped++ {
		line, err := r.ReadString('\n')
		if strings.TrimSpace(line) == installMarker {
			platform, _ := r.ReadString('\n')
			return strings.TrimSpace(platform)
		}
		if err != nil {
			return ""
		}
	}
	return ""
}

func cmdInstallAgent(args []string) error {
	fs := newFlags("install-agent")
	cfgPath := configFlag(fs)
	binary := fs.String("binary", "", "agent binary to upload instead of the embedded one")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	target := remoteBinary(cfg.SSH.RemoteCommand)
	script := fmt.Sprintf(installScript, target)

	t := sshTransport(cfg, nil)
	t.RemoteCommand = "sh -c " + shellQuote(script)
	cmd := exec.Command(t.Command, t.Args()...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var remoteErr bytes.Buffer
	cmd.Stderr = io.MultiWriter(os.Stderr, &remoteErr)
	fmt.Fprintf(os.Stderr, "connecting to %s (this can take a while)...\n", cfg.SSH.Host)
	if err := cmd.Start(); err != nil {
		return err
	}
	kill := func() { syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }

	out := bufio.NewReader(stdout)
	lineCh := make(chan string, 1)
	go func() { lineCh <- readPlatform(out) }()
	var platform string
	select {
	case platform = <-lineCh:
	case <-time.After(cfg.Connection.ConnectTimeout.D()):
		kill()
		cmd.Wait()
		return fmt.Errorf("no answer from %s within %s", cfg.SSH.Host, cfg.Connection.ConnectTimeout)
	}
	if platform == "" {
		cmd.Wait()
		return errors.New("ssh failed (see the messages above); check that 'ssh " + cfg.SSH.Host + "' works")
	}
	goos, goarch, err := parsePlatform(platform)
	if err != nil {
		kill()
		cmd.Wait()
		return err
	}
	payload, source, err := agentPayload(*binary, goos, goarch)
	if err != nil {
		kill()
		cmd.Wait()
		return err
	}
	fmt.Fprintf(os.Stderr, "destination is %s/%s; uploading %s (%s) to %s\n", goos, goarch, bytesH(uint64(len(payload))), source, target)

	var sent atomic.Int64
	stopProgress := progress(&sent, int64(len(payload)))
	_, err = fmt.Fprintf(stdin, "%d\n", len(payload))
	if err == nil {
		_, err = io.Copy(stdin, &countingReader{r: bytes.NewReader(payload), n: &sent})
	}
	stdin.Close()
	stopProgress()
	rest, _ := io.ReadAll(out)
	werr := cmd.Wait()
	if err != nil || werr != nil {
		return fmt.Errorf("upload failed: %v%s", errors.Join(err, werr), installHint(remoteErr.String()))
	}
	fmt.Printf("installed: %s\n", strings.TrimSpace(string(rest)))
	fmt.Println("a running agentd without sessions switches to the new version on the next connection")
	return nil
}

// remoteBinary extracts the agent path from the remote command, using $HOME
// for a leading "~/" so the remote shell expands it.
func remoteBinary(remoteCommand string) string {
	bin := strings.Fields(remoteCommand)[0]
	if rest, ok := strings.CutPrefix(bin, "~/"); ok {
		return `"$HOME"/` + shellQuote(rest)
	}
	return shellQuote(bin)
}

func parsePlatform(s string) (goos, goarch string, err error) {
	f := strings.Fields(s)
	if len(f) != 2 {
		return "", "", fmt.Errorf("unexpected platform %q from the destination", s)
	}
	goos = strings.ToLower(f[0])
	switch f[1] {
	case "x86_64", "amd64":
		goarch = "amd64"
	case "aarch64", "arm64":
		goarch = "arm64"
	default:
		return "", "", fmt.Errorf("unsupported destination architecture %q", f[1])
	}
	return goos, goarch, nil
}

// agentPayload returns the gzipped binary to upload and where it came from.
// Sources, in order: --binary; the payload embedded by "make build"; this
// binary itself when the destination has the same platform; a build of the
// same module version made with the local Go toolchain (binaries installed
// with "go install" embed no payload).
//
// Every candidate goes through checkAgentBinary. In particular a Linux client
// installed with a plain "go install" is dynamically linked against its own
// glibc; uploading it to an older destination fails with "version
// `GLIBC_2.34' not found", so such a binary is skipped in favour of a static
// build.
func agentPayload(binary, goos, goarch string) ([]byte, string, error) {
	if binary != "" {
		raw, err := os.ReadFile(binary)
		if err != nil {
			return nil, "", err
		}
		if err := checkAgentBinary(raw, goos, goarch); err != nil {
			return nil, "", fmt.Errorf("%s: %w", binary, err)
		}
		gz, err := gzipBytes(raw)
		return gz, binary, err
	}
	if p, ok := agentbin.Payload(goos, goarch); ok {
		raw, err := gunzipBytes(p)
		if err == nil {
			err = checkAgentBinary(raw, goos, goarch)
		}
		if err != nil {
			return nil, "", fmt.Errorf("embedded %s/%s agent: %w", goos, goarch, err)
		}
		return p, "embedded", nil
	}
	var notUsable error // why this binary was not used as the agent
	if goos == runtime.GOOS && goarch == runtime.GOARCH {
		gz, err := thisBinary(goos, goarch)
		if err == nil {
			return gz, "this binary", nil
		}
		notUsable = err
		fmt.Fprintf(os.Stderr, "not uploading this binary as the agent: %v\n", err)
	}
	const hint = "install gotunnel with 'CGO_ENABLED=0 go install github.com/sontran0410/gotunnel/cmd/gotunnel@latest' or 'make build' (static), or pass --binary with a static agent"
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return nil, "", fmt.Errorf("no usable agent binary for %s/%s: %w\n%s", goos, goarch, errors.Join(notUsable, errors.New("no build information")), hint)
	}
	fmt.Fprintf(os.Stderr, "no embedded agent for %s/%s; building %s with your Go toolchain...\n", goos, goarch, bi.Main.Version)
	raw, err := buildAgent(bi.Main.Path, bi.Main.Version, bi.Path, "", goos, goarch)
	if err == nil {
		err = checkAgentBinary(raw, goos, goarch)
	}
	if err != nil {
		return nil, "", fmt.Errorf("no usable agent binary for %s/%s: %w\n%s", goos, goarch, errors.Join(notUsable, err), hint)
	}
	gz, err := gzipBytes(raw)
	return gz, "built locally", err
}

// thisBinary returns the running executable, gzipped, if it can serve as the
// agent for goos/goarch.
func thisBinary(goos, goarch string) ([]byte, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(exe)
	if err != nil {
		return nil, err
	}
	if err := checkAgentBinary(raw, goos, goarch); err != nil {
		return nil, err
	}
	return gzipBytes(raw)
}

// checkAgentBinary verifies that raw can run on a destination of goos/goarch.
// For Linux that means an ELF executable for the right CPU that is statically
// linked: a dynamically linked Go binary (built with cgo) needs the build
// machine's glibc and fails on older destinations with errors such as
// "version `GLIBC_2.34' not found".
func checkAgentBinary(raw []byte, goos, goarch string) error {
	if goos != "linux" {
		return nil
	}
	f, err := elf.NewFile(bytes.NewReader(raw))
	if err != nil {
		return errors.New("not a Linux executable (ELF); was it built for another operating system?")
	}
	defer f.Close()
	machines := map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}
	if want, ok := machines[goarch]; ok && f.Machine != want {
		built := f.Machine.String()
		for arch, m := range machines {
			if m == f.Machine {
				built = arch
			}
		}
		return fmt.Errorf("built for %s but the destination is %s", built, goarch)
	}
	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			interp, _ := io.ReadAll(io.LimitReader(p.Open(), 256))
			return fmt.Errorf("dynamically linked (needs %s and the destination's C library, so it fails on older systems); build with CGO_ENABLED=0 to get a static binary",
				strings.TrimRight(string(interp), "\x00"))
		}
	}
	return nil
}

// installHint explains common failures of the remote install script.
func installHint(stderr string) string {
	switch {
	case strings.Contains(stderr, "GLIBC") || strings.Contains(stderr, "libc.so"):
		return "\nhint: the uploaded binary is dynamically linked and needs a newer libc than the destination has. " +
			"gotunnel's own builds are static: use a gotunnel built with 'make build' or 'CGO_ENABLED=0 go install', or pass --binary with a static agent"
	case strings.Contains(stderr, "Exec format error"):
		return "\nhint: the binary does not match the destination's CPU or operating system"
	}
	return ""
}

func gunzipBytes(gz []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

// buildAgent compiles package pkg of module mod at version ver for
// goos/goarch inside a temporary module and returns the binary. The version
// is stamped explicitly because the temporary module is the main module of
// that build. replaceDir, if set, points mod at a local directory (tests).
func buildAgent(mod, ver, pkg, replaceDir, goos, goarch string) ([]byte, error) {
	if replaceDir == "" && !fetchableVersion(ver) {
		return nil, fmt.Errorf("this binary's version %q is not a published module version (install with 'go install %s@latest')", ver, pkg)
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		return nil, errors.New("no Go toolchain in PATH")
	}
	dir, err := os.MkdirTemp("", "gotunnel-agent-build")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	goCmd := func(env []string, args ...string) error {
		cmd := exec.Command(goBin, args...)
		cmd.Dir = dir
		// Ignore the caller's workspace and flags (e.g. -mod=vendor).
		cmd.Env = append(os.Environ(), append([]string{"GOWORK=off", "GOFLAGS="}, env...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("go %s: %v\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if err := goCmd(nil, "mod", "init", "gotunnel-agent-build"); err != nil {
		return nil, err
	}
	edit := []string{"mod", "edit", "-require=" + mod + "@" + ver}
	if replaceDir != "" {
		edit = append(edit, "-replace="+mod+"="+replaceDir)
	}
	if err := goCmd(nil, edit...); err != nil {
		return nil, err
	}
	out := filepath.Join(dir, "gotunnel")
	ldflags := "-s -w -X " + mod + "/internal/version.Version=" + ver
	env := []string{"GOOS=" + goos, "GOARCH=" + goarch, "CGO_ENABLED=0"}
	if err := goCmd(env, "build", "-mod=mod", "-trimpath", "-ldflags="+ldflags, "-o", out, pkg); err != nil {
		return nil, err
	}
	return os.ReadFile(out)
}

// fetchableVersion reports whether v names a module version that the Go
// toolchain can download (not a local or modified build).
func fetchableVersion(v string) bool {
	return strings.HasPrefix(v, "v") && !strings.Contains(v, "+")
}

func gzipBytes(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// progress prints upload progress on a terminal until the returned function
// is called.
func progress(sent *atomic.Int64, total int64) func() {
	if !daemon.IsTerminal(os.Stderr) {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(finished)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				fmt.Fprint(os.Stderr, "\r\033[K")
				return
			case <-t.C:
				n := sent.Load()
				rate := float64(n) / time.Since(start).Seconds()
				fmt.Fprintf(os.Stderr, "\r\033[K  %s / %s  (%s/s)", bytesH(uint64(n)), bytesH(uint64(total)), bytesH(uint64(rate)))
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

func shellQuote(s string) string {
	if s != "" && !strings.ContainsAny(s, " \t\n\"'\\$`!*?[]{}()<>|&;#~") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// remoteVersionCommand returns a shell command printing the agent version.
func remoteVersionCommand(cfg *config.Config) string {
	return "sh -c " + shellQuote(remoteBinary(cfg.SSH.RemoteCommand)+" version")
}
