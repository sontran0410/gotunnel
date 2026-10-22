package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"gotunnel/internal/agentbin"
	"gotunnel/internal/config"
	"gotunnel/internal/daemon"
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
	cmd.Stderr = os.Stderr
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
		return fmt.Errorf("upload failed: %v", errors.Join(err, werr))
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
func agentPayload(binary, goos, goarch string) ([]byte, string, error) {
	if binary != "" {
		raw, err := os.ReadFile(binary)
		if err != nil {
			return nil, "", err
		}
		gz, err := gzipBytes(raw)
		return gz, binary, err
	}
	if p, ok := agentbin.Payload(goos, goarch); ok {
		return p, "embedded", nil
	}
	if goos == runtime.GOOS && goarch == runtime.GOARCH {
		exe, err := os.Executable()
		if err != nil {
			return nil, "", err
		}
		raw, err := os.ReadFile(exe)
		if err != nil {
			return nil, "", err
		}
		gz, err := gzipBytes(raw)
		return gz, "this binary", err
	}
	embedded := strings.Join(agentbin.Embedded(), ", ")
	if embedded == "" {
		embedded = "none"
	}
	return nil, "", fmt.Errorf("no agent binary for %s/%s (embedded: %s); build with 'make build' or pass --binary", goos, goarch, embedded)
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
