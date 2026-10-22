package main

import (
	"bytes"
	"debug/buildinfo"
	"debug/elf"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFetchableVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"v0.1.0":                                   true,
		"v0.0.0-20261022072344-6102d89f379a":       true,
		"v0.0.0-20261022072344-6102d89f379a+dirty": false,
		"(devel)": false,
		"":        false,
		"dev":     false,
	} {
		if got := fetchableVersion(v); got != want {
			t.Errorf("fetchableVersion(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestParsePlatform(t *testing.T) {
	for in, want := range map[string]string{"Linux x86_64": "linux/amd64", "Linux aarch64": "linux/arm64", "Darwin arm64": "darwin/arm64"} {
		goos, goarch, err := parsePlatform(in)
		if err != nil || goos+"/"+goarch != want {
			t.Errorf("parsePlatform(%q) = %s/%s, %v; want %s", in, goos, goarch, err, want)
		}
	}
	if _, _, err := parsePlatform("Linux mips"); err == nil {
		t.Error("mips accepted")
	}
}

func TestRemoteBinary(t *testing.T) {
	if got := remoteBinary("~/.gotunnel/bin/gotunnel agent stdio"); got != `"$HOME"/.gotunnel/bin/gotunnel` {
		t.Errorf("got %s", got)
	}
	if got := remoteBinary("/opt/my tools/gotunnel agent stdio"); got != "/opt/my" {
		t.Errorf("got %s", got)
	}
}

// TestBuildAgentFromLocalModule cross-compiles the agent the way install-agent
// does for binaries without an embedded payload, using this checkout instead
// of a published version.
func TestBuildAgentFromLocalModule(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a full binary")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	const mod = "github.com/sontran0410/gotunnel"
	raw, err := buildAgent(mod, "v0.0.0", mod+"/cmd/gotunnel", root, "linux", "arm64")
	if err != nil {
		t.Fatal(err)
	}
	f, err := elf.NewFile(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("not an ELF binary: %v", err)
	}
	if f.Machine != elf.EM_AARCH64 {
		t.Fatalf("machine = %v, want arm64", f.Machine)
	}
	// What install-agent uploads must pass its own checks: static, right CPU.
	if err := checkAgentBinary(raw, "linux", "arm64"); err != nil {
		t.Fatalf("a freshly built agent fails checkAgentBinary: %v", err)
	}
	if err := checkAgentBinary(raw, "linux", "amd64"); err == nil {
		t.Fatal("an arm64 agent was accepted for an amd64 destination")
	}
	bi, err := buildinfo.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{}
	for _, s := range bi.Settings {
		settings[s.Key] = s.Value
	}
	if settings["CGO_ENABLED"] != "0" || settings["GOOS"] != "linux" || !bytes.Contains(raw, []byte("v0.0.0")) {
		t.Fatalf("unexpected build: settings %v", settings)
	}
}

// elfImage builds a minimal little-endian ELF64 executable for machine whose
// program headers have the given types. A PT_INTERP header points at interp,
// which is what makes a binary dynamically linked.
func elfImage(machine elf.Machine, interp string, progs ...elf.ProgType) []byte {
	const ehsize, phsize = 64, 56
	interpOff := ehsize + phsize*len(progs)
	buf := make([]byte, interpOff+len(interp)+1)
	copy(buf, "\x7fELF")
	buf[4], buf[5], buf[6] = byte(elf.ELFCLASS64), byte(elf.ELFDATA2LSB), byte(elf.EV_CURRENT)
	le := binary.LittleEndian
	le.PutUint16(buf[16:], uint16(elf.ET_EXEC))
	le.PutUint16(buf[18:], uint16(machine))
	le.PutUint32(buf[20:], uint32(elf.EV_CURRENT))
	le.PutUint64(buf[32:], ehsize) // e_phoff
	le.PutUint16(buf[52:], ehsize) // e_ehsize
	le.PutUint16(buf[54:], phsize) // e_phentsize
	le.PutUint16(buf[56:], uint16(len(progs)))
	for i, typ := range progs {
		ph := buf[ehsize+phsize*i:]
		le.PutUint32(ph[0:], uint32(typ))
		if typ == elf.PT_INTERP {
			le.PutUint64(ph[8:], uint64(interpOff))      // p_offset
			le.PutUint64(ph[32:], uint64(len(interp)+1)) // p_filesz
			le.PutUint64(ph[40:], uint64(len(interp)+1)) // p_memsz
		}
	}
	copy(buf[interpOff:], interp)
	return buf
}

func TestCheckAgentBinary(t *testing.T) {
	static := elfImage(elf.EM_X86_64, "", elf.PT_LOAD)
	dynamic := elfImage(elf.EM_X86_64, "/lib64/ld-linux-x86-64.so.2", elf.PT_PHDR, elf.PT_INTERP, elf.PT_LOAD)
	machO := []byte{0xcf, 0xfa, 0xed, 0xfe, 7, 0, 0, 1, 3, 0, 0, 0, 2, 0, 0, 0}

	if err := checkAgentBinary(static, "linux", "amd64"); err != nil {
		t.Errorf("static amd64 agent rejected: %v", err)
	}
	// The failure that motivated these checks: a cgo build links the
	// build machine's glibc and dies on older destinations.
	err := checkAgentBinary(dynamic, "linux", "amd64")
	if err == nil || !strings.Contains(err.Error(), "dynamically linked") || !strings.Contains(err.Error(), "ld-linux-x86-64") {
		t.Errorf("dynamic agent: err = %v", err)
	}
	if err := checkAgentBinary(static, "linux", "arm64"); err == nil || !strings.Contains(err.Error(), "arm64") {
		t.Errorf("wrong CPU: err = %v", err)
	}
	if err := checkAgentBinary(machO, "linux", "amd64"); err == nil || !strings.Contains(err.Error(), "not a Linux executable") {
		t.Errorf("macOS binary for a Linux destination: err = %v", err)
	}
	if err := checkAgentBinary(machO, "darwin", "arm64"); err != nil {
		t.Errorf("non-Linux destinations are not checked: %v", err)
	}
}

func TestAgentPayloadRejectsUnusableBinary(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	static := write("static", elfImage(elf.EM_X86_64, "", elf.PT_LOAD))
	dynamic := write("dynamic", elfImage(elf.EM_X86_64, "/lib64/ld-linux-x86-64.so.2", elf.PT_INTERP))

	gz, source, err := agentPayload(static, "linux", "amd64")
	if err != nil || source != static {
		t.Fatalf("static binary: %v (source %q)", err, source)
	}
	if raw, err := gunzipBytes(gz); err != nil || !bytes.Equal(raw, elfImage(elf.EM_X86_64, "", elf.PT_LOAD)) {
		t.Fatalf("payload does not round-trip: %v", err)
	}
	if _, _, err := agentPayload(dynamic, "linux", "amd64"); err == nil || !strings.Contains(err.Error(), "dynamically linked") {
		t.Fatalf("dynamic binary accepted: %v", err)
	}
}

func TestInstallHint(t *testing.T) {
	glibc := "/home/u/.gotunnel/bin/gotunnel: /lib64/libc.so.6: version `GLIBC_2.34' not found (required by /home/u/.gotunnel/bin/gotunnel)"
	if h := installHint(glibc); !strings.Contains(h, "dynamically linked") || !strings.Contains(h, "CGO_ENABLED=0") {
		t.Errorf("glibc hint = %q", h)
	}
	if h := installHint("sh: ./gotunnel: cannot execute binary file: Exec format error"); !strings.Contains(h, "CPU") {
		t.Errorf("exec format hint = %q", h)
	}
	if h := installHint("Permission denied"); h != "" {
		t.Errorf("unexpected hint %q", h)
	}
}
