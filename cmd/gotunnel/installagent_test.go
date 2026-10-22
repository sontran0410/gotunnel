package main

import (
	"bytes"
	"debug/buildinfo"
	"debug/elf"
	"path/filepath"
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
