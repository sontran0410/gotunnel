// Package version holds build and protocol version identifiers.
package version

import "runtime/debug"

// Version is the gotunnel release version: the module version recorded by
// the Go toolchain ("v0.2.0" for "go install …@v0.2.0", a pseudo-version for
// untagged commits, a "+dirty" suffix for modified checkouts), or "dev" when
// the build has no version information. It can be overridden with
// -ldflags "-X github.com/sontran0410/gotunnel/internal/version.Version=…".
var Version = "dev"

func init() {
	if Version != "dev" {
		return
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		Version = bi.Main.Version
	}
}

// Proto is the wire protocol version spoken between client and agent. Peers
// with different Proto values refuse to talk to each other.
const Proto = 1
