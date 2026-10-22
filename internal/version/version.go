// Package version holds build and protocol version identifiers.
package version

// Version is the gotunnel release version. It is overridden at build time with
// -ldflags "-X gotunnel/internal/version.Version=...".
var Version = "0.1.0-dev"

// Proto is the wire protocol version spoken between client and agent. Peers
// with different Proto values refuse to talk to each other.
const Proto = 1
