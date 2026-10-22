// Package agentbin gives "gotunnel install-agent" the agent binaries to
// upload. Builds made with "make build" embed gzipped Linux binaries (build
// tag embedagent); plain "go build" binaries embed nothing.
package agentbin

import "fmt"

// Name returns the payload file name for a platform.
func Name(goos, goarch string) string {
	return fmt.Sprintf("gotunnel-%s-%s.gz", goos, goarch)
}

// Payload returns the gzipped agent binary for a platform, if embedded.
func Payload(goos, goarch string) ([]byte, bool) {
	return payload(Name(goos, goarch))
}
