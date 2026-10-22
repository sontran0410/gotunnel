//go:build embedagent

package agentbin

import "embed"

//go:embed payload/*.gz
var files embed.FS

func payload(name string) ([]byte, bool) {
	b, err := files.ReadFile("payload/" + name)
	return b, err == nil
}
