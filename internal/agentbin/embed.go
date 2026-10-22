//go:build embedagent

package agentbin

import (
	"embed"
	"io/fs"
)

//go:embed payload/*.gz
var files embed.FS

func payload(name string) ([]byte, bool) {
	b, err := files.ReadFile("payload/" + name)
	return b, err == nil
}

func embedded() []string {
	names, _ := fs.Glob(files, "payload/*.gz")
	for i, n := range names {
		names[i] = n[len("payload/"):]
	}
	return names
}
