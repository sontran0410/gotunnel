//go:build !embedagent

package agentbin

func payload(string) ([]byte, bool) { return nil, false }

func embedded() []string { return nil }
