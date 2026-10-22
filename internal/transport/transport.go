// Package transport opens links (byte pipes) from the client to the agent.
package transport

import (
	"context"
	"io"
)

// Link is a bidirectional byte stream between client and agent.
type Link interface {
	io.ReadWriteCloser
}

// Diagnoser is implemented by links that can explain why they failed, for
// example by returning the ssh client's error output.
type Diagnoser interface {
	Diagnose() string
}

// Transport opens new links to the agent.
type Transport interface {
	Dial(ctx context.Context) (Link, error)
	String() string
}

// Func adapts a function to the Transport interface. It is used by tests.
type Func struct {
	Name string
	F    func(ctx context.Context) (Link, error)
}

// Dial calls F.
func (f Func) Dial(ctx context.Context) (Link, error) { return f.F(ctx) }

func (f Func) String() string { return f.Name }
