package proto

import (
	"bufio"
	"fmt"
	"io"
)

// Preamble is written by the agent relay before anything else. The client
// discards whatever precedes it, such as text printed by the remote user's
// shell startup files.
const Preamble = "GOTUNNEL/1\n"

// WritePreamble writes the preamble to w.
func WritePreamble(w io.Writer) error {
	_, err := io.WriteString(w, Preamble)
	return err
}

// ReadPreamble consumes r up to and including the preamble. It returns the
// bytes skipped before it, and fails if more than maxSkip bytes precede it.
func ReadPreamble(r *bufio.Reader, maxSkip int) (skipped []byte, err error) {
	matched := 0
	for {
		b, err := r.ReadByte()
		if err != nil {
			return skipped, fmt.Errorf("waiting for agent preamble: %w", err)
		}
		if b == Preamble[matched] {
			matched++
			if matched == len(Preamble) {
				return skipped, nil
			}
			continue
		}
		// The preamble's first byte does not reappear in it, so on a mismatch
		// the partial match is plain garbage.
		skipped = append(skipped, Preamble[:matched]...)
		matched = 0
		if b == Preamble[0] {
			matched = 1
		} else {
			skipped = append(skipped, b)
		}
		if len(skipped) > maxSkip {
			return skipped, fmt.Errorf("no agent preamble within %d bytes of remote output", maxSkip)
		}
	}
}
