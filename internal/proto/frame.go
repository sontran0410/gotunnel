// Package proto defines the wire formats spoken between the gotunnel client
// and agent.
//
// The link (the ssh stdio pipe, relayed to agentd) carries frames:
//
//	type (1 byte) | length (3 bytes, big endian) | payload
//
// A link starts with a handshake (HELLO → WELCOME or REJECT); afterwards it
// carries session frames (DATA, ACK, PING, PONG, BYE). Inside DATA runs a
// yamux session whose streams start with a length-prefixed JSON Request.
package proto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Frame types.
const (
	FrameHello     byte = 0x01 // client → agent: open or resume a session
	FrameWelcome   byte = 0x02 // agent → client: handshake accepted
	FrameReject    byte = 0x03 // agent → client: handshake refused
	FrameAdmin     byte = 0x04 // CLI/relay → agentd: admin request
	FrameAdminResp byte = 0x05 // agentd → CLI/relay: admin response
	FrameData      byte = 0x10 // session payload bytes
	FrameAck       byte = 0x11 // u64: total payload bytes received
	FramePing      byte = 0x12 // u64 nonce
	FramePong      byte = 0x13 // u64 nonce echoed back
	FrameBye       byte = 0x14 // graceful end of the session
)

// HeaderSize is the size of a frame header.
const HeaderSize = 4

// MaxFrame bounds the payload size accepted by ReadFrame.
const MaxFrame = 1 << 20

// ErrFrameTooLarge is returned for frames exceeding MaxFrame.
var ErrFrameTooLarge = errors.New("proto: frame too large")

// PutHeader writes a frame header for a payload of n bytes into dst[:4].
func PutHeader(dst []byte, typ byte, n int) {
	dst[0] = typ
	dst[1] = byte(n >> 16)
	dst[2] = byte(n >> 8)
	dst[3] = byte(n)
}

// AppendFrame appends a complete frame to dst.
func AppendFrame(dst []byte, typ byte, payload []byte) []byte {
	var hdr [HeaderSize]byte
	PutHeader(hdr[:], typ, len(payload))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

// AppendU64Frame appends a frame whose payload is a single uint64.
func AppendU64Frame(dst []byte, typ byte, v uint64) []byte {
	var p [8]byte
	binary.BigEndian.PutUint64(p[:], v)
	return AppendFrame(dst, typ, p[:])
}

// U64 decodes a uint64 payload.
func U64(payload []byte) (uint64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("proto: expected 8-byte payload, got %d", len(payload))
	}
	return binary.BigEndian.Uint64(payload), nil
}

// WriteFrame writes one frame with a single Write call.
func WriteFrame(w io.Writer, typ byte, payload []byte) error {
	_, err := w.Write(AppendFrame(make([]byte, 0, HeaderSize+len(payload)), typ, payload))
	return err
}

// ReadFrame reads one frame. The payload aliases buf when it fits.
func ReadFrame(r io.Reader, buf []byte) (typ byte, payload []byte, err error) {
	var hdr [HeaderSize]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := int(hdr[1])<<16 | int(hdr[2])<<8 | int(hdr[3])
	if n > MaxFrame {
		return 0, nil, ErrFrameTooLarge
	}
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	payload = buf[:n]
	if _, err = io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return hdr[0], payload, nil
}

// WriteJSONFrame writes a frame whose payload is v encoded as JSON.
func WriteJSONFrame(w io.Writer, typ byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return WriteFrame(w, typ, b)
}
