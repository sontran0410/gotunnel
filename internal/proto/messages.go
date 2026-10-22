package proto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Hello opens a new session (Session empty) or resumes an existing one.
type Hello struct {
	Proto   int    `json:"proto"`
	Version string `json:"version"`
	Session string `json:"session,omitempty"` // hex session ID when resuming
	Token   string `json:"token,omitempty"`   // hex session token when resuming
	Recv    uint64 `json:"recv"`              // payload bytes the client has received
	// Session parameters chosen by the client.
	TTLMillis       int64 `json:"ttl_ms"`
	PingMillis      int64 `json:"ping_ms"`
	DeadAfterMillis int64 `json:"dead_after_ms"`
	MaxInflight     int   `json:"max_inflight"`
	Inflight        int   `json:"inflight"`
	Adaptive        bool  `json:"adaptive"`
}

// TTL returns the requested session TTL.
func (h Hello) TTL() time.Duration { return time.Duration(h.TTLMillis) * time.Millisecond }

// PingInterval returns the requested ping interval.
func (h Hello) PingInterval() time.Duration { return time.Duration(h.PingMillis) * time.Millisecond }

// DeadAfter returns the client's dead-link threshold.
func (h Hello) DeadAfter() time.Duration { return time.Duration(h.DeadAfterMillis) * time.Millisecond }

// Welcome accepts a Hello.
type Welcome struct {
	Proto   int    `json:"proto"`
	Version string `json:"version"`
	Session string `json:"session"`
	Token   string `json:"token"`
	Recv    uint64 `json:"recv"` // payload bytes the agent has received
	Resumed bool   `json:"resumed"`
}

// Reject codes.
const (
	RejectProto          = "proto"           // incompatible protocol version
	RejectUnknownSession = "unknown_session" // session expired or never existed
	RejectBadToken       = "bad_token"       // token does not match the session
	RejectBadRequest     = "bad_request"
)

// Reject refuses a Hello. After an unknown_session or bad_token reject the
// client may send a new Hello on the same link.
type Reject struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (r *Reject) Error() string {
	return fmt.Sprintf("agent rejected handshake: %s: %s", r.Code, r.Message)
}

// Admin commands understood by agentd.
const (
	AdminStatus = "status"
	AdminStop   = "stop"
)

// AdminRequest is sent by the CLI or a relay to agentd.
type AdminRequest struct {
	Cmd string `json:"cmd"`
}

// AdminResponse answers an AdminRequest.
type AdminResponse struct {
	OK       bool          `json:"ok"`
	Error    string        `json:"error,omitempty"`
	PID      int           `json:"pid"`
	Version  string        `json:"version"`
	Started  time.Time     `json:"started"`
	Managed  string        `json:"managed,omitempty"`
	IdleExit string        `json:"idle_exit,omitempty"`
	Sessions []SessionInfo `json:"sessions"`
}

// SessionInfo describes one agent session.
type SessionInfo struct {
	ID        string    `json:"id"`
	Created   time.Time `json:"created"`
	Attached  bool      `json:"attached"`
	Detached  time.Time `json:"detached,omitempty"`
	TTL       string    `json:"ttl"`
	Streams   int       `json:"streams"`
	Listeners []string  `json:"listeners,omitempty"`
	BytesIn   uint64    `json:"bytes_in"`
	BytesOut  uint64    `json:"bytes_out"`
}

// Stream request operations.
const (
	OpDial     = "dial"     // client → agent: connect to Target
	OpListen   = "listen"   // client → agent: open reverse listener Name on Addr
	OpUnlisten = "unlisten" // client → agent: close reverse listener Name
	OpReverse  = "reverse"  // agent → client: connection arrived on listener Name
)

// Request is the first message on every yamux stream.
type Request struct {
	Op     string `json:"op"`
	Target string `json:"target,omitempty"`
	Name   string `json:"name,omitempty"`
	Addr   string `json:"addr,omitempty"`
	Prio   string `json:"prio,omitempty"`
}

// Response codes for failed dials.
const (
	CodeRefused     = "refused"
	CodeUnreachable = "unreachable"
	CodeTimeout     = "timeout"
	CodeDNS         = "dns"
	CodeError       = "error"
)

// Response answers a Request.
type Response struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`
	Error string `json:"error,omitempty"`
}

// Err converts a failed response into an error.
func (r *Response) Err() error {
	if r.OK {
		return nil
	}
	return &RemoteError{Code: r.Code, Msg: r.Error}
}

// RemoteError is an error reported by the other side of a stream.
type RemoteError struct {
	Code string
	Msg  string
}

func (e *RemoteError) Error() string { return e.Msg }

const maxMsg = 64 << 10

// WriteMsg writes v as JSON prefixed with a 2-byte length.
func WriteMsg(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > maxMsg-1 {
		return errors.New("proto: message too large")
	}
	buf := make([]byte, 2+len(b))
	binary.BigEndian.PutUint16(buf, uint16(len(b)))
	copy(buf[2:], b)
	_, err = w.Write(buf)
	return err
}

// ReadMsg reads a message written by WriteMsg into v.
func ReadMsg(r io.Reader, v any) error {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	b := make([]byte, binary.BigEndian.Uint16(hdr[:]))
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
