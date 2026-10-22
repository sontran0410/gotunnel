package proto

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := bytes.Repeat([]byte("x"), 70000)
	if err := WriteFrame(&buf, FrameData, payload); err != nil {
		t.Fatal(err)
	}
	buf.Write(AppendU64Frame(nil, FrameAck, 1<<40+7))
	typ, p, err := ReadFrame(&buf, nil)
	if err != nil || typ != FrameData || !bytes.Equal(p, payload) {
		t.Fatalf("data frame: typ=%x len=%d err=%v", typ, len(p), err)
	}
	typ, p, err = ReadFrame(&buf, make([]byte, 16))
	if err != nil || typ != FrameAck {
		t.Fatalf("ack frame: typ=%x err=%v", typ, err)
	}
	if v, err := U64(p); err != nil || v != 1<<40+7 {
		t.Fatalf("U64 = %d, %v", v, err)
	}
	if _, _, err := ReadFrame(&buf, nil); err != io.EOF {
		t.Fatalf("err at end = %v, want EOF", err)
	}
}

func TestReadFrameTruncated(t *testing.T) {
	b := AppendFrame(nil, FrameData, []byte("hello"))
	_, _, err := ReadFrame(bytes.NewReader(b[:6]), nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
}

func TestReadFrameTooLarge(t *testing.T) {
	var hdr [HeaderSize]byte
	PutHeader(hdr[:], FrameData, MaxFrame+1)
	if _, _, err := ReadFrame(bytes.NewReader(hdr[:]), nil); err != ErrFrameTooLarge {
		t.Fatalf("err = %v", err)
	}
}

func TestReadPreambleSkipsGarbage(t *testing.T) {
	for _, garbage := range []string{"", "Welcome!\n", "GOTUNNEL/2\nGOT", "GGGOTUNNEL/"} {
		r := bufio.NewReader(strings.NewReader(garbage + Preamble + "rest"))
		skipped, err := ReadPreamble(r, 1024)
		if err != nil {
			t.Fatalf("garbage %q: %v", garbage, err)
		}
		if string(skipped) != garbage {
			t.Errorf("skipped = %q, want %q", skipped, garbage)
		}
		rest, _ := io.ReadAll(r)
		if string(rest) != "rest" {
			t.Errorf("rest = %q", rest)
		}
	}
}

func TestReadPreambleLimit(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(strings.Repeat("x", 100) + Preamble))
	if _, err := ReadPreamble(r, 10); err == nil {
		t.Fatal("expected error")
	}
	r = bufio.NewReader(strings.NewReader("no preamble"))
	if _, err := ReadPreamble(r, 1024); err == nil {
		t.Fatal("expected EOF error")
	}
}

func TestMsgRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, Request{Op: OpDial, Target: "db:5432", Prio: "bulk"}); err != nil {
		t.Fatal(err)
	}
	var req Request
	if err := ReadMsg(&buf, &req); err != nil || req.Target != "db:5432" || req.Prio != "bulk" {
		t.Fatalf("req = %+v, err = %v", req, err)
	}
	resp := Response{OK: false, Code: CodeRefused, Error: "connection refused"}
	var re *RemoteError
	if err := resp.Err(); !errors.As(err, &re) || re.Code != CodeRefused {
		t.Fatalf("Err() = %v", err)
	}
}
