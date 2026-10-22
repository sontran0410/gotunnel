package client

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"

	"gotunnel/internal/proto"
)

// SOCKS5 (RFC 1928), CONNECT only, no authentication. Domain names are passed
// to the agent unresolved so they are resolved on the destination side.

const (
	socksVersion          = 5
	socksNoAuth           = 0x00
	socksNoAcceptable     = 0xff
	socksCmdConnect       = 0x01
	socksAtypIPv4         = 0x01
	socksAtypDomain       = 0x03
	socksAtypIPv6         = 0x04
	socksSucceeded        = 0x00
	socksGeneralFailure   = 0x01
	socksNetUnreachable   = 0x03
	socksHostUnreachable  = 0x04
	socksConnRefused      = 0x05
	socksTTLExpired       = 0x06
	socksCmdNotSupported  = 0x07
	socksAtypNotSupported = 0x08
)

// socksHandshake negotiates with a SOCKS5 client and returns the requested
// "host:port". Protocol errors are answered before returning.
func socksHandshake(rw io.ReadWriter) (string, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(rw, hdr[:]); err != nil {
		return "", err
	}
	if hdr[0] != socksVersion {
		return "", fmt.Errorf("not a SOCKS5 client (version byte %d)", hdr[0])
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(rw, methods); err != nil {
		return "", err
	}
	noAuth := false
	for _, m := range methods {
		if m == socksNoAuth {
			noAuth = true
		}
	}
	if !noAuth {
		rw.Write([]byte{socksVersion, socksNoAcceptable})
		return "", errors.New("client requires authentication")
	}
	if _, err := rw.Write([]byte{socksVersion, socksNoAuth}); err != nil {
		return "", err
	}

	var req [4]byte
	if _, err := io.ReadFull(rw, req[:]); err != nil {
		return "", err
	}
	if req[0] != socksVersion {
		return "", errors.New("bad request version")
	}
	var host string
	switch req[3] {
	case socksAtypIPv4:
		var ip [4]byte
		if _, err := io.ReadFull(rw, ip[:]); err != nil {
			return "", err
		}
		host = net.IP(ip[:]).String()
	case socksAtypIPv6:
		var ip [16]byte
		if _, err := io.ReadFull(rw, ip[:]); err != nil {
			return "", err
		}
		host = net.IP(ip[:]).String()
	case socksAtypDomain:
		var n [1]byte
		if _, err := io.ReadFull(rw, n[:]); err != nil {
			return "", err
		}
		name := make([]byte, n[0])
		if _, err := io.ReadFull(rw, name); err != nil {
			return "", err
		}
		host = string(name)
	default:
		socksReply(rw, socksAtypNotSupported)
		return "", fmt.Errorf("unsupported address type %d", req[3])
	}
	var port [2]byte
	if _, err := io.ReadFull(rw, port[:]); err != nil {
		return "", err
	}
	if req[1] != socksCmdConnect {
		socksReply(rw, socksCmdNotSupported)
		return "", fmt.Errorf("unsupported command %d (only CONNECT)", req[1])
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port[0])<<8|int(port[1]))), nil
}

// socksReply sends a reply with an unspecified bound address.
func socksReply(w io.Writer, code byte) error {
	_, err := w.Write([]byte{socksVersion, code, 0, socksAtypIPv4, 0, 0, 0, 0, 0, 0})
	return err
}

// socksReplyCode maps an open error to a SOCKS reply code.
func socksReplyCode(err error) byte {
	var re *proto.RemoteError
	if !errors.As(err, &re) {
		return socksGeneralFailure
	}
	switch re.Code {
	case proto.CodeRefused:
		return socksConnRefused
	case proto.CodeDNS:
		return socksHostUnreachable
	case proto.CodeUnreachable:
		return socksNetUnreachable
	case proto.CodeTimeout:
		return socksTTLExpired
	}
	return socksGeneralFailure
}
