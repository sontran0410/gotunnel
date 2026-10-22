// Package control is the client daemon's local API: JSON over HTTP on a unix
// socket that only the owner can open. The CLI uses it to query and change a
// running daemon.
package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"gotunnel/internal/config"
)

// API is implemented by the daemon.
type API interface {
	Status() any
	AddForward(config.Forward) error
	RemoveForward(name string) error
	AddReverse(config.Reverse) error
	RemoveReverse(name string) error
	Reload() []error
	Reconnect()
	Shutdown()
}

// Handler returns the HTTP handler serving api.
func Handler(api API) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, api.Status())
	})
	mux.HandleFunc("POST /forwards", func(w http.ResponseWriter, r *http.Request) {
		var f config.Forward
		if !decode(w, r, &f) {
			return
		}
		reply(w, api.AddForward(f))
	})
	mux.HandleFunc("DELETE /forwards/{name}", func(w http.ResponseWriter, r *http.Request) {
		reply(w, api.RemoveForward(r.PathValue("name")))
	})
	mux.HandleFunc("POST /reverse", func(w http.ResponseWriter, r *http.Request) {
		var rv config.Reverse
		if !decode(w, r, &rv) {
			return
		}
		reply(w, api.AddReverse(rv))
	})
	mux.HandleFunc("DELETE /reverse/{name}", func(w http.ResponseWriter, r *http.Request) {
		reply(w, api.RemoveReverse(r.PathValue("name")))
	})
	mux.HandleFunc("POST /reload", func(w http.ResponseWriter, r *http.Request) {
		res := ReloadResult{Warnings: []string{}}
		for _, err := range api.Reload() {
			res.Warnings = append(res.Warnings, err.Error())
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("POST /reconnect", func(w http.ResponseWriter, r *http.Request) {
		api.Reconnect()
		reply(w, nil)
	})
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, r *http.Request) {
		reply(w, nil)
		go api.Shutdown()
	})
	return mux
}

// ReloadResult is returned by POST /reload.
type ReloadResult struct {
	Warnings []string `json:"warnings"`
}

type errorBody struct {
	Error string `json:"error"`
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "malformed request: " + err.Error()})
		return false
	}
	return true
}

func reply(w http.ResponseWriter, err error) {
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, struct {
		OK bool `json:"ok"`
	}{true})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// Listen opens the control socket (mode 0600), replacing a stale one. The
// caller must hold the daemon lock.
func Listen(sock string) (net.Listener, error) {
	os.Remove(sock)
	l, err := net.Listen("unix", sock)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sock, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

// ErrNotRunning is returned when no daemon answers on the socket.
var ErrNotRunning = errors.New("gotunnel is not running")

// Client talks to a running daemon.
type Client struct {
	hc *http.Client
}

// NewClient returns a client for the daemon listening on sock.
func NewClient(sock string) *Client {
	return &Client{hc: &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}}
}

// Do performs a request; body and out may be nil.
func (c *Client) Do(ctx context.Context, method, path string, body, out any) error {
	var rd *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://gotunnel"+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Op == "dial" {
			return ErrNotRunning
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var eb errorBody
		if json.NewDecoder(resp.Body).Decode(&eb) == nil && eb.Error != "" {
			return errors.New(eb.Error)
		}
		return fmt.Errorf("daemon answered %s", resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
