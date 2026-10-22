# Protocol

Everything the client and agent exchange, from the bytes on the link up to the local control API. Constants are defined in `internal/proto`; the version number is in `internal/version`.

## 1. Link

A link is one bidirectional byte pipe between client and agent. On the client it is the stdin/stdout of `ssh <host> '<remote_command>'`. On the destination, the relay (`gotunnel agent stdio`) connects those pipes to agentd's unix socket.

### 1.1 Preamble

Before anything else the relay writes `GOTUNNEL/1\n`. The client discards every byte before it (up to 64 KiB), because remote shell startup files may print text. The agent never writes to stdout before the preamble.

### 1.2 Frames

After the preamble, both directions carry frames:

```
+--------+-----------------------+----------------+
| type   | length (24-bit, BE)   | payload        |
| 1 byte | 3 bytes               | length bytes   |
+--------+-----------------------+----------------+
```

`ReadFrame` rejects payloads larger than 1 MiB.

| Type | Name | Direction | Payload |
|---|---|---|---|
| 0x01 | HELLO | client → agent | JSON `proto.Hello` |
| 0x02 | WELCOME | agent → client | JSON `proto.Welcome` |
| 0x03 | REJECT | agent → client | JSON `proto.Reject` |
| 0x04 | ADMIN | CLI/relay → agentd | JSON `proto.AdminRequest` (only on a direct socket connection, never through ssh) |
| 0x05 | ADMIN-RESP | agentd → CLI/relay | JSON `proto.AdminResponse` |
| 0x10 | DATA | both | session payload bytes (at most 4 KiB per frame) |
| 0x11 | ACK | both | uint64 BE: total payload bytes received |
| 0x12 | PING | both | uint64 BE nonce (sender's monotonic nanoseconds) |
| 0x13 | PONG | both | the PING nonce, echoed |
| 0x14 | BYE | both | empty: the sender closed the session |

## 2. Handshake

The client sends HELLO. The agent answers WELCOME or REJECT. After `unknown_session` or `bad_token`, the client may send another HELLO on the same link, which starts a new session. agentd reads at most 4 handshake frames per link, each within 2 minutes.

```json
HELLO   {"proto":1,"version":"v0.1.0","session":"<32 hex>","token":"<64 hex>","recv":123456,
         "ttl_ms":1800000,"ping_ms":10000,"dead_after_ms":45000,
         "max_inflight":8388608,"inflight":65536,"adaptive":true}
WELCOME {"proto":1,"version":"v0.1.0","session":"<32 hex>","token":"<64 hex>","recv":98765,"resumed":true}
REJECT  {"code":"unknown_session","message":"session expired or agent restarted"}
```

- `session` and `token` are empty for a new session. The agent generates a random 128-bit ID and a 256-bit token. The token travels only inside ssh and is compared in constant time.
- `recv` is the number of session payload bytes the sender has received. Each side resends everything after the other side's `recv`.
- `resumed` is true when an existing session was attached.
- Session parameters are chosen by the client and clamped by the agent: TTL 1m–24h, ping 1s–5m, dead-after 5s–30m (the agent uses twice that), max_inflight 256 KiB–64 MiB, inflight 8 KiB–64 MiB.

| Reject code | Meaning | Client reaction |
|---|---|---|
| `proto` | `Hello.proto` ≠ the agent's `version.Proto` | fail and log; the user must run `install-agent` |
| `unknown_session` | session expired or agentd restarted | drop the old session (connections reset); send HELLO for a new one on the same link |
| `bad_token` | token does not match | same as `unknown_session` |
| `bad_request` | malformed HELLO | fail |

## 3. Session semantics

The session payload is a single byte stream in each direction. Implicit offsets number the bytes.

- **Send.** The sender keeps unacknowledged bytes and never holds more than its send limit. While no link is attached, writers block once the limit is reached.
- **ACK.** The receiver sends `ACK(recvOffset)` once 4 KiB is unacknowledged or 25 ms has passed, and also with any other frame it sends. An ACK above what was actually sent on the current link is a protocol error, and the link is detached.
- **Resume.** On attach, the sender discards bytes up to the peer's `recv` and resends the rest. A peer `recv` outside `[sendBase, sendNext]` is `ErrBadOffset`, and the agent closes that session.
- **Liveness.** PING goes out every ping interval. Any received frame counts as life. If nothing arrives for dead-after, the link is detached; the client then reconnects, and the agent just waits. After system sleep (the wall clock jumped more than 3s past the monotonic clock), a PING must be answered within 10s.
- **BYE** ends the session for both sides. A client sends it on idle close and on shutdown; agentd sends it when it shuts down.

## 4. Streams (yamux)

Inside the session payload runs a [yamux](https://github.com/hashicorp/yamux/blob/master/spec.md) session. The client is the yamux client (odd stream IDs) and the agent the server (even IDs); both can open streams. The configuration is in `tunnel.YamuxConfig`:

- keepalive off;
- `StreamOpenTimeout` 0;
- `ConnectionWriteTimeout` TTL+5m;
- `StreamCloseTimeout` max(TTL, 5m);
- window 256 KiB.

The scheduler changes the yamux frame sequence in two ways that the peer must accept:
1. **Splitting.** A data frame larger than 4 KiB is split into consecutive data frames of the same stream. SYN/ACK flags go on the first piece, FIN/RST on the last.
2. **Reset injection.** `sched.Conn.Reset(id)` sends a window-update frame with the RST flag (`flags=0x8`, length 0) after the stream's pending frames.

### 4.1 Requests

The first bytes on every stream are a request, and the opener waits for a response:

```
2-byte BE length | JSON
```

Messages are at most 64 KiB.

```json
Request  {"op":"dial","target":"db.internal:5432","prio":"interactive"}
Response {"ok":true}
Response {"ok":false,"code":"refused","error":"dial tcp 10.0.0.5:5432: connect: connection refused"}
```

| op | Opened by | Fields | Effect |
|---|---|---|---|
| `dial` | client | `target`, `prio` | agent connects to `target` (10s timeout); after OK the stream carries raw bytes |
| `listen` | client | `name`, `addr`, `prio` | agent opens a reverse listener owned by the session (replacing one with the same name) |
| `unlisten` | client | `name` | agent closes that listener |
| `reverse` | agent | `name` | a connection arrived on listener `name`; the client dials its configured target and replies |

Error codes for failed dials: `refused`, `unreachable`, `timeout`, `dns`, `error`. The client's SOCKS server maps them to SOCKS5 replies: 0x05, 0x03, 0x06, 0x04, 0x01.

`prio` is `interactive`, `normal` or `bulk`. Each side applies it to its own scheduler for that stream.

### 4.2 Data and closing

After OK, the stream is a transparent byte pipe. Half-close is propagated: yamux `Close` is FIN, and on TCP it maps to `CloseWrite`. If either side's copy fails, both connections are closed and the stream is reset (RST injection).

## 5. Admin protocol (agentd socket)

Any process with access to `run/agentd.sock` (mode 0600, directory 0700) may send one ADMIN frame instead of HELLO:

```json
ADMIN      {"cmd":"status"}            or {"cmd":"stop"}
ADMIN-RESP {"ok":true,"pid":123,"version":"v0.1.0","started":"…","managed":"auto",
            "idle_exit":"1h0m0s","sessions":[{"id":"…","created":"…","attached":true,
            "detached":"…","ttl":"30m0s","streams":2,"listeners":["back=127.0.0.1:19000"],
            "bytes_in":1234,"bytes_out":5678}]}
```

`stop` answers first, then shuts down. Shutdown sends BYE to attached clients.

## 6. Install protocol (`install-agent`)

This runs over plain ssh (`sh -c '<script>'`), not over the agent protocol:
1. The remote script prints `GOTUNNEL-INSTALL/1`, then `uname -sm`. The client skips any output before the marker.
2. The client sends `<size>\n` followed by `size` bytes of a gzipped binary.
3. The script runs `head -c size | gunzip` into a temporary file, `chmod 755`, `mv` it over the target, and runs `<target> version`.

## 7. Client control API

The client daemon serves HTTP+JSON on `run/ctl.sock` (mode 0600):

| Method and path | Body | Result |
|---|---|---|
| `GET /status` | none | `client.Status` (see [operations.md](operations.md#status-fields)) |
| `POST /forwards` | `config.Forward` | adds a forward (bind errors fail the request) |
| `DELETE /forwards/{name}` | none | removes it; open connections continue |
| `POST /reverse` | `config.Reverse` | adds a reverse forward (registered when a session exists) |
| `DELETE /reverse/{name}` | none | removes it and sends `unlisten` |
| `POST /reload` | none | `{"warnings":[…]}` after re-reading the config file |
| `POST /reconnect` | none | drops the current link; the session resumes |
| `POST /shutdown` | none | stops the daemon |

Errors are `4xx` with `{"error":"…"}`.

Example: `curl --unix-socket ~/.gotunnel/run/ctl.sock http://x/status`.

## 8. Compatibility rules

- **Additive JSON fields are compatible.** Go's `encoding/json` ignores unknown fields, and a missing field decodes as its zero value. Make sure the zero value means "old behaviour".
- **A new `op` is compatible** only if old peers can answer it with an error response. Old agents reply `ok:false` with "unknown operation".
- **Anything else requires bumping `version.Proto`:** changing frame types or layout, offsets or ACK semantics, the preamble, or yamux settings that change behaviour on the wire. An agent rejects mismatched clients with `proto`, and users must then run `install-agent`.
- `version.Version` (the module version from the build info) is informational. A mismatch only produces a warning and an automatic agentd restart when it is idle.
