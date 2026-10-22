# Architecture

This document explains how gotunnel is built and why. Read it before changing anything below `internal/client` or `internal/agent`. Wire formats are in [protocol.md](protocol.md); rules and recipes for changes are in [maintaining.md](maintaining.md).

## 1. Problem and goals

The client machine must forward many TCP ports to a destination that is reachable only through several ssh jump hosts:

- One hop may need `ProxyCommand openssl s_client -quiet -connect …`.
- Setting up that path takes a long time, and bandwidth is low.
- Software can be installed on the client and the destination, never on the jump hosts.

Goals:
1. Pay the expensive ssh setup once, then carry all forwards over that one connection.
2. Keep open TCP connections alive across link failures (resume).
3. Support always-on and on-demand forwards, SOCKS5, reverse forwards, and runtime changes.
4. Keep interactive traffic responsive while bulk transfers run.
5. Run as a daemon on both ends.

Non-goals: UDP, a VPN/TUN device, more than one destination per instance, and Windows.

## 2. Processes

```
CLIENT MACHINE                                        DESTINATION
gotunnel up (client daemon)                           agentd = "gotunnel agent run"
 ├─ local listeners (forwards, SOCKS)                  ├─ unix socket ~/.gotunnel/run/agentd.sock
 ├─ control API: HTTP on unix socket run/ctl.sock      ├─ sessions (survive link loss until TTL)
 ├─ Supervisor ── spawns ──▶ ssh <alias> … ──────┐     ├─ connections to targets
 │                          (own process group,   │     └─ reverse listeners
 │                           ProxyCommand children)│
 │                                                └──▶ sshd ──▶ relay = "gotunnel agent stdio"
 │                                                              (one per link: stdio ⇄ agentd socket)
 └─ CLI commands talk to the daemon over run/ctl.sock
```

- **Client daemon**: one process per `GOTUNNEL_HOME` (enforced with flock on `run/client.lock`).
- **ssh**: one process per link. It is started with `Setpgid` so that killing the group also kills ProxyCommand children such as `openssl s_client`.
- **relay**: started by sshd for each link. It writes the preamble, connects to agentd (starting it if needed), and copies bytes both ways. It dies with the ssh connection.
- **agentd**: a long-lived process, one per destination user (flock on `run/agentd.lock`). Because session state lives here rather than in the relay, a session can outlive any single ssh connection.

## 3. Layers

```
application TCP connection
   │ netutil.Splice (half-close aware, 8 KiB buffers)
yamux stream ── first message: proto.Request / proto.Response (JSON, 2-byte length)
   │ github.com/hashicorp/yamux session (config: tunnel.YamuxConfig)
sched.Conn ── yamux frames → per-stream queues → DRR → session (blocks at the send limit)
   │                                ▲ sched.Controller sets the limit from RTT/bandwidth samples
session.Session ── offsets, ACK, replay buffer, ping, dead-link detection, resume
   │ proto frames: type(1) | length(3) | payload
link ── client: ssh stdio pipes; agent: unix socket to the relay
```

`internal/tunnel.NewStack` builds the yamux, scheduler, controller and session layers identically on both sides. The only difference is the yamux role (client or server).

### 3.1 Transport (`internal/transport`)

`transport.SSH.Dial` runs the system ssh client:

```
ssh -T -e none -o BatchMode=yes -o ControlPath=none -o ClearAllForwardings=yes \
    -o RemoteCommand=none -o ServerAliveInterval=15 -o ServerAliveCountMax=3 [-C] \
    [extra_args…] -- <host> '<remote_command>'
```

| Option | Why |
|---|---|
| `-T -e none` | No tty and no escape character, so the pipe is binary-clean |
| `BatchMode=yes` | Never prompt, because a daemon has nobody to answer; fail fast instead of hanging |
| `ControlPath=none` | Never attach to or become a ControlMaster. gotunnel must own the connection's lifetime, so killing ssh really closes the path |
| `ClearAllForwardings=yes` | Ignore `LocalForward`/`RemoteForward` lines of the alias, which would compete for ports |
| `RemoteCommand=none` | An alias with `RemoteCommand` would otherwise refuse a command-line command |
| `ServerAlive*` | A second line of dead-link detection inside ssh |

The link is ssh's stdin/stdout, using `os.Pipe` (not `cmd.StdoutPipe`, so `Wait` does not race with reads). ssh's stderr is kept (last 20 lines) and logged as `ssh: …`. `Diagnose()` turns it into errors such as "ssh exit status 255: Permission denied (publickey)".

`Close()` closes stdin first. ssh then delivers what was already written (for example a final BYE) and ends the session by itself. If ssh is still running after 1s, it gets SIGTERM; after 2s more, SIGKILL to the whole process group. `Close` runs in a goroutine on detach, so the grace period never delays a reconnect.

### 3.2 Session (`internal/session`)

A `Session` is a byte stream (`io.ReadWriteCloser`) that survives link changes:

- **Offsets.** Each payload byte has an implicit sequence number: its offset in the stream. DATA frames carry at most 4 KiB.
- **Send side.** `Write` appends to a ring buffer of unacknowledged bytes covering `[sendBase, sendBase+len)`. It blocks while that buffer holds `SendLimit` bytes, which is the backpressure, and that includes the time when no link is attached. The writer goroutine sends bytes from `wire` onwards.
- **Receive side.** Received bytes go into a ring buffer for `Read`, and `recvOffset` advances. An ACK carrying `recvOffset` is sent after 4 KiB or 25 ms, whichever comes first, and also rides along with any other outgoing frame.
- **ACK handling.** An ACK drops acknowledged bytes from the send buffer and produces a `Sample` (RTT, bytes in flight at send time, delivery rate) for the controller.
- **Attach(r, w, c, peerRecv).** Validates that `sendBase ≤ peerRecv ≤ sendNext` (otherwise `ErrBadOffset`) and discards everything the peer already has. It sets `wire = peerRecv` so the rest is resent, then starts three goroutines for the link: `readLoop`, `writeLoop` and `monitor`.
- **Resume(…, welcome).** Used by the agent. Under the session lock it detaches the old link, calls `welcome(recvOffset)` (which writes WELCOME to the new link), then attaches. This keeps the reported offset consistent even when two links race.
- **monitor.** Ticks every second:
  - sends PING every `PingInterval`;
  - detaches with `ErrDeadLink` if nothing at all arrived for `DeadAfter`;
  - detects system sleep (the wall clock jumped more than 3s past the monotonic clock) and then requires an answer within `WakeProbe` (10s).
- **Close()** queues BYE, waits up to 2s for it to be written, then detaches. **Abort()** closes without notifying the peer. Receiving BYE closes the session with `ErrPeerClosed`.

Detaching closes `link.done`, closes the link asynchronously and records `detachedAt`. A detached session keeps all its state; `LinkDone()` lets the supervisor wait for detachment.

### 3.3 Scheduler and controller (`internal/sched`)

`sched.Conn` is the `io.ReadWriteCloser` handed to yamux. Reads pass straight to the session. Writes are where it works:

- **Write never blocks on I/O.** It parses yamux frames from the byte stream (yamux writes header and body in separate calls), splits data frames larger than 4 KiB, and queues them, which bounds the granularity of interleaving.
- **Queues.**
  - Stream-0 frames (yamux ping and GoAway) go to a control queue that is always served first.
  - Every other stream has its own FIFO. Order within a stream is never changed, so FIN and RST stay behind that stream's data.
- **Dequeue.** Deficit round robin in fq_codel style:
  - A stream that just became active joins the "new" list and is served before streams in the "old" list. Sparse traffic such as keystrokes therefore goes almost immediately.
  - A stream that used up its quantum moves to the back of the old list.
  - Quanta per class: interactive 32 KiB, normal 16 KiB, bulk 4 KiB.
- **pump.** A goroutine moves one frame at a time into `session.Write`. That call blocks at the send limit, so the backlog waits here, where it can still be reordered, instead of in ssh's buffers, where it cannot.
- **Memory bound.** yamux never sends more than a stream's window (256 KiB) before the receiver consumes it, so queued bytes are at most streams × 256 KiB.
- **`SetClass` / `Forget`.** Assign a priority to a stream ID and release it. Classes are set right after a stream opens (client) or once its request is read (agent).
- **`Reset(id)`.** yamux has no public way to reset a stream, so this queues a yamux window-update frame with the RST flag behind the stream's pending frames. `tunnel.Proxy` uses it when a splice ends with an error, so the peer stops sending instead of filling a window nobody reads.

`sched.Controller` decides the session's send limit (bytes in flight), in the manner of TCP BBR:
- **Bandwidth.** `bw` is the maximum delivery rate over a window of max(1s, 10 × minRTT).
- **Delay.** `minRTT` is the lowest RTT. If it has not been refreshed for 30s, the controller drains to the minimum limit for max(200ms, 2 × minRTT) and re-measures it (a standing queue would otherwise hide the true RTT).
- **Limit.** `bw × minRTT × gain + 8 KiB headroom`, clamped to [8 KiB, replay_buffer]. The gain is 2.0 while bandwidth grew by at least 25% within the last round (one minRTT); after three rounds without such growth it is 1.25.
- **Fixed mode.** With `adaptive_inflight: false` the limit stays at `max_inflight`.

Measured with `internal/client/latency_test.go`, 1-byte echo through an interactive forward during a bulk download:

| Link | Without in-flight control | With scheduler + controller | Link used by the download |
|---|---|---|---|
| 1 Mbit/s, 100 ms RTT | 1.9 s | about 250 ms | 87–94% |
| 256 kbit/s, 300 ms RTT | 7.9 s | about 0.8 s | about 78% |

### 3.4 Client (`internal/client`)

- **`Supervisor`** (`supervisor.go`) is the transport state machine; its diagram is in the type comment.
  - `Run` is the only goroutine that dials.
  - The transport is *wanted* while any of these holds: an `always` forward or any reverse forward exists, a proxied connection is active, or less than `idle_timeout` has passed since the last activity. `Stack(ctx)` counts as activity and waits for a session.
  - **`connect`** dials, skips any remote output before the preamble, sends HELLO and reads WELCOME or REJECT:
    - WELCOME with `resumed` attaches the existing session;
    - any other WELCOME builds a new stack and calls `onSession`;
    - REJECT `unknown_session` or `bad_token` drops the old session (its streams reset) and retries with a new HELLO on the same link (2 attempts).
    - `connect_timeout` bounds the whole exchange. On failure the error includes ssh's diagnosis.
  - **After a failure** it backs off for 1s, doubling up to 60s, with jitter of 50–100% of the value.
  - **While connected** it waits for link loss (then resumes), session end (then drops it), a reconnect request (then detaches the link), or idleness (checked every 5s and on activity; then closes with BYE).
- **`Client`** (`client.go`) owns the listeners:
  - **Forwards and SOCKS** use a `listener` that retries binding every 30s when the port is busy. Each accepted connection calls `open`, which waits for a session and sends `Request{dial}`; `open` retries once if the session was replaced in flight. The connection is then spliced with `Stack.Proxy`.
  - **Reverse forwards** are registered (`Request{listen}`) on every new session by `onSession`, which then accepts agent-opened streams (`Request{reverse}`) and dials the local target.
  - **Runtime changes** are `AddForward`/`RemoveForward`/`AddReverse`/`RemoveReverse`. `Apply` reconciles a reloaded config by name. ssh/connection/daemon changes only produce a "restart required" warning.
- **`socks.go`** implements SOCKS5 CONNECT without authentication. Domain names are passed unresolved, so they are resolved on the destination. Dial errors are mapped to SOCKS reply codes.

### 3.5 Agent (`internal/agent`)

- **`RunRelay`** ("agent stdio"):
  1. Writes `proto.Preamble`, then `Connect(spawn=true)`. This replaces an idle agentd of another version (unless systemd or launchd manages it) and starts agentd if it is missing (`agent run --idle-exit 1h --managed auto`).
  2. Copies bytes between stdio and the socket until either side ends.
  3. Never writes anything else to stdout.
- **`RunStandalone`** ("agent stdio --standalone") serves a single link in-process. Sessions die with the link. Use it for debugging only.
- **`Server`** (agentd):
  - `HandleConn` reads up to 4 frames, each within 2 minutes:
    - an ADMIN frame gets a status or stop answer;
    - HELLO creates a session (new ID and token), resumes one (`Session.Resume`), or rejects (`proto`, `unknown_session`, `bad_token`, `bad_request`). After a rejected resume the client may send another HELLO on the same link.
  - **Session parameters** come from HELLO and are clamped: TTL 1m–24h, ping 1s–5m, dead-after 2 × (5s–30m) (the client owns reconnection, the agent only notices abandonment), max in-flight 256 KiB–64 MiB.
  - **`janitor`** runs every 5s. It closes sessions detached for longer than their TTL and, with `--idle-exit`, shuts agentd down after that long without sessions or handshakes.
  - **`agentSession.handleStream`** reads the request (5 min deadline):
    - `dial` connects to the target (10s timeout), replies, then proxies;
    - `listen` / `unlisten` manage reverse listeners owned by the session (they survive link loss and close with the session);
    - each reverse connection opens a stream with `Request{reverse}` (2 min timeout).
  - **`Shutdown`** (from admin stop or a signal) closes every session with BYE.

### 3.6 Daemons, services, CLI

- **`internal/daemon`**:
  - `AcquireLock` uses flock, which the kernel releases when the process dies, so stale locks cannot happen.
  - `Spawn` re-execs the binary with `setsid`, stdin from /dev/null and stdout/stderr appended to `*.out`, and sets `GOTUNNEL_DAEMON_CHILD=1`.
  - `WaitReady` polls until the child answers or exits.
  - `RotatingFile` is the log file (10 MiB × 3 backups); `Follow` implements `logs -f`.
  - `ManagedBy` detects systemd (`INVOCATION_ID`), launchd (`XPC_SERVICE_NAME`) or a background child.
- **`internal/service`**:
  - launchd plist `local.gotunnel.<name>` with `RunAtLoad`, and `KeepAlive.SuccessfulExit=false`: it restarts after crashes but not after `gotunnel down`, which exits 0. The current PATH is recorded so ProxyCommands such as Homebrew's openssl resolve.
  - systemd --user unit (`Restart=on-failure`) with `loginctl enable-linger`.
  - Otherwise a crontab `@reboot` line tagged `# gotunnel:<name>`.
- **`internal/control`**: HTTP+JSON on `run/ctl.sock` (mode 0600). Endpoints are listed in [protocol.md](protocol.md#7-client-control-api).
- **`internal/agentbin`**: gzipped Linux agents embedded with build tag `embedagent` (`make build`). Plain builds embed nothing.
- **`cmd/gotunnel`**: one file per command group. `install-agent` uploads in a single ssh round trip:
  1. a remote script prints a marker and `uname -sm`;
  2. it reads the payload size and the gzipped binary from stdin;
  3. it installs atomically and prints the new version.

## 4. Key flows

**Startup (`up -d`)**
1. The CLI loads the config, spawns `gotunnel up -c <abs path>` detached, and polls `/status` for up to 15s.
2. The child takes `client.lock`, opens the log, starts the listeners and serves `ctl.sock`. `Supervisor.Run` starts; if anything is `always`, it dials immediately.
3. ssh runs `agent stdio`. The relay writes the preamble and connects to agentd, starting it if needed. The client sends HELLO; agentd creates a session and answers WELCOME; both sides attach. `onSession` registers the reverse listeners.

**A connection on a forward**
1. Accept, then `ConnStart` (activity), then `Supervisor.Stack` returns the session, waiting for one to exist.
2. `Stack.Request`: OpenStream, `SetClass`, write `Request{dial, target, prio}`, read the `Response`. The agent dials the target (10s) and replies.
3. `Stack.Proxy` splices until both directions reach EOF. On error it resets the stream (RST injection). Then `ConnEnd`.

**Link loss**
1. The session detaches on a read or write error, or on dead-link detection (no bytes for `dead_after`). Application connections stall; the agent keeps its target connections.
2. The supervisor sees `LinkDone`, counts a link loss and reconnects immediately (backoff applies only after failures). HELLO carries the session ID, token and `recvOffset`.
3. The agent's `Resume` detaches the old link if it was still attached ("newest link wins"), answers WELCOME with its `recvOffset` and attaches. Both sides resend from each other's offset, and the streams continue.

**Session lost** (agent restarted, or detached longer than `session_ttl`): the resume is rejected with `unknown_session`. The client aborts the old stack, so every open connection resets, and creates a new session on the same link. Listeners keep running.

**On-demand and idle**: with no `always` forward and no reverse forward, the first connection triggers the dial. Once there has been no activity for `idle_timeout`, the session is closed with BYE and the state becomes `idle`.

**Shutdown (`down`)**: `POST /shutdown` cancels the context. The supervisor closes the session (BYE, then `sshLink.Close` closes stdin so ssh flushes) and the listeners stop. agentd receives BYE and closes the session and its target connections.

**Agent upgrade**:
1. `install-agent` replaces `~/.gotunnel/bin/gotunnel` atomically (`mv`).
2. The running agentd keeps the old inode.
3. The next relay (new binary) sees a version mismatch. If agentd has no sessions and is not service-managed, the relay stops it and starts the new version with the same idle-exit and managed settings.

## 5. Concurrency model

| Object | Goroutines | Lock and blocking rules |
|---|---|---|
| `session.Session` | per link: `readLoop`, `writeLoop`, `monitor` | `mu` guards all state. `Write`/`Read` wait on `wcond`/`rcond`. Link I/O happens outside `mu`, except the welcome callback in `Resume`. Goroutines exit when `s.link != l` |
| `sched.Conn` | `pump` | `mu` guards the queues. `Write` only queues. `pump` calls `session.Write` without holding `mu` |
| yamux session | its own recv/send loops | the send loop calls `sched.Conn.Write` (never blocks); the recv loop calls `session.Read` |
| `client.Supervisor` | `Run` | `mu` guards state. `changed` is a close-and-replace broadcast channel. Only `Run` dials or swaps the stack |
| `client.Client` | per listener accept loop, per connection | `mu` guards the maps |
| `agent.Server` | accept loop, janitor, per conn/stream | `mu` guards the session map. `agentSession.mu` guards the listeners. `closeOnce` makes `close` idempotent |

## 6. Timeouts and limits

| What | Value | Where |
|---|---|---|
| connect (ssh + preamble + handshake) | `connect_timeout` (3m) | `client/supervisor.go` |
| text accepted before the preamble | 64 KiB | `client/supervisor.go` |
| reconnect backoff after failures | 1s doubling to 60s, jitter 50–100% | `client/supervisor.go` |
| idle check | every 5s | `client/supervisor.go` |
| a local connection waiting for its stream | `connect_timeout` + 30s | `client/client.go` |
| bind retry for busy listen ports | 30s | `client/client.go` |
| SOCKS handshake | 30s | `client/client.go` |
| reverse: request read / local dial | 1m / 10s | `client/client.go` |
| DATA frame / ACK every / ACK delay | 4 KiB / 4 KiB / 25ms | `session/session.go` |
| ping / dead link | `ping_interval` (10s) / `dead_after` (45s); agent uses 2 × `dead_after` | `session`, `agent/server.go` |
| wake probe after sleep | 10s | `tunnel/tunnel.go` |
| BYE flush on Close | 2s | `session/session.go` |
| scheduler chunk / quanta | 4 KiB / 32, 16, 4 KiB | `sched/sched.go` |
| in-flight limit | 8 KiB … `replay_buffer` (8 MiB), starts at `max_inflight` (64 KiB) | `tunnel/tunnel.go`, `sched/controller.go` |
| yamux write / close timeout | TTL + 5m / max(TTL, 5m); open timeout off | `tunnel/tunnel.go` |
| agent: handshake frame / stream request / target dial / reverse request | 2m / 5m / 10s / 2m | `agent/server.go` |
| agent janitor / auto idle exit | 5s / 1h | `agent/server.go`, `agent/run.go` |
| ssh close: stdin EOF grace, SIGTERM grace | 1s, 2s | `transport/ssh.go` |
| max frame / max stream message | 1 MiB / 64 KiB | `proto` |

## 7. Design decisions

- **The system ssh client instead of `x/crypto/ssh`.** The user's `~/.ssh/config` already knows the jump chain, the openssl ProxyCommand, keys and known hosts, so reusing it costs nothing and is exactly compatible. The `Transport` interface leaves room for a native implementation.
- **A relay plus a long-lived agentd.** Processes started by sshd die with their connection, so state that must survive link loss cannot live in them. This is the same reason tmux has a server.
- **yamux, with our own session underneath.** yamux provides well-tested streams and flow control but assumes a reliable connection. The session layer makes the connection reliable across links, so yamux never notices. yamux must therefore be configured never to give up (`tunnel.YamuxConfig`).
- **Scheduling below yamux, not inside it.** yamux has no priorities. Parsing its 12-byte headers keeps yamux unmodified while allowing per-stream queues, chunking and RST injection.
- **Limit in-flight bytes, not just prioritize.** Without a limit, ssh and network buffers hold seconds of bulk data that no priority can overtake. BBR-style estimation keeps the link full with a small queue, across a wide range of links.
- **flock for single instances.** It is released automatically on crash.
- **HTTP over a unix socket for control.** Easy to debug with curl, permissioned by the file mode.
- **Everything binds 127.0.0.1 by default.** The tunnel must not be usable from the LAN.

## 8. Known limitations

- The session lives in process memory. A client crash or an agentd restart loses open connections; listeners come back with the restarted process.
- systemd --user installation is covered only by unit-file tests and has not been run on a real systemd host. The cron fallback and launchd have been run.
- No access control on dial targets: anyone who can use the ssh account can reach what the destination can reach (the same as plain ssh port forwarding).
- One destination per `GOTUNNEL_HOME`. Run separate instances with different `GOTUNNEL_HOME` values.
- Unix only (flock, setsid, process groups). Linux agent payloads exist for amd64 and arm64.
