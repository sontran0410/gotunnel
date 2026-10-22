# Maintaining gotunnel

This is the working manual for changing the code: the rules that must hold, how to verify a change, step-by-step recipes for common tasks, the test inventory, and how to debug. Read [architecture.md](architecture.md) for how the parts fit together and [protocol.md](protocol.md) for wire formats.

## 1. Rules that must always hold

Every rule below exists because breaking it causes a failure that is hard to notice: data corruption, hangs, or tunnels that die during outages. If a change seems to require breaking one, stop and redesign.

| # | Rule | Why | Where it is enforced or tested |
|---|---|---|---|
| R1 | yamux must never give up while the link is down. Keep `StreamOpenTimeout = 0`, `EnableKeepAlive = false` (with `KeepAliveInterval > 0` for `VerifyConfig`), and `ConnectionWriteTimeout` and `StreamCloseTimeout` ≥ the session TTL | a yamux timeout closes the whole session; the outage would then kill every connection, which is exactly what resume prevents | `tunnel.YamuxConfig`; `TestConnectionsSurviveLinkFailures`, `TestStackSurvivesLinkFailures` |
| R2 | `sched.Conn.Write` must only parse and queue, never block on I/O | yamux has a single send loop; one blocked write would stall every stream, including interactive ones | `internal/sched/sched.go` |
| R3 | The scheduler may reorder frames only *between* streams. Inside a stream the order is fixed; only stream-0 frames jump the queue | FIN/RST overtaking data truncates streams; data overtaking SYN is a protocol error | `TestParseSplitAndPreserveStreamOrder`, `TestResetQueuedAfterPendingFrames` |
| R4 | The session ACKs only bytes already stored in its receive buffer. On attach it resends from the peer's offset. `Resume` writes WELCOME *before* attaching, under the session lock | anything else loses or duplicates bytes after a reconnect | `session.go`; `TestTransferSurvivesLinkFailures` (~140 forced failures, checksum verified), `TestResumeReportsOffsetBeforeAttach` |
| R5 | Never hold `Session.mu` during link I/O. The only exception is the `welcome` callback inside `Resume` | deadlocks between `Read`/`Write` and the link goroutines | `session.go` |
| R6 | On the agent, stdout is the protocol. The relay writes the preamble, then only bytes copied from agentd. Logs go to stderr | any stray byte corrupts the stream | `agent/run.go` (`RunRelay`), `cmd/gotunnel/agentcmd.go` |
| R7 | Keep the ssh options in `transport.SSH.Args` (`-T -e none`, `BatchMode=yes`, `ControlPath=none`, `ClearAllForwardings=yes`, `RemoteCommand=none`) | each one prevents a real failure; see the table in architecture.md §3.1 | `internal/transport/ssh.go` |
| R8 | `sshLink.Close` closes stdin first, waits, then signals the process group | BYE must reach the agent; ProxyCommand children must not leak | `transport/ssh.go`; e2e asserts that agentd drops the session after `down` |
| R9 | Wire changes must be additive JSON fields whose zero value keeps the old behaviour, or new ops that old peers can refuse. Anything else bumps `version.Proto` | old agents keep running on destinations until someone runs `install-agent` | [protocol.md §8](protocol.md#8-compatibility-rules) |
| R10 | Single instance via flock; remove a socket file only while holding the matching lock | two daemons on one socket corrupt each other | `daemon.AcquireLock`, `agent.Run`, `cmdUp`/`runClient` |
| R11 | Every on-disk location comes from `internal/paths`. Unix socket paths must stay under ~100 bytes. Tests isolate themselves with `GOTUNNEL_HOME` and a short `GOTUNNEL_RUNTIME_DIR` | long paths fail with "invalid argument"; tests must not touch the real `~/.gotunnel` | `paths.RunDir`, e2e `newEnv` |
| R12 | Defaults bind to `127.0.0.1`; never log tokens; the agent socket directory stays 0700 | the tunnel acts with the user's ssh identity | config defaults, `paths.EnsureDir` |
| R13 | No new dependencies without strong reason (today: yamux and yaml) | every dependency must be cross-compiled into the agent and maintained | `go.mod` |

## 2. Workflow

1. **Locate** the code with the map below, then read the package doc comment and the functions you will touch.
2. **Change** the code, matching the surrounding style: comments explain *why*, user-facing errors say what to do.
3. Run **`make check`** (gofmt check, vet, staticcheck, all tests except the latency benchmark). Then run whatever else the verification matrix asks for.
4. **Update the docs** listed for your kind of change (section 4).
5. **Commit** small logical units with an imperative subject (`Add …`, `Fix …`) and a body explaining why.

| Concern | Start here |
|---|---|
| config keys, defaults, validation | `internal/config/config.go` |
| CLI commands, output formatting | `cmd/gotunnel/*.go` (`main.go` dispatches) |
| when the transport goes up or down, reconnect, resume | `internal/client/supervisor.go` |
| forwards, SOCKS, reverse, reload | `internal/client/client.go`, `socks.go` |
| ssh invocation, link close | `internal/transport/ssh.go` |
| byte stream, ACK, replay, ping, dead links | `internal/session/session.go` |
| priorities, chunking, RST, in-flight limit | `internal/sched/sched.go`, `controller.go` |
| stack assembly, yamux config, stream request/proxy helpers | `internal/tunnel/tunnel.go` |
| agentd sessions, dialing, reverse listeners, admin | `internal/agent/server.go` |
| relay, agentd start/stop, auto-start, upgrade | `internal/agent/run.go` |
| wire structs and framing | `internal/proto/` |
| locks, daemonize, log files | `internal/daemon/` |
| launchd, systemd, cron | `internal/service/service.go` |
| control API (CLI ↔ daemon) | `internal/control/control.go` |
| embedded agent binaries | `internal/agentbin/`, `Makefile` |

## 3. Verification matrix

`make check` is always required. Add the following depending on what you touched:

| You changed | Also run |
|---|---|
| `session`, `sched`, `tunnel`, `proto` | `go test -race -count=3 ./internal/session/ ./internal/sched/ ./internal/client/` and `make test` (includes the latency benchmark; compare its log lines with the table in architecture.md §3.3) |
| `client`, `agent` | `make e2e` |
| `cmd/gotunnel`, `control`, `daemon`, `service` | `make e2e`; for `service`, also a manual `service install` / `status` / `down` / `uninstall` cycle on a Mac |
| `transport/ssh.go`, `install-agent`, `doctor` | `make e2e` and `make lab-test` (Docker; on this Mac start it with `colima start` and stop it afterwards) |
| controller constants | `go test -v -run 'TestControllerTracksBDP' ./internal/sched/` and `go test -v -run TestInteractiveLatency ./internal/client/` |

Notes:
- `make lint` runs staticcheck through `go run` at a pinned version. The first run downloads it and needs network access.
- Tests with timing (latency benchmark, dead-link detection, idle close) use generous thresholds. If one fails once under heavy load, rerun it alone before suspecting the code; if it fails twice, it is real.

## 4. Recipes

### 4.1 Add a configuration option
1. Add the field with a yaml tag to the right struct in `internal/config/config.go`. Use `Duration`/`ByteSize` for durations and sizes, and a pointer (`*bool`) when false must differ from "unset".
2. Set the default in `ApplyDefaults` (or the `DefaultForward`/`DefaultSocks`/`DefaultReverse` helpers) and validate it in `Validate` (or `ValidateForward`/…). Validation appends to `errs`, so every problem is reported at once.
3. Use it:
   - session parameters go through `client.Params` → `tunnel.Params` → `tunnel.NewStack`;
   - parameters the agent needs also go into `proto.Hello` (recipe 4.2);
   - CLI-only options go in `cmd/gotunnel`.
4. If `Apply` (reload) cannot apply it live, make sure it belongs to the ssh/connection/daemon sections, which already warn "restart required"; otherwise extend `Apply`.
5. Add a test case to `internal/config/config_test.go`.
6. Add the key to `examples/config.yaml` (with a comment) and to the table in `docs/operations.md`.

### 4.2 Send a new parameter to the agent
1. Add a field to `proto.Hello` whose zero value means "old behaviour" (rule R9).
2. Fill it in `Supervisor.hello()`.
3. Read it, and clamp it, in `agent.Server.newSession`.
4. Document it in `docs/protocol.md` §2.

### 4.3 Add a CLI command
1. Add a `case` to the switch in `cmd/gotunnel/main.go` and a line to `usage`.
2. Implement `cmdX(args []string) error` in the relevant file, using `newFlags("x")` (or `parseNamed` for `NAME [flags]`) and `configFlag(fs)` if it needs the config.
3. Return errors; `main` prints them. Return `exitCode(n)` to exit silently with a specific code.
4. If it must talk to the running daemon, add a control endpoint (4.4).
5. Add it to `docs/operations.md` (command reference) and, if user-visible, `README.md`. Extend `test/e2e/e2e_test.go` if it changes daemon behaviour.

### 4.4 Add a control API endpoint
1. Add the method to the `control.API` interface and a route in `control.Handler`.
2. Implement it on `daemonAPI` in `cmd/gotunnel/clientcmd.go`, delegating to a `client.Client` method.
3. Call it from the CLI with `controlClient().Do(ctx, method, path, body, &out)`.
4. Document it in `docs/protocol.md` §7.

### 4.5 Add a stream operation (new `proto.Op…`)
1. Add the constant (and any `Request` fields) in `internal/proto/messages.go`.
2. Handle it in `agentSession.handleStream` (agent) or `Client.handleIncoming` (client, for agent-opened streams). Always answer with `tunnel.Reply` and call `Sched.Forget` (success) or `Sched.Reset` (failure) for the stream.
3. Send it with `stack.Request(ctx, req, class)`.
4. Old peers answer unknown ops with an error, so handle that gracefully (R9). Document it in `docs/protocol.md` §4.1.

### 4.6 Make an incompatible wire change
1. Increment `version.Proto` in `internal/version/version.go`.
2. Make the change on both sides in the same commit.
3. Update `docs/protocol.md`.
4. Users must run `gotunnel install-agent`. Agents with the old version reject new clients with `proto`, and the error message already says so.

### 4.7 Add a priority class
1. Add `config.PrioX` and accept it in `config.ValidPriority`.
2. Add `sched.ClassX`, its quantum in the `quantum` table and the case in `sched.ParseClass`.
3. Document it in `docs/operations.md` and `examples/config.yaml`.

### 4.8 Tune the in-flight controller
- The constants are in `internal/sched/controller.go` (`steadyGain`, `startupGain`, `probeRTTInterval`) and `internal/tunnel/tunnel.go` (`minInflight`, the 8 KiB headroom). DATA frame and ACK sizes are in `session.Options.defaults`; the chunk size is `sched.DefaultChunkSize`.
- Higher gain or headroom raises throughput and latency together; lower values do the reverse. Measure with the latency benchmark for both link profiles. Keep the median under the test thresholds (500ms and 2s) and the bulk share at 70% or more, then update the numbers in architecture.md §3.3.

### 4.9 Change how ssh is run
- Edit `transport.SSH.Args`. `install-agent` and `doctor` reuse it (they replace `RemoteCommand`), so they follow automatically.
- Keep rule R7. Test with `make e2e` (fake ssh) and `make lab-test` (real OpenSSH with ProxyJump and a TLS ProxyCommand).

### 4.10 Change service installation
- Everything is in `internal/service/service.go`. The callers are `cmdService` and `cmdAgentService` in `cmd/gotunnel/agentcmd.go`.
- launchd: `KeepAlive.SuccessfulExit=false` is what lets `gotunnel down` stop a managed client. Keep the PATH recording.
- Unit tests cover file generation (`service_test.go`). Installing for real changes the user's machine, so uninstall after testing.

### 4.11 Change the embedded agent build
- `make payload` builds `dist/gotunnel-linux-{amd64,arm64}` (static, `-trimpath -s -w`) and gzips them into `internal/agentbin/payload/`. `make build` embeds them with `-tags embedagent`.
- To add an architecture, extend `AGENT_ARCHS` in the `Makefile` and `parsePlatform` in `cmd/gotunnel/installagent.go`.

### 4.12 Write an in-process integration test
The fake agent in `internal/client/client_test.go` serves links from an in-process agentd over `net.Pipe`, or over `netutil.SlowLink` for slow-link tests:

```go
func TestSomething(t *testing.T) {
	fa := newFakeAgent(t)          // in-process agentd; fa.killLinks(), fa.restart(graceful)
	echo := startEcho(t)           // TCP echo server with half-close
	listen := freePort(t)
	cfg := baseConfig()            // short ping/dead timers
	cfg.Forwards = []config.Forward{{Name: "x", Listen: listen, Target: echo, Mode: config.ModeAlways}}
	c := startClient(t, cfg, fa)   // Start + Run, stopped at cleanup
	waitFor(t, "connected", func() bool { return c.Status().Transport.State == client.StateConnected })
	echoRoundTrip(t, listen, []byte("hello"))
}
```

To simulate a slow path, set `fa.pipe = func() (net.Conn, net.Conn) { sl := netutil.NewSlowLink(rate, oneWay); return sl.A, sl.B }`.

## 5. Test inventory

| Test | Covers |
|---|---|
| `internal/config/config_test.go` | parsing, defaults, aggregated validation errors, unknown keys, sizes, the example config |
| `internal/proto/proto_test.go` | framing, truncation, preamble skipping, request messages |
| `internal/session/session_test.go` | ring buffer (randomized against a reference), transfer, ~140 forced link failures mid-frame in both directions, BYE, dead-link detection, backpressure while detached, bad offsets, `Resume` ordering |
| `internal/sched/sched_test.go` | frame parsing/splitting/order, interactive frames jumping a bulk queue, weighted share, RST injection, controller (BDP tracking, growth, RTT probe), yamux over sched over session under link failures |
| `internal/service/service_test.go` | plist, systemd unit and crontab generation |
| `internal/client/client_test.go` | client + agentd in one process: on-demand dial and idle close, a 4 MiB transfer across ~30 resumes, agent restart (graceful and crash), SOCKS (including refused), reverse forwards across link loss, runtime forward changes, dial failure |
| `internal/client/latency_test.go` | keystroke latency and throughput during a bulk download over two simulated slow links, with and without in-flight control (skipped with `-short`) |
| `test/e2e/e2e_test.go` | the real binary with a fake ssh: install-agent (with shell noise), doctor, `up -d`, a second `up` refused, resume after `reconnect`, SOCKS, reverse, runtime forward add/rm, reload, agent restart → new session, `down` → agent drops the session |
| `test/lab/lab.sh test` | real OpenSSH: TLS hop through `openssl s_client`, ProxyJump through toxiproxy; download across a TCP reset, an ssh session across a 35s blackhole, keystroke latency at 512 kbit/s |

Helpers:
- `netutil.FlakyConn` breaks a link after N bytes.
- `netutil.SlowLink` is an in-memory link with bandwidth, latency and an unbounded buffer, like ssh.
- `fakeAgent`, `startEcho` and `freePort` live in the client tests; the e2e package has its own `fakeSSH` script and env helpers.

Lab specifics (`test/lab`):
- sshd settings live in `/etc/ssh/sshd_config.d/lab.conf`, because sshd uses the first value it reads and Alpine's main file says `AllowTcpForwarding no`.
- `PerSourcePenalties no` is needed because all traffic to jump1 arrives from 127.0.0.1 through socat.
- `known_hosts` is reset on every `up`, because host keys are regenerated with the image.
- `cronie` provides a setuid `crontab` for the cron fallback.

## 6. Debugging

- **Client log** (`gotunnel logs -f`; set `log_level: debug` and restart for more):

  | Message | Meaning |
  |---|---|
  | `connect failed err=… retry_in=…` | ssh or the handshake failed; the error includes ssh's diagnosis |
  | `connected took=…` | how long setting up the path took |
  | `link lost; resuming` | the link died with the session intact |
  | `session resumed` | the reconnect kept every connection |
  | `session could not be resumed; open connections are lost` | the agent forgot the session |
  | `idle: closing session` | idle timeout reached |
  | `ssh: …` | ssh's own stderr, including relay errors |

- **Agent log** (`~/.gotunnel/logs/agentd.log` on the destination): `session created/resumed/closed reason=…`, `resume with wrong token`, `no sessions, exiting`, `dial failed` (debug).
- **State.** `gotunnel status --json` on the client; `gotunnel agent status --json` on the destination.
- **Raw control API.** `curl --unix-socket ~/.gotunnel/run/ctl.sock http://x/status`.
- **Hangs.** `kill -QUIT <pid>` dumps every goroutine to `client.out` / `agentd.out`; the process exits. Look for goroutines blocked in `Session.Write` (send limit reached: is a link attached?), `sched.(*Conn).pump`, or yamux `waitForSendErr`.
- **Failure signatures in tests:**
  - "data mismatch / corrupted" points to offset or ACK logic (R4);
  - a test that hangs points to lock order (R5) or a blocking write (R2);
  - "only N link failures" means the test stopped exercising resume (it ran too fast). Make the test slower; do not loosen the assertion.

## 7. Build and versions

- `version.Version` comes from `git describe --tags --always --dirty` through `-ldflags` in the `Makefile`. Client and agent compare it only to warn. `version.Proto` is the compatibility number (R9).
- `make build` produces `bin/gotunnel` for this machine (about 15 MB including two gzipped Linux agents). `make agent` produces static Linux binaries in `dist/`.
- `go build ./cmd/gotunnel` also works but cannot `install-agent` to Linux (no payload). It can install to a destination with the same OS and architecture, using its own binary.

## 8. Known gaps and ideas

- A native `x/crypto/ssh` transport behind the `transport.Transport` interface would remove the dependency on the system ssh and openssl.
- Persisting runtime `forward add` changes to the config file.
- A target allowlist on the agent.
- Verifying `agent service install` on a real systemd host (only the cron fallback and launchd have been run for real).
