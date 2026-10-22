# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`gotunnel` (module `github.com/sontran0410/gotunnel`) is a single Go binary that keeps many TCP port forwards alive between a client machine and a destination that is reachable only through slow ssh jump hosts. It runs `ssh <alias> 'gotunnel agent stdio'` once and multiplexes every forward over it (yamux). When the link breaks, it resumes the session so open connections survive. Nothing is installed on the jump hosts; the same binary runs on the destination as the agent.

## Read before changing code

1. [docs/maintaining.md](docs/maintaining.md): **rules R1–R13**, the verification matrix, recipes for common changes, the test inventory and debugging. Required reading.
2. [docs/architecture.md](docs/architecture.md): processes, layers, flows, the concurrency model and timeouts.
3. [docs/protocol.md](docs/protocol.md): wire formats and compatibility rules (when to bump `version.Proto`).
4. [docs/operations.md](docs/operations.md): config keys, CLI, services, status fields, troubleshooting.

## Commands

- `make check` — gofmt check, vet, staticcheck (pinned, run through `go run`) and all tests except the slow latency benchmark. **Run it before every commit.**
- `make test` — every test with `-race`, including the ~50s latency benchmark.
- `go test -race -run TestName ./internal/client/` — runs a single test.
- `make e2e` — the real binary with a fake ssh: install-agent, daemon, control API, relay, auto-started agentd.
- `make build` — `bin/gotunnel` with the Linux agents embedded. `go build ./cmd/gotunnel` and `go install github.com/sontran0410/gotunnel/cmd/gotunnel@latest` build without them, and `install-agent` then cross-compiles the agent with the local Go toolchain.
- `make dist` — release binaries for linux/darwin × amd64/arm64 plus `SHA256SUMS` in `dist/`. Run it on a clean tagged commit so the binaries report that version.
- `GOTOOLCHAIN=go1.25.0 go test -short ./...` — checks the oldest supported Go (the `go` directive is 1.25; 1.25.0, 1.26.5 and 1.27.3 are tested).
- `make lab-test` — Docker lab with real sshd, a TLS ProxyCommand hop and toxiproxy faults.
  - Needs Docker; on this Mac that is `colima start`. Stop Colima again when done.
  - Remove the lab with `make lab-down`.

## Rules you must not break (details and reasons in docs/maintaining.md §1)

- yamux must never time out while the link is down: keep `tunnel.YamuxConfig` as it is (`StreamOpenTimeout=0`, keepalive off, timeouts ≥ TTL).
- `sched.Conn.Write` only queues and never blocks. The scheduler never reorders frames within a stream.
- The session ACKs only stored bytes. `Session.Resume` writes WELCOME before attaching. Never hold `Session.mu` during link I/O.
- Agent stdout is the protocol channel; the relay logs only to stderr.
- Keep the ssh options in `transport.SSH.Args`. `sshLink.Close` closes stdin before killing, so BYE gets flushed.
- Wire changes are additive (zero value means old behaviour); anything else bumps `version.Proto`.
- Paths come from `internal/paths`. Tests set `GOTUNNEL_HOME` and a short `GOTUNNEL_RUNTIME_DIR` (unix socket path limit).
- Listeners default to `127.0.0.1`; never log session tokens; no new dependencies without strong reason.

## Where things are

| Concern | File |
|---|---|
| transport state machine (dial, resume, idle) | `internal/client/supervisor.go` |
| forwards, SOCKS, reverse forwards, reload | `internal/client/client.go`, `socks.go` |
| resumable byte stream (offsets, ACK, ping) | `internal/session/session.go` |
| priorities, chunking, RST injection, in-flight control | `internal/sched/sched.go`, `controller.go` |
| stack assembly, yamux config, request/proxy helpers | `internal/tunnel/tunnel.go` |
| agentd (sessions, dialing, reverse listeners, admin) | `internal/agent/server.go`; relay and lifecycle in `run.go` |
| ssh process | `internal/transport/ssh.go` |
| config keys | `internal/config/config.go`; example in `examples/config.yaml` |
| CLI | `cmd/gotunnel/` (`main.go` dispatches; `doctor.go`, `installagent.go`, `agentcmd.go`, `clientcmd.go`) |
| daemonize, locks, logs / services / control API | `internal/daemon/`, `internal/service/`, `internal/control/` |

When you change behaviour, update the doc that describes it (each recipe in docs/maintaining.md §4 lists which one) and `examples/config.yaml` for config changes.
