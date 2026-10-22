# gotunnel

Persistent port forwards over a slow, multi-hop ssh path.

gotunnel is for a destination machine that you can only reach through a chain of jump hosts. One of those hops may need a `ProxyCommand` such as `openssl s_client -quiet -connect …`. The path is slow to set up and has little bandwidth, and you cannot install anything on the jump hosts.

gotunnel runs your usual `ssh <alias>` **once**, keeps it alive, and carries every forward over it:

- **Many forwards, one ssh connection.** A new connection costs about one round trip, with no new ssh handshake or authentication.
- **Connections survive network trouble.** If the path breaks, gotunnel reconnects and resumes the session. Open connections (an ssh session, a database client) just pause and continue.
- **Always-on and on-demand forwards.** On-demand forwards bring the transport up on first use and let it close when idle.
- **SOCKS5 proxy.** Reaches any host:port on the destination side; names are resolved there.
- **Reverse forwards.** The destination can reach services on your machine.
- **Bandwidth priorities.** Keystrokes get ahead of bulk transfers, and gotunnel avoids filling ssh's buffers, so interactive sessions stay responsive during big downloads.
- **Runs as a daemon on both ends.** Uses launchd, systemd --user or cron, and restarts after crashes.

## How it works

```
your machine                                                    destination
┌────────────────────────┐                                ┌──────────────────────────┐
│ apps → 127.0.0.1:port  │                                │ agentd (daemon)          │
│ gotunnel client daemon │  ssh <alias> 'gotunnel agent   │  keeps sessions alive,   │
│  forwards/socks/reverse│  stdio' (once, kept alive)     │  connects to targets     │
│  yamux · scheduler ·   │ ═════ jump1 ═ jump2 ═ … ══════▶│        ▲ unix socket     │
│  resumable session     │      (nothing installed        │ relay (one per ssh link) │
└────────────────────────┘       on the jump hosts)       └──────────────────────────┘
```

Every byte of the session is numbered, and both sides keep unacknowledged bytes. When a link dies, the client opens a new one. Each side then resends from the offset the other side reports, and the applications above never notice. See [docs/architecture.md](docs/architecture.md).

## Requirements

**Client (your machine)**
- macOS or Linux, and Go 1.25 or newer to install (tested with 1.25.0, 1.26.5 and 1.27.3).
- The OpenSSH client.
- A single `ssh <alias>` must reach the destination (`ProxyJump`/`ProxyCommand` in `~/.ssh/config`) without typing anything. Use keys loaded in ssh-agent or the macOS keychain; a daemon cannot type passphrases.
- If you have never connected before, connect once by hand to accept the host keys.

**Destination**
- Linux on amd64 or arm64.
- `sh`, `gunzip` and `head` (present on any normal system).
- No root needed.

## Install

```sh
go install github.com/sontran0410/gotunnel/cmd/gotunnel@latest
```

This puts `gotunnel` in `$(go env GOPATH)/bin` (or `$GOBIN`). Binaries installed this way do not embed the Linux agent. When `gotunnel install-agent` needs one, it builds the same version for the destination with your Go toolchain, so `go` must be on your PATH and able to download modules.

Alternatively, build from a checkout with `make build`. That produces `bin/gotunnel` with the Linux amd64/arm64 agents embedded, so no Go toolchain is needed afterwards.

## Quick start

```sh
mkdir -p ~/.config/gotunnel
curl -fsSL https://raw.githubusercontent.com/sontran0410/gotunnel/main/examples/config.yaml \
  -o ~/.config/gotunnel/config.yaml            # or copy examples/config.yaml from a checkout
$EDITOR ~/.config/gotunnel/config.yaml         # set ssh.host and your forwards

gotunnel install-agent    # uploads the agent through ssh (one ssh connection, ~3 MB)
gotunnel doctor           # checks every piece and explains problems
gotunnel service install  # starts at login, restarts after crashes
gotunnel status
```

Use `gotunnel up -d` instead of `service install` to start it by hand. `gotunnel down` stops it.

## Everyday commands

| Command | What it does |
|---|---|
| `gotunnel status` | Shows the transport state, RTT, in-flight bytes, link losses/resumes and every forward with its counters |
| `gotunnel logs -f` | Follows the client log |
| `gotunnel forward add NAME --listen 127.0.0.1:5432 --target db:5432` | Adds a forward to the running daemon (not saved to the file) |
| `gotunnel forward rm NAME` | Removes it; open connections continue |
| `gotunnel reload` | Applies changes made to the config file |
| `gotunnel reconnect` | Drops the link now; the session resumes on a new one |
| `gotunnel doctor` | Diagnoses the configuration, ssh route, ports, service and agent |

On the destination, the agent starts automatically when the client connects. To keep it as a service, run `~/.gotunnel/bin/gotunnel agent service install`. To look at it, run `~/.gotunnel/bin/gotunnel agent status`.

## Documentation

- [docs/operations.md](docs/operations.md): configuration reference, CLI reference, services, files, status fields, troubleshooting.
- [docs/architecture.md](docs/architecture.md): components, data flows, concurrency, timeouts, design decisions.
- [docs/protocol.md](docs/protocol.md): wire formats and compatibility rules.
- [docs/maintaining.md](docs/maintaining.md): rules, recipes and the testing guide for changing the code.

## Development

```sh
make check   # lint + all fast tests (run before every commit)
make test    # everything, including a ~50s latency benchmark
make e2e     # real binary end to end, with a fake ssh
make lab-test  # Docker lab: real sshd, TLS hop, network faults (needs Docker)
```
