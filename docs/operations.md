# Operations

This guide is for running gotunnel: configuration, commands, services, files, status output and troubleshooting.

## Configuration reference

The file is `~/.config/gotunnel/config.yaml` (or `$XDG_CONFIG_HOME/gotunnel/config.yaml`); pass `-c` to use another. Unknown keys are errors, and all problems are reported at once. A commented example is in [examples/config.yaml](../examples/config.yaml). Durations use Go syntax (`45s`, `5m`, `1h30m`). Sizes accept `B`, `K`/`KB`/`KiB`, `M`/`MB`/`MiB`, `G`… — all of them binary multiples.

### `ssh`

| Key | Default | Meaning |
|---|---|---|
| `host` | required | ssh alias that reaches the destination in one command (`ProxyJump`/`ProxyCommand` in `~/.ssh/config`) |
| `ssh_command` | `ssh` | ssh binary |
| `extra_args` | `[]` | options inserted before the host, e.g. `["-F", "/path/ssh_config"]` |
| `compression` | `true` | `ssh -C`; helps text on slow links, useless for already compressed or encrypted data |
| `remote_command` | `~/.gotunnel/bin/gotunnel agent stdio` | starts the relay remotely. Its first word is also where `install-agent` installs the binary |

### `connection`

| Key | Default | Meaning |
|---|---|---|
| `connect_timeout` | `3m` | time allowed for ssh through the jump chain plus the handshake |
| `ping_interval` | `10s` | ping frequency while connected |
| `dead_after` | `45s` | no data for this long means the link is dead and gotunnel reconnects. Must be greater than `ping_interval` |
| `idle_timeout` | `15m` | close the transport after this long without activity. Only applies when there are no `always` forwards and no reverse forwards |
| `session_ttl` | `30m` | how long agentd keeps a detached session, including its open connections. Minimum 1m; the agent clamps it to 24h |
| `replay_buffer` | `8MiB` | maximum bytes in flight (and kept for replay). Minimum 256KiB; the agent clamps it to 64MiB |
| `max_inflight` | `64KiB` | starting in-flight limit; must lie between 16KiB and `replay_buffer` |
| `adaptive_inflight` | `true` | tune the in-flight limit from RTT and bandwidth. With `false`, `max_inflight` stays fixed |

### `daemon`

| Key | Default | Meaning |
|---|---|---|
| `log_file` | `~/.gotunnel/logs/client.log` | rotating log (10 MiB × 3 backups) |
| `log_level` | `info` | `debug`, `info`, `warn`, `error` |

### `forwards[]`

| Key | Default | Meaning |
|---|---|---|
| `name` | `forward-<listen port>` | unique name, used by `forward rm` and in status |
| `listen` | required | local `host:port`. Use `127.0.0.1:port`; an empty host listens on every interface |
| `target` | required | `host:port` as seen from the destination (can be another machine in its network) |
| `mode` | `on-demand` | `always` keeps the transport up permanently; `on-demand` brings it up on first use |
| `priority` | `normal` | `interactive`, `normal` or `bulk` (bandwidth scheduling) |

### `socks[]`

| Key | Default | Meaning |
|---|---|---|
| `name` | `socks-<listen port>` | unique name |
| `listen` | required | local `host:port` of a SOCKS5 proxy (no authentication; CONNECT only; names resolved on the destination) |
| `priority` | `normal` | priority of its connections |

### `reverse[]`

| Key | Default | Meaning |
|---|---|---|
| `name` | `reverse-<remote port>` | unique name |
| `remote_listen` | required | `host:port` to listen on at the destination |
| `target` | required | `host:port` reachable from the client machine |
| `priority` | `normal` | priority of its connections |

Any reverse forward keeps the transport up permanently.

## Command reference

Every command accepts `-h`. Exit codes: 0 ok, 1 error, 2 usage, 3 "not running" (`status`, `agent status`).

### Client machine

| Command | Notes |
|---|---|
| `gotunnel up [-c file] [-d]` | Runs the client. `-d` starts it in the background and waits until it answers. Without `-d` it runs in the foreground; that is what launchd and systemd use |
| `gotunnel down` | Stops the daemon (`POST /shutdown`, or SIGTERM to the lock holder) and waits up to 15s |
| `gotunnel status [--json]` | See [status fields](#status-fields) |
| `gotunnel logs [-c file] [-f] [-n 50]` | Prints and follows the client log |
| `gotunnel reload` | Re-reads the config file. Forwards, SOCKS and reverse forwards are reconciled by name; ssh/connection/daemon changes need a restart (a warning says so). SIGHUP does the same |
| `gotunnel reconnect` | Drops the link; the session resumes on a new one |
| `gotunnel forward add NAME --listen A --target B [--mode M] [--priority P]` | Runtime only, not saved |
| `gotunnel forward rm NAME` / `forward ls` | |
| `gotunnel reverse add NAME --remote-listen A --target B [--priority P]` / `reverse rm NAME` | Runtime only |
| `gotunnel service install\|uninstall\|status [-c file]` | launchd (macOS) or systemd --user (Linux), with crontab `@reboot` as a fallback. `install` stops a manually started client first |
| `gotunnel install-agent [-c file] [--binary file]` | Uploads the agent through ssh in one round trip. The binary is taken from the first available source: `--binary`; the Linux agent embedded by `make build`; this binary itself when the destination has the same OS and architecture; or a build of the same module version made with the local Go toolchain (binaries from `go install` embed no agent) |
| `gotunnel doctor [-c file]` | Checks the config, ssh, the resolved jump route (`ssh -G`), ProxyCommand programs, the daemon, listen ports, the service, and the agent version over ssh |
| `gotunnel version` | |

### Destination (binary at `~/.gotunnel/bin/gotunnel`)

| Command | Notes |
|---|---|
| `gotunnel agent status [--json]` | agentd PID, how it was started (`auto`, `manual`, `cron`, `systemd`), idle exit, sessions |
| `gotunnel agent start` / `stop` | Starts agentd in the background with no idle exit (`manual`), or stops it. Stopping ends all sessions with BYE |
| `gotunnel agent logs [-f] [-n 50]` | `~/.gotunnel/logs/agentd.log` |
| `gotunnel agent service install\|uninstall\|status` | systemd --user unit with linger, or crontab `@reboot`. Not needed for normal use, because the relay starts agentd on demand (`auto`, which exits after 1h without sessions) |
| `gotunnel agent run [--idle-exit 0s] [--managed M] [--log-level L]` | Foreground agentd, for service managers |
| `gotunnel agent stdio [--standalone]` | The relay; ssh runs it. `--standalone` serves the link in-process (no resume; for debugging) |

## Running in the background

| | Client (macOS) | Agent (Linux) |
|---|---|---|
| One-off | `gotunnel up -d` / `gotunnel down` | nothing to do: started on demand; or `agent start` / `agent stop` |
| At login/boot, restart on crash | `gotunnel service install`: launchd `~/Library/LaunchAgents/local.gotunnel.client.plist` | `agent service install`: `~/.config/systemd/user/gotunnel-agent.service` + `loginctl enable-linger`, or a crontab `@reboot` line |
| After `gotunnel down` | launchd does not restart it (clean exit); it starts again at the next login | — |
| Logs | `~/.gotunnel/logs/client.log` (+ `client.out` for crash output) | `~/.gotunnel/logs/agentd.log` (+ `agentd.out`) |

launchd and systemd start services with a minimal PATH. `service install` records your current PATH, so ProxyCommands such as Homebrew's `openssl` keep working. If you install new tools later, run `service install` again.

## Files and environment

| Path | Content |
|---|---|
| `~/.config/gotunnel/config.yaml` | client configuration |
| `$GOTUNNEL_HOME` (default `~/.gotunnel`) | all state |
| `…/run/` (mode 0700) | `ctl.sock`, `client.lock`, `agentd.sock`, `agentd.lock` |
| `…/logs/` | `client.log*`, `client.out`, `agentd.log*`, `agentd.out` |
| `…/bin/gotunnel` | agent binary on the destination (installed by `install-agent`) |

| Variable | Effect |
|---|---|
| `GOTUNNEL_HOME` | state directory (use separate values to run several independent instances) |
| `GOTUNNEL_RUNTIME_DIR` | overrides `run/`. Used when socket paths would exceed ~100 bytes; gotunnel then falls back to `/tmp/gotunnel-<uid>` by itself |
| `XDG_CONFIG_HOME` | location of the default config file |

## Status fields

`gotunnel status` (and `--json`):

| Field | Meaning |
|---|---|
| `transport.state` | `idle` (no session wanted), `connecting`, `connected`, `reconnecting` (link lost, session kept), `stopped` |
| `since` | time of the last state change |
| `last_error` | why the last connection attempt failed (includes ssh's error output) |
| `session` | short ID of the current session |
| `rtt_ms` / `min_rtt_ms` | latest and smallest round-trip times measured on acknowledged data and pings |
| `inflight` / `inflight_limit` | unacknowledged bytes and the current limit chosen by the controller |
| `queued` | bytes waiting in the scheduler (not yet allowed onto the link) |
| `bytes_in` / `bytes_out` | session payload received and sent |
| `link_losses` | links that died (each followed by a reconnect) |
| `resumes` | reconnects that resumed the session (no connection lost) |
| `sessions` | sessions created. A number greater than 1 means connections were lost at least once (agent restart or TTL expiry) |
| forwards/socks `active`, `total`, `bytes_up`, `bytes_down` | current and total connections; bytes from this machine (`up`) and towards it (`down`) |
| `listening` / `error` | whether the local port is bound; bind errors are retried every 30s |
| reverse `state` | `pending`, `listening` or `error` (with `error`) |

## Troubleshooting

Run `gotunnel doctor` first. It checks every item below and prints a hint.

| Symptom or message | Cause | Fix |
|---|---|---|
| `Permission denied (publickey)` | key not available without a passphrase | `ssh-add` the key (on macOS: `ssh-add --apple-use-keychain`), or set `IdentityFile` |
| `Host key verification failed` | first connection to a host, or the host key changed | `ssh <alias>` once by hand and accept or fix the key |
| `no answer from agent within 3m0s` | a hop hangs or is very slow | try `ssh -v <alias>`; increase `connect_timeout` if the path is just slow |
| `agent rejected handshake: proto` | agent and client protocol versions differ | `gotunnel install-agent` |
| `agent version differs from client` (warning) | the agent is an older build | `gotunnel install-agent`; the idle agentd restarts by itself |
| `ssh: … not found` (exit 127) / doctor: `not installed on the destination` | no agent binary at `remote_command` | `gotunnel install-agent` |
| `session could not be resumed; open connections are lost` | the outage was longer than `session_ttl`, or agentd restarted | expected; raise `session_ttl` for longer outages |
| `gotunnel is already running (pid N)` | another instance holds the lock | `gotunnel down`, or `gotunnel status` to see who runs it |
| forward `! listen tcp …: address already in use (retrying)` | another program owns the port | free the port or change `listen`; gotunnel retries every 30s |
| works by hand, fails as a service (`openssl: not found` in `client.out`) | the service has a minimal PATH | `gotunnel service install` again from a shell with the right PATH |
| agentd killed at logout (Linux) | systemd `KillUserProcesses=yes` | `gotunnel agent service install` (enables linger), or ask an admin for `loginctl enable-linger <user>` |
| interactive lag during transfers | the transfer is not marked `bulk`, or `adaptive_inflight: false` | set `priority: bulk` on bulk forwards and `interactive` on shells |
| goroutine dump needed | a suspected hang | `kill -QUIT <pid>` prints all stacks to `~/.gotunnel/logs/client.out` (the process exits; launchd restarts it) |

To see handshakes, link events and dial failures, set `daemon.log_level: debug` and restart. `reload` does not apply daemon settings. Use `gotunnel down` then `gotunnel up -d`; under launchd, `gotunnel service install` restarts the service.

## Security notes

- Everything travels inside your ssh connection; gotunnel adds no network listener on the destination. agentd listens only on a unix socket in a 0700 directory.
- Local listeners should be on `127.0.0.1`: anyone who can connect to them uses the tunnel as you.
- Session tokens are random 256-bit values, are never logged, and are compared in constant time.
- No root is needed on either side.
