#!/usr/bin/env bash
# Docker lab reproducing the target environment:
#
#   this machine --TLS (openssl s_client)--> jump1 --> toxiproxy --> jump2 --> dest
#
# Only dest runs gotunnel. toxiproxy sits on the jump1→jump2 hop and is used to
# cut, blackhole or slow down the path while connections are open.
#
#   ./lab.sh up     build the image, start the hosts, install the agent
#   ./lab.sh test   run the scenarios against the real binary
#   ./lab.sh down   remove containers and networks
set -euo pipefail
cd "$(dirname "$0")"
LAB=$PWD
WORK=$LAB/.work
ROOT=$(cd ../.. && pwd)
BIN=$ROOT/bin/gotunnel
IMAGE=gotunnel-lab
TOXIPROXY_IMAGE=ghcr.io/shopify/toxiproxy:2.12.0
TOXI=http://127.0.0.1:18474
CFG=$WORK/config.yaml

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*"; exit 1; }
pass() { printf '\033[32mok: %s\033[0m\n' "$*"; }

toxic() { # toxic add NAME JSON | toxic rm NAME
	case $1 in
	add) curl -fsS -X POST "$TOXI/proxies/jump2/toxics" -d "$3" >/dev/null ;;
	rm) curl -fsS -X DELETE "$TOXI/proxies/jump2/toxics/$2" >/dev/null || true ;;
	esac
}

up() {
	mkdir -p "$WORK"
	[ -f "$WORK/id_ed25519" ] || ssh-keygen -q -t ed25519 -N '' -f "$WORK/id_ed25519" -C gotunnel-lab
	[ -f "$WORK/tls.pem" ] || openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj /CN=jump1 \
		-keyout "$WORK/tls.key" -out "$WORK/tls.crt" 2>/dev/null
	cat "$WORK/tls.key" "$WORK/tls.crt" > "$WORK/tls.pem"

	step "building image"
	mkdir -p "$WORK/ctx"
	cp Dockerfile entrypoint.sh "$WORK/ctx/"
	docker build -q -t "$IMAGE" "$WORK/ctx" >/dev/null

	step "starting hosts"
	down >/dev/null 2>&1 || true
	rm -f "$WORK/known_hosts" # host keys are regenerated with every image build
	for n in edge mid inner; do docker network create "gtlab-$n" >/dev/null; done
	run() { # run NAME ROLE NETWORK [docker args...]
		local name=$1 role=$2 net=$3
		shift 3
		docker run -d --name "gtlab-$name" --hostname "$name" --network "gtlab-$net" --network-alias "$name" \
			-v "$WORK:/lab:ro" "$@" "$IMAGE" "$role" >/dev/null
	}
	run jump1 jump1 edge -p 127.0.0.1:2443:443
	docker network connect --alias jump1 gtlab-mid gtlab-jump1
	run jump2 jump2 mid
	docker network connect --alias jump2 gtlab-inner gtlab-jump2
	run dest dest inner
	docker run -d --name gtlab-toxiproxy --network gtlab-mid --network-alias toxiproxy \
		-p 127.0.0.1:18474:8474 "$TOXIPROXY_IMAGE" -host 0.0.0.0 >/dev/null
	for _ in $(seq 50); do curl -fsS "$TOXI/version" >/dev/null 2>&1 && break; sleep 0.2; done
	curl -fsS -X POST "$TOXI/proxies" \
		-d '{"name":"jump2","listen":"0.0.0.0:22022","upstream":"jump2:22","enabled":true}' >/dev/null

	cat > "$WORK/ssh_config" <<EOF
Host lab-jump1
  HostName jump1
  ProxyCommand openssl s_client -quiet -connect 127.0.0.1:2443
Host lab-jump2
  HostName toxiproxy
  Port 22022
  ProxyJump lab-jump1
Host lab-dest
  HostName dest
  ProxyJump lab-jump2
Host *
  User tunnel
  IdentityFile $WORK/id_ed25519
  IdentitiesOnly yes
  UserKnownHostsFile $WORK/known_hosts
  StrictHostKeyChecking accept-new
  LogLevel ERROR
EOF
	cat > "$CFG" <<EOF
ssh:
  host: lab-dest
  extra_args: ["-F", "$WORK/ssh_config"]
connection:
  ping_interval: 5s
  dead_after: 20s
  connect_timeout: 1m
  session_ttl: 10m
daemon:
  log_level: debug
forwards:
  - name: dest-ssh
    listen: 127.0.0.1:12222
    target: 127.0.0.1:22
    mode: always
    priority: interactive
  - name: echo
    listen: 127.0.0.1:17000
    target: 127.0.0.1:7000
    priority: interactive
  - name: web
    listen: 127.0.0.1:18080
    target: 127.0.0.1:8080
    priority: bulk
socks:
  - listen: 127.0.0.1:11080
reverse:
  - name: back
    remote_listen: 127.0.0.1:19000
    target: 127.0.0.1:19100
EOF
	for i in $(seq 50); do
		ssh -F "$WORK/ssh_config" -o BatchMode=yes lab-dest true 2>/dev/null && break
		[ "$i" = 50 ] && fail "ssh lab-dest does not work"
		sleep 0.5
	done
	pass "ssh lab-dest works through TLS jump1, toxiproxy and jump2"
}

down() {
	docker rm -f gtlab-jump1 gtlab-jump2 gtlab-dest gtlab-toxiproxy >/dev/null 2>&1 || true
	for n in edge mid inner; do docker network rm "gtlab-$n" >/dev/null 2>&1 || true; done
}

client_env() {
	export GOTUNNEL_HOME=$WORK/client
	export GOTUNNEL_RUNTIME_DIR=/tmp/gtlab-$(id -u)
	mkdir -p "$GOTUNNEL_RUNTIME_DIR"
}

state() { "$BIN" status --json 2>/dev/null | sed -n "s/.*\"$1\": *\"\{0,1\}\([^\",]*\).*/\1/p" | head -1; }

wait_state() { # wait_state STATE SECONDS
	for _ in $(seq $(($2 * 5))); do
		[ "$(state state)" = "$1" ] && return 0
		sleep 0.2
	done
	return 1
}

echo_rtt() { # prints the median echo round trip in ms through the echo forward
	python3 - "$@" <<'EOF'
import socket, statistics, sys, time
n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
s = socket.create_connection(("127.0.0.1", 17000), timeout=60)
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
samples = []
for i in range(n):
    t = time.time()
    s.sendall(b"k")
    if s.recv(1) != b"k":
        sys.exit("bad echo")
    samples.append((time.time() - t) * 1000)
    time.sleep(0.2)
print(int(statistics.median(samples)))
EOF
}

test_lab() {
	[ -x "$BIN" ] || fail "$BIN missing: run 'make build' first"
	client_env
	"$BIN" down >/dev/null 2>&1 || true

	step "install-agent through the jump chain"
	"$BIN" install-agent -c "$CFG"

	step "doctor"
	"$BIN" doctor -c "$CFG" || fail "doctor reported problems"

	step "start the client in the background"
	t0=$(date +%s)
	"$BIN" up -d -c "$CFG"
	wait_state connected 90 || { "$BIN" status; fail "not connected"; }
	pass "connected in $(($(date +%s) - t0))s"

	step "ssh to dest through the tunnel"
	out=$(ssh -F "$WORK/ssh_config" -o BatchMode=yes -o HostKeyAlias=dest -p 12222 tunnel@127.0.0.1 hostname)
	[ "$out" = dest ] || fail "got '$out'"
	pass "ssh -p 12222 localhost reached $out"

	step "download 20 MiB through the bulk forward"
	want=$(curl -fsS http://127.0.0.1:18080/big.sha256)
	got=$(curl -fsS http://127.0.0.1:18080/big.bin | shasum -a 256 | cut -d' ' -f1)
	[ "$got" = "$want" ] || fail "checksum mismatch"
	pass "checksum matches"

	step "SOCKS: dest's web server by name"
	got=$(curl -fsS --socks5-hostname 127.0.0.1:11080 http://localhost:8080/big.sha256)
	[ "$got" = "$want" ] || fail "socks got '$got'"
	pass "socks5h works (name resolved on dest)"

	step "reverse forward: dest reaches this machine"
	mkdir -p "$WORK/www" && echo "hello from the client" > "$WORK/www/hi.txt"
	(cd "$WORK/www" && exec python3 -m http.server 19100 --bind 127.0.0.1 >/dev/null 2>&1) &
	web=$!
	trap 'kill $web 2>/dev/null || true' EXIT
	sleep 1
	got=$(docker exec gtlab-dest wget -qO- http://127.0.0.1:19000/hi.txt)
	[ "$got" = "hello from the client" ] || fail "reverse got '$got'"
	pass "dest fetched a file served on this machine"

	step "a download survives a connection reset on the jump1→jump2 hop"
	toxic add bw '{"name":"bw","type":"bandwidth","stream":"downstream","attributes":{"rate":1024}}'
	curl -fsS http://127.0.0.1:18080/big.bin -o "$WORK/big.bin" &
	dl=$!
	sleep 5
	resumes=$(state resumes)
	toxic add cut '{"name":"cut","type":"reset_peer","stream":"downstream","attributes":{"timeout":0}}'
	sleep 3
	toxic rm cut
	wait $dl || fail "download failed"
	toxic rm bw
	got=$(shasum -a 256 "$WORK/big.bin" | cut -d' ' -f1)
	[ "$got" = "$want" ] || fail "checksum mismatch after reset"
	[ "$(state resumes)" -gt "$resumes" ] || fail "no resume recorded"
	pass "download completed intact; session resumed ($(state resumes) resumes so far)"

	step "an interactive ssh session survives a 35s blackhole (longer than dead_after)"
	ssh -F "$WORK/ssh_config" -o BatchMode=yes -o HostKeyAlias=dest -o ServerAliveInterval=0 -p 12222 tunnel@127.0.0.1 \
		'for i in $(seq 1 45); do echo tick $i; sleep 1; done' > "$WORK/ticks.txt" 2>&1 &
	sshpid=$!
	sleep 5
	toxic add hole '{"name":"hole","type":"timeout","stream":"downstream","attributes":{"timeout":0}}'
	toxic add hole-up '{"name":"hole-up","type":"timeout","stream":"upstream","attributes":{"timeout":0}}'
	sleep 35
	toxic rm hole
	toxic rm hole-up
	wait $sshpid || { cat "$WORK/ticks.txt"; fail "ssh session broke"; }
	[ "$(grep -c tick "$WORK/ticks.txt")" = 45 ] || { cat "$WORK/ticks.txt"; fail "ticks missing"; }
	pass "all 45 ticks arrived; the ssh session never noticed (link losses: $(state link_losses))"

	step "keystroke latency on a slow path (512 kbit/s, +100ms) while downloading"
	toxic add slow '{"name":"slow","type":"bandwidth","stream":"downstream","attributes":{"rate":64}}'
	toxic add lat '{"name":"lat","type":"latency","stream":"downstream","attributes":{"latency":100}}'
	idle=$(echo_rtt 10)
	curl -fsS http://127.0.0.1:18080/big.bin -o /dev/null --max-time 40 &
	dl=$!
	sleep 8
	busy=$(echo_rtt 20)
	kill $dl 2>/dev/null || true
	wait $dl 2>/dev/null || true
	toxic rm slow
	toxic rm lat
	pass "echo round trip: ${idle}ms idle, ${busy}ms during a bulk download"

	step "status"
	"$BIN" status

	step "stop"
	"$BIN" down
	docker exec gtlab-dest su tunnel -c '~/.gotunnel/bin/gotunnel agent status' || true
}

case "${1:-}" in
up) up ;;
test) test_lab ;;
down) down ;;
*) echo "usage: $0 up|test|down" >&2; exit 2 ;;
esac
