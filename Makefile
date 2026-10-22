# The version comes from the module version the Go toolchain records (git tag
# or pseudo-version), the same as for "go install", so client and agent builds
# from the same commit always match.
LDFLAGS := -s -w
PAYLOAD := internal/agentbin/payload
AGENT_ARCHS := amd64 arm64

STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2
DIST_PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64
DIST_VERSION = $(shell git describe --tags --always --dirty)

.PHONY: help build agent payload dist check test lint e2e lab-up lab-test lab-down clean

help:
	@echo "make build     client binary with embedded Linux agents (bin/gotunnel)"
	@echo "make dist      release binaries for linux/darwin amd64/arm64 + SHA256SUMS (dist/)"
	@echo "make check     lint + fast tests: run before every commit"
	@echo "make test      all tests with the race detector (includes the ~50s latency benchmark)"
	@echo "make e2e       end-to-end test of the real binary with a fake ssh"
	@echo "make lab-test  Docker lab with real sshd and network faults (needs Docker)"
	@echo "make clean     remove build outputs"

# On Linux the client binary must be static (CGO_ENABLED=0): it is uploaded as
# the agent to destinations with the same platform, and a cgo build would link
# the build machine's glibc and fail on older destinations.
ifeq ($(shell go env GOOS),linux)
HOST_CGO := CGO_ENABLED=0
endif

# Client binary for this machine, with the Linux agents embedded for install-agent.
build: payload
	$(HOST_CGO) go build -trimpath -tags embedagent -ldflags '$(LDFLAGS)' -o bin/gotunnel ./cmd/gotunnel

# Static Linux agent binaries.
agent:
	@mkdir -p dist
	for a in $(AGENT_ARCHS); do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$a go build -trimpath -ldflags '$(LDFLAGS)' -o dist/gotunnel-linux-$$a ./cmd/gotunnel || exit 1; \
	done

payload: agent
	@mkdir -p $(PAYLOAD)
	for a in $(AGENT_ARCHS); do gzip -9 -c dist/gotunnel-linux-$$a > $(PAYLOAD)/gotunnel-linux-$$a.gz; done

# Release binaries for every platform (each works as client and agent and
# embeds the Linux agents), named gotunnel-<version>-<os>-<arch>, plus
# SHA256SUMS. Run on a clean tagged commit so they report that version.
dist: payload
	@for p in $(DIST_PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "dist/gotunnel-$(DIST_VERSION)-$$os-$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -tags embedagent -ldflags '$(LDFLAGS)' \
			-o dist/gotunnel-$(DIST_VERSION)-$$os-$$arch ./cmd/gotunnel || exit 1; \
	done
	cd dist && (command -v sha256sum >/dev/null && sha256sum gotunnel-$(DIST_VERSION)-* || shasum -a 256 gotunnel-$(DIST_VERSION)-*) > SHA256SUMS

# Lint plus every test except the slow latency benchmark.
check: lint
	go test -race -short -count=1 ./...

test:
	go test -race -count=1 ./...

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l .; echo "run gofmt -w ."; exit 1)
	go run $(STATICCHECK) ./...

# End-to-end test of the real binary with a fake ssh (no network needed).
e2e:
	go test -race -count=1 ./test/e2e/

# Docker lab: client → jump1 (TLS, openssl s_client) → toxiproxy → jump2 → dest.
lab-up: build
	cd test/lab && ./lab.sh up

lab-test: lab-up
	cd test/lab && ./lab.sh test

lab-down:
	cd test/lab && ./lab.sh down

clean:
	rm -rf bin dist $(PAYLOAD)
