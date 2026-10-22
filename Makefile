VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 0.1.0-dev)
LDFLAGS := -s -w -X gotunnel/internal/version.Version=$(VERSION)
PAYLOAD := internal/agentbin/payload
AGENT_ARCHS := amd64 arm64

STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2

.PHONY: help build agent payload check test lint e2e lab-up lab-test lab-down clean

help:
	@echo "make build     client binary with embedded Linux agents (bin/gotunnel)"
	@echo "make check     lint + fast tests: run before every commit"
	@echo "make test      all tests with the race detector (includes the ~50s latency benchmark)"
	@echo "make e2e       end-to-end test of the real binary with a fake ssh"
	@echo "make lab-test  Docker lab with real sshd and network faults (needs Docker)"
	@echo "make clean     remove build outputs"

# Client binary for this machine, with the Linux agents embedded for install-agent.
build: payload
	go build -trimpath -tags embedagent -ldflags '$(LDFLAGS)' -o bin/gotunnel ./cmd/gotunnel

# Static Linux agent binaries.
agent:
	@mkdir -p dist
	for a in $(AGENT_ARCHS); do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$a go build -trimpath -ldflags '$(LDFLAGS)' -o dist/gotunnel-linux-$$a ./cmd/gotunnel || exit 1; \
	done

payload: agent
	@mkdir -p $(PAYLOAD)
	for a in $(AGENT_ARCHS); do gzip -9 -c dist/gotunnel-linux-$$a > $(PAYLOAD)/gotunnel-linux-$$a.gz; done

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
