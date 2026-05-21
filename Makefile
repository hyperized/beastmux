.PHONY: all help fmt vet test test-integration lint cover build build-aarch64 clean

PKG          := ./...
COVERPROFILE := coverage.out
BINARY       := beastmux

# VERSION is derived from the nearest reachable tag, with -dirty when
# the worktree has uncommitted changes. Override on the command line
# (make build VERSION=v1.2.3-rc1) for one-off release builds.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

all: fmt vet test ## fmt + vet + test (default)

help: ## show this help text
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## format code via golangci-lint formatters (gofmt + gci); falls back to go fmt
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint fmt $(PKG); \
	else \
		echo "golangci-lint not found — falling back to 'go fmt'"; \
		go fmt $(PKG); \
	fi

vet: ## run go vet
	go vet $(PKG)

test: ## run unit tests with -race and coverage
	go test -race -cover -coverprofile=$(COVERPROFILE) $(PKG)

# Integration tests live behind the `integration` build tag so they
# do not run as part of the default `make test`. Use this slot for
# end-to-end tests that spin up real TCP listeners + dialers and
# exercise the full mux/dedup/fanout chain.
test-integration: ## run integration tests (build tag: integration)
	go test -race -tags=integration $(PKG)

cover: test ## render coverage as coverage.html
	go tool cover -html=$(COVERPROFILE) -o coverage.html

lint: ## run golangci-lint
	golangci-lint run $(PKG)

build: ## build the beastmux binary for the host platform
	go build -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/beastmux

build-aarch64: ## cross-build the beastmux binary for linux/arm64
	env GOOS=linux GOARCH=arm64 go build -ldflags '$(LDFLAGS)' -o $(BINARY)-aarch64 ./cmd/beastmux

clean: ## remove generated artefacts
	rm -f $(COVERPROFILE) coverage.html $(BINARY) $(BINARY)-aarch64
