# tc-repro — v5 backfill workload (mysqld SIGSEGV reproduction).
# Build / test convenience only; no runtime orchestration (host session
# management lives in run-unattended.sh and is a host concern).

SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

MODULE      := github.com/matias-sanchez/pt-simulates/testcase_v5
BIN_NAME    := tc-repro
BUILD_DIR   := build
LINUX_BIN   := $(BUILD_DIR)/$(BIN_NAME)-linux-amd64
SCHEMA      := sql/schema.sql
SCHEMA_EMB  := internal/schema/embedded.sql

# Provenance stamped into the binary (visible via `tc-repro --config <c> status`
# artifacts). On an unborn HEAD, fall back to "unknown" so ldflags stay valid.
COMMIT     := $(shell git rev-parse --verify HEAD 2>/dev/null)
ifeq ($(COMMIT),)
COMMIT := unknown
endif
BUILT_AT   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS    := -s -w -X main.commit=$(COMMIT) -X main.builtAt=$(BUILT_AT)

.PHONY: all schema-check schema-sync build build-linux test lint vet clean smoke smoke-up smoke-down

all: build

# The schema is embedded via go:embed (which forbids escaping the package
# directory), so a copy lives in internal/schema/. This keeps the two in sync.
schema-check:
	@cmp $(SCHEMA) $(SCHEMA_EMB) || { \
		echo "embedded.sql diverges from $(SCHEMA); rerun: make schema-sync"; \
		exit 1; \
	}

schema-sync:
	cp $(SCHEMA) $(SCHEMA_EMB)

# Local build for the current platform.
build: schema-check
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(BIN_NAME) ./cmd/$(BIN_NAME)

# Static linux/amd64 build to run on the target MySQL host (pure Go, no cgo).
build-linux: schema-check
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
		go build -trimpath -ldflags="$(LDFLAGS)" -o $(LINUX_BIN) ./cmd/$(BIN_NAME)

# Unit tests. The Docker-backed integration smoke is tagged `integration`
# and is NOT run here (use `make smoke`).
test: schema-check
	go test ./...

lint: schema-check
	golangci-lint run

vet:
	go vet ./...

clean:
	rm -rf $(BUILD_DIR)

# Integration smoke: bring up a containerized MySQL 8.0.36, run init+run
# end-to-end, tear down. Requires Docker (see compose/docker-compose.yml).
smoke: smoke-up
	go test -tags=integration -timeout 5m ./internal/smoke/... || { \
		$(MAKE) smoke-down; \
		exit 1; \
	}
	$(MAKE) smoke-down

smoke-up:
	docker compose -f compose/docker-compose.yml up -d

smoke-down:
	docker compose -f compose/docker-compose.yml down -v
