# AI Engineering Workflow Makefile — farm-server
# Compatible with Linux / macOS / Windows Git Bash.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

GO ?= go
GOLANGCI_LINT ?= golangci-lint
COVERAGE_MIN ?= 0
BUF ?= buf
BUF_CACHE_DIR ?= /tmp/farm-server-buf-cache

.PHONY: help doctor fmt-check lint test test-short build coverage coverage-check proto-lint proto-generate proto-breaking proto-check check route9-acceptance route9-soak route9-mysql route9-external route9-stateful route9-scale route9-kafka-outage route10-observability-check clean status ownership

ROUTE95_SOAK_DURATION ?= 30m
ROUTE95_SOAK_RPS ?= 20000
ROUTE95_SOAK_TIMEOUT ?= 35m
ROUTE95_GATEWAY_URLS ?= http://127.0.0.1:28080
ROUTE95_USERS ?= 1000
ROUTE95_TARGET_RPS ?= 1000
ROUTE95_MIN_ACCEPTED_RATIO ?= 1.0
ROUTE95_STATEFUL_DURATION ?= 30m
ROUTE95_WARMUP ?= 30s
ROUTE95_DEVICE_PREFIX ?= route95-stateful
ROUTE95_MIX ?= purchase=49,sell=49,plant=1,water=1,harvest=0
ROUTE95_OUTPUT ?=
ROUTE95_SCALE ?= 1

help:
	@echo "farm-server AI workflow commands:"
	@echo ""
	@echo "  make doctor                       Check required tools and rule files"
	@echo "  make fmt-check                    Check Go formatting"
	@echo "  make lint                         Run golangci-lint (if installed)"
	@echo "  make test                         Run tests (no race by default; use -race when CGO/gcc available)"
	@echo "  make test-short                   Run short tests"
	@echo "  make build                        Compile all Go packages (from server/)"
	@echo "  make proto-check                  Lint/generate/check protobuf contracts"
	@echo "  make check                        Full local quality gate"
	@echo "  make route9-acceptance            Route 9 local E2E/fault/race gates"
	@echo "  make route9-soak                  Route 9 scheduler soak (default 30m at 20k/s)"
	@echo "  make route9-mysql                 Route 9 ACK-loss/fence gate against disposable MySQL"
	@echo "  make route9-external              Route 9 real MySQL/Redis/Kafka middleware gates"
	@echo "  make route9-stateful              Public-protocol stateful capacity driver"
	@echo "  make route9-scale ROUTE95_SCALE=N Run one prepared 1/2/4/8 topology measurement"
	@echo "  make route9-kafka-outage          Run guarded route95 Kafka outage/recovery harness"
	@echo "  make route10-observability-check  Validate Route 10 metrics/tools without load"
	@echo "  make coverage-check COVERAGE_MIN=70"
	@echo "  make clean                        Remove generated artifacts"
	@echo "  make status                       Show workflow status and Git summary"
	@echo "  make ownership                    Show daily ownership checklist"

doctor:
	@command -v "$(GO)" >/dev/null || { echo "[ERROR] go not found"; exit 2; }
	@test -f server/go.mod || { echo "[ERROR] server/go.mod not found"; exit 2; }
	@for f in CONSTITUTION.md WORKFLOW.md STATUS.md; do \
		test -f ".ai-rules/$$f" || { echo "[ERROR] missing .ai-rules/$$f"; exit 2; }; \
	done
	@echo "[OK] doctor"

fmt-check:
	@cd server && \
	files="$$(git ls-files --cached --others --exclude-standard '*.go' 2>/dev/null || true)"; \
	if [ -z "$$files" ]; then \
		echo "[INFO] no tracked Go files; format check skipped"; \
	else \
		bad="$$(gofmt -l $$files)"; \
		test -z "$$bad" || { echo "[ERROR] gofmt required:"; printf '%s\n' "$$bad"; exit 1; }; \
	fi
	@echo "[OK] format"

lint:
	@if command -v "$(GOLANGCI_LINT)" >/dev/null; then \
		cd server && $(GOLANGCI_LINT) run ./... && echo "[OK] lint"; \
	else \
		echo "[INFO] golangci-lint not installed; lint skipped"; \
	fi

test:
	@cd server && $(GO) test -count=1 -timeout 120s ./...
	@echo "[OK] tests"

test-short:
	@cd server && $(GO) test -short -count=1 -timeout 60s ./...
	@echo "[OK] short tests"

build:
	@cd server && $(GO) build ./...
	@echo "[OK] build"

proto-lint:
	@command -v "$(BUF)" >/dev/null || { echo "[ERROR] buf not found"; exit 2; }
	@cd server && BUF_CACHE_DIR="$(BUF_CACHE_DIR)" $(BUF) lint
	@echo "[OK] proto lint"

proto-generate:
	@command -v "$(BUF)" >/dev/null || { echo "[ERROR] buf not found"; exit 2; }
	@cd server && BUF_CACHE_DIR="$(BUF_CACHE_DIR)" $(BUF) generate
	@echo "[OK] proto generate"

proto-breaking:
	@command -v "$(BUF)" >/dev/null || { echo "[ERROR] buf not found"; exit 2; }
	@if git cat-file -e main:server/proto 2>/dev/null; then \
		cd server && BUF_CACHE_DIR="$(BUF_CACHE_DIR)" $(BUF) breaking --against '../.git#branch=main,subdir=server'; \
		echo "[OK] proto breaking"; \
	else \
		echo "[INFO] initial protobuf baseline; breaking gate activates after the first merge to main"; \
	fi

proto-check: proto-lint proto-generate proto-breaking
	@git diff --exit-code -- server/gen
	@echo "[OK] generated protobuf files are current"

coverage:
	@cd server && $(GO) test -count=1 -coverprofile=coverage.out ./...
	@cd server && $(GO) tool cover -func=coverage.out | grep '^total:' || true

coverage-check: coverage
	@pct="$$(cd server && $(GO) tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}')"; \
	if [ "$(COVERAGE_MIN)" = "0" ]; then \
		echo "[INFO] coverage threshold disabled"; \
	else \
		test -n "$$pct" || { echo "[ERROR] unable to read total coverage"; exit 2; }; \
		if awk -v got="$$pct" -v min="$(COVERAGE_MIN)" 'BEGIN { exit !(got + 0 >= min + 0) }'; then \
			echo "[OK] coverage $$pct% >= $(COVERAGE_MIN)%"; \
		else \
			echo "[ERROR] coverage $$pct% < $(COVERAGE_MIN)%"; exit 1; \
		fi; \
	fi

check: doctor fmt-check proto-check test build
	@if command -v "$(GOLANGCI_LINT)" >/dev/null; then $(MAKE) lint; fi
	@echo "[OK] full quality gate"

route9-acceptance:
	@cd server && $(GO) test -count=1 -timeout 180s ./...
	@cd server && $(GO) test -race -count=1 -timeout 300s \
		./internal/farm/actor ./internal/farm/infrastructure \
		./internal/farm/routing ./internal/farm/realtime \
		./internal/gateway/ws ./internal/rpccontract \
		./pkg/discovery ./pkg/redisstore
	@echo "[OK] Route 9 local acceptance; external MySQL and 30-60m soak remain separate gates"

route9-soak:
	@cd server && ROUTE95_SOAK_DURATION="$(ROUTE95_SOAK_DURATION)" \
		ROUTE95_SOAK_RPS="$(ROUTE95_SOAK_RPS)" \
		$(GO) test ./internal/farm/actor -run '^TestRoute95SchedulerSoak$$' \
		-v -count=1 -timeout "$(ROUTE95_SOAK_TIMEOUT)"

route9-mysql:
	@test -n "$${ROUTE95_MYSQL_DSN:-}" || { echo "[ERROR] ROUTE95_MYSQL_DSN is required"; exit 2; }
	@cd server && $(GO) test ./internal/farm/infrastructure \
		-run '^TestRoute95ExternalMySQLACKLossAndFence$$' -v -count=1 -timeout 120s

route9-external:
	@for name in ROUTE95_MYSQL_DSN ROUTE95_REDIS_ADDR ROUTE95_KAFKA_BROKERS; do \
		test -n "$${!name:-}" || { echo "[ERROR] $$name is required"; exit 2; }; \
	done
	@cd server && $(GO) test ./internal/farm/infrastructure \
		-run '^TestRoute95External(MySQLACKLossAndFence|KafkaRoundTrip)$$' \
		-v -count=1 -timeout 120s
	@cd server && $(GO) test ./internal/farm/realtime \
		-run '^TestRoute95ExternalRedisPubSubRoundTrip$$' \
		-v -count=1 -timeout 60s

route9-stateful:
	@cd server && $(GO) run ../loadtest/route95_stateful.go \
		-urls "$(ROUTE95_GATEWAY_URLS)" -users "$(ROUTE95_USERS)" \
		-target-rps "$(ROUTE95_TARGET_RPS)" -min-accepted-ratio "$(ROUTE95_MIN_ACCEPTED_RATIO)" \
		-duration "$(ROUTE95_STATEFUL_DURATION)" \
		-warmup "$(ROUTE95_WARMUP)" -device-prefix "$(ROUTE95_DEVICE_PREFIX)" \
		-mix "$(ROUTE95_MIX)" $(if $(ROUTE95_OUTPUT),-output "$(ROUTE95_OUTPUT)")

route9-scale:
	@case "$(ROUTE95_SCALE)" in 1|2|4|8) ;; *) echo "[ERROR] ROUTE95_SCALE must be 1, 2, 4, or 8"; exit 2;; esac
	@$(MAKE) route9-stateful

route9-kafka-outage:
	@bash loadtest/route95_kafka_outage.sh

route10-observability-check:
	@bash -n loadtest/route10_collect_mysql.sh loadtest/route10_collect_host.sh \
		loadtest/route10_snapshot.sh loadtest/route10_freeze_baseline.sh \
		loadtest/route10_seed_snapshot.sh
	@cd server && $(GO) test ./pkg/admission ./pkg/observability ./pkg/errcode \
		./pkg/rpcgrpc ./internal/farm/infrastructure ./internal/gateway/http \
		./internal/rpccontract
	@cd server && $(GO) test ../loadtest/route95_stateful.go ../loadtest/route95_stateful_test.go
	@echo "[OK] Route 10 observability code/tools; no formal load was generated"

clean:
	@rm -f -- server/coverage.out server/coverage.html
	@echo "[OK] clean"

status:
	@echo "Workflow state: .ai-rules/STATUS.md"
	@git -C . status --short 2>/dev/null || true
	@git -C . log -1 --oneline 2>/dev/null || true

ownership:
	@echo "Open and complete: .ai-rules/OWNERSHIP.md"
