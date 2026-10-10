.DEFAULT_GOAL := help
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: help env lint fmt test build security ci dev start stop logs integration demo

GOLANGCI_LINT_VERSION := v2.14.0
GOLANGCI_LINT := $(CURDIR)/bin/golangci-lint-$(GOLANGCI_LINT_VERSION)/golangci-lint$(if $(filter Windows_NT,$(OS)),.exe)

define in_modules
@mapfile -d '' -t modules < <( \
	find . -type d \( -name .git -o -name vendor -o -name docs \) -prune \
		-o -type f -name go.mod -print0 \
); \
for module_file in "$${modules[@]}"; do \
	( \
		cd "$$(dirname "$$module_file")"; \
		echo "$@ $$module_file"; \
		$(1) \
	); \
done
endef

help:
	@echo "make env          Write .env from .env.example with fresh secrets"
	@echo "make lint         Check formatting, go vet, and the linters with golangci-lint"
	@echo "make fmt          Format Go source files with gofumpt and goimports"
	@echo "make test         Run tests with race detection"
	@echo "make build        Compile Go packages"
	@echo "make security     Verify dependencies and scan known vulnerabilities"
	@echo "make ci           Run all four checks"
	@echo "make dev          Build and start Compose with foreground logs"
	@echo "make start        Start Compose in the background"
	@echo "make stop         Stop Compose and preserve volumes"
	@echo "make logs         Follow Compose logs"
	@echo "make integration  Test services with disposable PostgreSQL and RabbitMQ"
	@echo "make demo         Check every spec criterion on Compose. P=P3 runs one problem, LOAD=1 adds k6, KEEP=1 keeps the stack"

env:
	./scripts/env.sh

$(GOLANGCI_LINT):
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$(dir $@)" $(GOLANGCI_LINT_VERSION)

# gofumpt only separates multiline declarations, so this also covers one-line ones.
lint: $(GOLANGCI_LINT)
	@git ls-files -co --exclude-standard '*.go' | xargs awk ' \
		FNR == 1 { prev = "" } \
		/^(func|type|var|const) / && prev != "" && prev !~ /^\/\// { \
			print FILENAME ":" FNR ": add a blank line above this top-level declaration"; bad = 1 \
		} \
		{ prev = $$0 } END { exit bad }'
	$(call in_modules,"$(GOLANGCI_LINT)" run ./...)

fmt: $(GOLANGCI_LINT)
	$(call in_modules,"$(GOLANGCI_LINT)" fmt ./...)

test:
	$(call in_modules,go test -race -count=1 -timeout=5m ./...)

build:
	$(call in_modules,go build ./...)

security:
	$(call in_modules, \
		go mod verify; \
		go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 -test ./...)

ci:
	@$(MAKE) lint
	@$(MAKE) test
	@$(MAKE) build
	@$(MAKE) security

dev:
	docker compose up --build

start:
	docker compose up -d --build

stop:
	docker compose down

logs:
	docker compose logs -f

integration:
	./scripts/integration.sh

DEMO_RUN = TestDemo$(if $(P),/$(P))

demo:
	cd demo && DEMO_LOAD=$(LOAD) DEMO_KEEP=$(KEEP) go test -tags demo -count=1 -timeout 30m -v -run '$(DEMO_RUN)' .
