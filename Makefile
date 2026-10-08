.DEFAULT_GOAL := help
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: help setup quality fmt test build security ci dev start stop logs

define in_modules
@mapfile -d '' -t modules < <( \
	find . -type d \( -name .git -o -name vendor -o -name docs \) -prune \
		-o -type f -name go.mod -print0 \
); \
if (( $${#modules[@]} == 0 )); then \
	echo "No application Go modules exist yet. Add a go.mod and application code before running checks." >&2; \
	exit 1; \
fi; \
for module_file in "$${modules[@]}"; do \
	( \
		cd "$$(dirname "$$module_file")"; \
		echo "$@ $$module_file"; \
		packages=$$(go list ./...); \
		if [[ -z "$$packages" ]]; then \
			echo "No Go packages found in $$module_file." >&2; \
			exit 1; \
		fi; \
		$(1) \
	); \
done
endef

help:
	@echo "make setup     Download Go dependencies"
	@echo "make quality   Check formatting and run go vet"
	@echo "make fmt       Format Go source files"
	@echo "make test      Run tests with race detection"
	@echo "make build     Compile Go packages"
	@echo "make security  Verify dependencies and scan known vulnerabilities"
	@echo "make ci        Run all four checks"
	@echo "make dev       Build and start Compose with foreground logs"
	@echo "make start     Start Compose in the background"
	@echo "make stop      Stop Compose and preserve volumes"
	@echo "make logs      Follow Compose logs"
	@echo "make integration  Test services with disposable PostgreSQL and RabbitMQ"
	@echo "make e2e         Verify a disposable full Compose stack"
	@echo "make e2e-p2      Include a 90-second k6 load test and TCP connection samples"
	@echo "make evidence-images  Run tests and capture their output"

setup:
	$(call in_modules,go mod download)

quality:
	$(call in_modules, \
		unformatted=$$(find . -type d \( -name .git -o -name vendor \) -prune \
			-o -type f -name '*.go' -print0 | xargs -0 -r gofmt -l); \
		if [[ -n "$$unformatted" ]]; then \
			printf '%s\n' "$$unformatted"; \
			echo "Run make fmt to format these files." >&2; \
			exit 1; \
		fi; \
		go vet ./...)

fmt:
	$(call in_modules, \
		find . -type d \( -name .git -o -name vendor \) -prune \
			-o -type f -name '*.go' -print0 | xargs -0 -r gofmt -w)

test:
	$(call in_modules,go test -race -count=1 -timeout=5m ./...)

build:
	$(call in_modules,go build ./...)

security:
	$(call in_modules, \
		go mod verify; \
		go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 -test ./...)

ci:
	@$(MAKE) quality
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

.PHONY: integration
integration:
	./scripts/test-aggregator-integration.sh

.PHONY: e2e e2e-p2
e2e:
	python3 scripts/verify-aggregator-stack.py

e2e-p2:
	python3 scripts/verify-aggregator-stack.py --p2

FREEZE ?= freeze
.PHONY: evidence-images
evidence-images:
	python3 scripts/render-test-evidence.py --freeze "$(FREEZE)"
