.DEFAULT_GOAL := test

# The database the suite talks to. The tests have no default of their own, so a
# run can never reach a database nobody named; this one is the container
# `make pg-up` starts. Point PGNOTCH_DSN elsewhere to override.
PG_PORT ?= 5432
PG_USER ?= pgnotch
PG_PASSWORD ?= pgnotch
PG_DATABASE ?= pgnotch
PGNOTCH_DSN ?= postgres://$(PG_USER):$(PG_PASSWORD)@localhost:$(PG_PORT)/$(PG_DATABASE)

.PHONY: help
help: ## List the targets
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F':.*##' '{ printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }'

# No skip: the package refuses to run without a DSN, so a green run spoke to PostgreSQL.
.PHONY: test
test: ## Run the suite (needs `make pg-up`, or a PGNOTCH_DSN of your own)
	PGNOTCH_DSN='$(PGNOTCH_DSN)' go test ./... -count=1 -timeout 10m

# The load generator, on the same database and by the same variable. Its own
# flags go in ARGS, since what a run should ask of the database is the whole of
# what it is for: `make load ARGS='-rps 500 -logs 8 -sizes 1k:9,32k:1'`.
.PHONY: load
load: ## Put an append load on logs of its own (ARGS='-rps 500 -logs 8')
	PGNOTCH_DSN='$(PGNOTCH_DSN)' go run ./cmd/pgnotch-load $(ARGS)

.PHONY: pg-up
pg-up: ## Start a PostgreSQL for the suite
	@PG_PORT=$(PG_PORT) PG_USER=$(PG_USER) PG_PASSWORD=$(PG_PASSWORD) \
		PG_DATABASE=$(PG_DATABASE) bash scripts/pg-up.sh

.PHONY: pg-down
pg-down: ## Remove the PostgreSQL container
	@bash scripts/pg-down.sh

# Pinned here and run with `go run` rather than added to go.mod: a linter's
# dependency tree is not a build input for this library.
GOLANGCI := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
MODERNIZE := golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v0.23.0

# modernize reports on stderr and exits 3 when it found something, so the run is
# judged by the filtered output rather than by the status.
.PHONY: lint
lint: ## Run the linters (needs no database)
	go run $(GOLANGCI) run --timeout 15m
	@out=$$(go run $(MODERNIZE) ./... 2>&1 | \
		grep -vE '^exit status|^go: (downloading|finding)|^#'); \
	if [ -n "$$out" ]; then printf '%s\n' "$$out"; exit 1; fi
	@echo "modernize: clean"

.PHONY: lint-fix
lint-fix: ## Apply what the linters can rewrite, then re-run them
	go run $(MODERNIZE) -fix ./... || true
	go run $(GOLANGCI) run --fix --timeout 15m
