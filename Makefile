.DEFAULT_GOAL := test

# The database the suite talks to. The tests themselves have no default — one
# that reached a database nobody named could truncate tables somebody cared
# about — so the default is here, where it names the container `make pg-up`
# starts and nothing else. Point PGNOTCH_DSN at your own to use it instead; it
# must be PostgreSQL 16 or newer, which is what pg-up.sh checks by probing
# `bytea STORAGE PLAIN`.
PG_PORT ?= 5433
PG_USER ?= pgnotch
PG_PASSWORD ?= pgnotch
PG_DATABASE ?= pgnotch
PGNOTCH_DSN ?= postgres://$(PG_USER):$(PG_PASSWORD)@localhost:$(PG_PORT)/$(PG_DATABASE)

.PHONY: help
help: ## List the targets
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | \
		awk -F':.*##' '{ printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }'

# No skip and no filter: the package refuses to run without a DSN, so a green
# run here is a run that spoke to PostgreSQL.
.PHONY: test
test: ## Run the suite (needs `make pg-up`, or a PGNOTCH_DSN of your own)
	PGNOTCH_DSN='$(PGNOTCH_DSN)' go test ./... -count=1 -timeout 10m

.PHONY: pg-up
pg-up: ## Start a PostgreSQL for the suite
	@PG_PORT=$(PG_PORT) PG_USER=$(PG_USER) PG_PASSWORD=$(PG_PASSWORD) \
		PG_DATABASE=$(PG_DATABASE) bash scripts/pg-up.sh

.PHONY: pg-down
pg-down: ## Remove the PostgreSQL container
	@bash scripts/pg-down.sh

# Two linters, because they answer different questions: golangci-lint is the
# idiom and correctness set, and modernize is "the standard library grew a way
# to say this" — it ships with gopls and it rewrites rather than only reports.
# Both are pinned here and run with `go run` rather than added to go.mod: a
# linter's dependency tree is not a build input for a library whose whole
# requirement list is pgx and goose.
GOLANGCI := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
MODERNIZE := golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v0.23.0

# modernize reports on stderr and exits 3 when it found something, so the run is
# judged by what is left after the noise is filtered rather than by the status.
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
