package pgnotch

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

// The registry, goose's version table, and the prefix every log's entry tables
// are named from.
const (
	registryTable = "pgnotch_logs"
	versionTable  = "pgnotch_migrations"
	entriesPrefix = "pgnotch_entries_"
)

// The versioned half of the schema, as ordinary goose SQL migrations.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// entriesTable names one half of a log's ring. slot is 0 or 1. The name is built
// from the ordinal the registry assigned rather than from the [LogID], because
// an id may be any string and an identifier is limited to 63 bytes.
func entriesTable(ordinal int64, slot int16) string {
	return fmt.Sprintf("%s%d_%d", entriesPrefix, ordinal, slot)
}

// ringTables names both halves of one log's ring, in slot order. Create, reclaim
// and drop all come through here; the down migration does not, since it must
// enumerate the same tables in SQL. Widening the ring means editing
// `migrations/00001_registry.sql` too, and no test covers the down.
func ringTables(ordinal int64) []string {
	return []string{entriesTable(ordinal, 0), entriesTable(ordinal, 1)}
}

// Provider is the goose provider for this package's schema, for an operator who
// wants what goose offers beyond [Migrate]: status, a targeted up-to, a down.
// The down takes every log's entries with it, since the registry migration is
// the only thing that can enumerate the entry tables.
//
// db may be anything that speaks to the right database; [stdlib.OpenDBFromPool]
// turns a pool into one and closing the result leaves the pool open. Options
// given here are applied after this package's own, so [goose.WithLogger] gets
// back the logging the [goose.NopLogger] installed here takes away.
func Provider(db *sql.DB, opts ...goose.ProviderOption) (*goose.Provider, error) {
	// The version table is this package's own, so a caller's migrations can share
	// a schema with these without the two disagreeing about the version.
	store, err := database.NewStore(database.DialectPostgres, versionTable)
	if err != nil {
		return nil, fmt.Errorf("pgnotch: building the goose store: %w", err)
	}
	dir, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("pgnotch: reading the migrations: %w", err)
	}
	// A store is the only way to name the version table, so the dialect is empty;
	// the global registry stays off to keep a caller's migrations out of it.
	base := []goose.ProviderOption{
		goose.WithStore(store),
		goose.WithDisableGlobalRegistry(true),
		goose.WithLogger(goose.NopLogger()),
	}
	provider, err := goose.NewProvider("", db, dir, append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("pgnotch: building the goose provider: %w", err)
	}
	return provider, nil
}

// Migrate applies the schema to whatever schema the pool's search_path names.
// It is safe to run concurrently with itself and with a running [Store]: goose
// takes the version table's lock, and no migration touches a log's entry tables.
// It does not close pool.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	// Closing this leaves the pool open; its failure says nothing a caller can act on.
	defer func() { _ = db.Close() }()

	provider, err := Provider(db)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("pgnotch: migrating: %w", err)
	}
	return nil
}

// Drop removes every table this package owns: its logs' entry tables, the
// registry and goose's version table. The schema itself is left alone; an
// operator giving that back too wants DROP SCHEMA.
//
// A [Store] does not survive it: it caches which tables each log's entries are
// in, and since ordinal is GENERATED ALWAYS AS IDENTITY the identity goes with
// the registry table, so a re-migrated schema hands out ordinal 1 again and
// those cached names are either gone or a later log's, appended to without
// error. Drop what nothing is using, and open again afterwards.
func Drop(ctx context.Context, pool *pgxpool.Pool) error {
	tables, err := entryTables(ctx, pool)
	if err != nil {
		return err
	}
	tables = append(tables, registryTable, versionTable)
	// One statement, because dropping the registry first would leave a failure
	// part way through with tables nothing can enumerate.
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS `+strings.Join(tables, ", ")); err != nil {
		return fmt.Errorf("pgnotch: dropping the schema: %w", err)
	}
	return nil
}

// entryTables names both halves of every log's ring, from the registry, which is
// complete: an ordinal and its tables are created in one transaction and nothing
// deletes a row. A schema with no registry table has no logs, which is not an error.
func entryTables(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT ordinal FROM `+registryTable)
	if err == nil {
		var ordinals []int64
		if ordinals, err = pgx.CollectRows(rows, pgx.RowTo[int64]); err == nil {
			tables := make([]string, 0, 2*len(ordinals))
			for _, ordinal := range ordinals {
				tables = append(tables, ringTables(ordinal)...)
			}
			return tables, nil
		}
	}
	// pgx reports a missing table when the rows are read rather than when the
	// query is sent, so both ways of being told arrive here.
	if isUndefinedTable(err) {
		return nil, nil
	}
	return nil, fmt.Errorf("pgnotch: listing the logs: %w", err)
}

// migrated reports whether the schema was migrated at all, not whether it is at
// this build's version — the two coincide only while there is one migration —
// with a read that creates nothing: asking goose the richer question would
// create the version table, which [Open] must not do.
func migrated(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var name *string
	if err := pool.QueryRow(ctx,
		`SELECT to_regclass($1)::text`, registryTable).Scan(&name); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("pgnotch: looking for the schema: %w", err)
	}
	return name != nil, nil
}
