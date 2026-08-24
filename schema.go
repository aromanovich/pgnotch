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

// The tables this package owns. Every name is unqualified and so resolves
// through the connection's search_path, which is what makes a PostgreSQL schema
// the unit of separation: point two deployments at two schemas and they share
// nothing, with no prefix for this package to invent and none for a caller to
// keep unique.
const (
	registryTable = "pgnotch_logs"
	versionTable  = "pgnotch_migrations"
	entriesPrefix = "pgnotch_entries_"
)

// The versioned half of the schema, as the files goose reads. They are ordinary
// goose SQL migrations with nothing in them this package has to supply, which
// is what lets the goose CLI apply the same directory against the same DSN —
// see the README.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// entriesTable names one half of a log's ring. slot is 0 or 1.
//
// The name is built from the ordinal the registry assigned the log rather than
// from its [LogID], which is what lets an id be any string at all: an ordinal is
// short, an identifier is 63 bytes, and a caller's ids are neither this
// package's to shorten nor safe to interpolate.
func entriesTable(ordinal int64, slot int16) string {
	return fmt.Sprintf("%s%d_%d", entriesPrefix, ordinal, slot)
}

// ringTables names both halves of one log's ring, in slot order. Creating them,
// locking them for a reclaim and dropping them all come through here, so none of
// the three is the place that learns the ring got wider.
//
// It is not the only place that knows, and the other one is in another language:
// the down migration has to enumerate the same tables with no Go to call, so it
// spells the name and the slot range itself. Widening the ring means editing
// `migrations/00001_registry.sql` too, and nothing here will say so — the down
// is not on any test's path.
func ringTables(ordinal int64) []string {
	return []string{entriesTable(ordinal, 0), entriesTable(ordinal, 1)}
}

// Provider is the goose provider for this package's schema, for an operator who
// wants what goose offers beyond [Migrate] — a status, a targeted up-to, a
// down. Callers who only want the schema applied want [Migrate].
//
// The down takes every log's entries with it, and cannot do otherwise: there is
// one migration and it is the registry, which is also the only thing that can
// enumerate the entry tables. Rolling it back while leaving them would strand
// them where nothing — no later migration, no [Drop] — could ever name them
// again. So a down here is not a gentler [Drop]; it is the same loss with
// goose's version table left behind.
//
// db may be anything that speaks to the right database; [stdlib.OpenDBFromPool]
// turns a pool into one and closing the result leaves the pool open. Options
// given here are applied after this package's own, so a caller that wants
// goose's logging back passes [goose.WithLogger].
func Provider(db *sql.DB, opts ...goose.ProviderOption) (*goose.Provider, error) {
	// The version table is this package's own rather than goose's default, so
	// that a caller's migrations can share a schema with these without the two
	// disagreeing about what version the schema is at.
	store, err := database.NewStore(database.DialectPostgres, versionTable)
	if err != nil {
		return nil, fmt.Errorf("pgnotch: building the goose store: %w", err)
	}
	dir, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("pgnotch: reading the migrations: %w", err)
	}
	// The dialect is empty because a store was given, which is the only way to
	// name the version table. Registering nothing globally is what keeps a
	// caller's own goose migrations out of this schema, and out of its version
	// table.
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

// Migrate applies the schema to whatever schema the pool's search_path names,
// and is what [Open] refuses to do for itself. It is safe to run concurrently
// with itself and with a running [Store]: goose takes the version table's lock,
// and nothing a migration does touches a log's entry tables.
//
// It does not close pool.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	// Closing this leaves the pool open, and its own failure says nothing a
	// caller could act on: it hands connections back to a pool that outlives it.
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
// registry and goose's version table. It is what a test's teardown wants and
// what an operator retiring a deployment wants, and it is deliberately not
// [Migrate]'s down — this says the tables cease to exist, where a down says a
// schema version is rolled back. What they mean differs; what they destroy,
// while there is one migration, does not. See [Provider].
//
// The schema itself is left alone: this package did not create it and does not
// know whether anything else is in it. An operator giving the whole thing back
// wants DROP SCHEMA.
//
// A [Store] does not survive it: it caches which tables each log's entries are
// in, and after a Drop those names are either gone or a later log's. Drop what
// nothing is using, and open again afterwards.
func Drop(ctx context.Context, pool *pgxpool.Pool) error {
	tables, err := entryTables(ctx, pool)
	if err != nil {
		return err
	}
	tables = append(tables, registryTable, versionTable)
	// One statement, because dropping the registry before an entry table would
	// leave a failure part way through with tables nothing can enumerate.
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS `+strings.Join(tables, ", ")); err != nil {
		return fmt.Errorf("pgnotch: dropping the schema: %w", err)
	}
	return nil
}

// entryTables names both halves of every log's ring, from the registry.
//
// The registry is the complete list: a log's ordinal is assigned and its tables
// are created in one transaction, so a row without its tables is not a state
// this package can reach, and nothing ever deletes a row. A schema with no
// registry table has no logs, which is not an error to ask about.
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

// migrated reports whether the schema is there, with a read that creates
// nothing. goose can answer a richer question — whether every migration this
// build knows about has been applied — and answering it creates the version
// table, which is the one thing [Open] must not do.
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
