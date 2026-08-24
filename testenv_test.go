package pgnotch_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aromanovich/pgnotch"
)

// envDSN names the database this package is tested against. There is no default
// on purpose: a suite that reaches a database somebody did not name can truncate
// tables somebody cared about.
const envDSN = "PGNOTCH_DSN"

// TestMain refuses to run this package's tests at all without a database: there
// is no configuration in which `go test ./...` is green having never spoken to
// PostgreSQL.
//
// The tables in rules_test.go need no database and run anyway, under the same
// rule: they are cases the driver tests cannot reach, not a suite that may stand
// in for a run.
func TestMain(m *testing.M) {
	if os.Getenv(envDSN) == "" {
		fmt.Fprintf(os.Stderr,
			"%s is not set, so there is no database to test against.\n"+
				"Point it at a PostgreSQL 16 or newer, e.g.\n"+
				"    %s=postgres://user:password@localhost:5432/db go test ./...\n",
			envDSN, envDSN)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// entryTablePattern matches a log's entry tables as a LIKE pattern. The
// underscores are escaped because LIKE reads a bare one as "any character".
const entryTablePattern = `pgnotch\_entries\_%`

// schemaCounter keeps schema names unique within one process; with the pid it
// is unique across concurrent runs as well.
var schemaCounter atomic.Int64

func newSchema() string {
	return fmt.Sprintf("pgnotch_t%d_%d", os.Getpid(), schemaCounter.Add(1))
}

// openStore is a migrated schema of this test's own, dropped when it ends.
func openStore(t *testing.T) *pgnotch.Store {
	t.Helper()
	store, _ := openStoreIn(t, newSchema(), nil)
	return store
}

// openStoreIn is [openStore] over a schema the caller named, and it hands the
// pool back. The pool is tunable because the cost guards cannot take one as
// given: one counts round trips and needs a DialFunc under the connection,
// another needs a statement cache of a stated size, and the full-page-image
// guard needs a single connection because the counters it reads are per backend.
func openStoreIn(t *testing.T, schema string, tune func(*pgxpool.Config)) (*pgnotch.Store, *pgxpool.Pool) {
	t.Helper()
	pool := openPoolIn(t, schema, tune)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.NoError(t, pgnotch.Migrate(ctx, pool))
	store, err := pgnotch.Open(ctx, pool)
	require.NoError(t, err)
	return store, pool
}

// openPoolIn is a pool whose every connection resolves unqualified names in a
// schema of this test's own, created here and dropped when the test ends — which
// is what keeps concurrent tests and consecutive runs out of each other's
// tables. The schema can be created through this same pool because naming it in
// `search_path` before it exists is allowed, and `CREATE SCHEMA` names its own
// target rather than resolving one.
func openPoolIn(t *testing.T, schema string, tune func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv(envDSN)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("unable to parse %s=%q: %v", envDSN, dsn, err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	if tune != nil {
		tune(config)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		// Fatal rather than a skip: the DSN names a database, and one that
		// cannot be reached is a broken run, not an absent one.
		t.Fatalf("no PostgreSQL at %s: %v\nit must be 16 or newer", dsn, err)
	}

	// Registered before anything is written: a test that fails part way through
	// still owes the schema's tables back.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`); err != nil {
			t.Errorf("unable to drop the schema %s: %v", schema, err)
		}
		pool.Close()
	})

	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+schema); err != nil {
		t.Fatalf("unable to create the schema %s: %v", schema, err)
	}
	return pool
}

// testContext is a deadline every test gets, so that a database that stops
// answering fails the run rather than hanging it.
func testContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
