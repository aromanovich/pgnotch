package pgnotch_test

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aromanovich/pgnotch"
)

// TestAppendsWriteFewFullPageImages asserts two things about what an append
// writes, against the catalogue and the server's own counters:
//
//  1. the entry tables have no TOAST relation, so `bytea STORAGE PLAIN` is
//     keeping payloads in line rather than in a second table with a btree under
//     it, and nothing else here would notice it stopping;
//  2. full-page images cost a constant per checkpoint rather than a rate per
//     entry: an append-only table touches only the page it is filling, so a
//     checkpoint costs an image of that page and of its free-space map page,
//     and every entry after that costs none.
//
// `fillfactor = 10` is not in the schema for a measured reason: it forced one
// row per page, and with it REGBUF_WILL_INIT, but only moved the image from the
// heap page to the free-space map page at the same rate, while costing eight
// times the space for a 900-byte entry. Nothing goes red if it is put back —
// the assertions below are no greener with it and the space it costs is
// invisible to every other test here.
func TestAppendsWriteFewFullPageImages(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	// One connection, so every statement below runs in the same backend as the
	// appends: WAL counters accumulate per backend and are published only when
	// that backend is asked to flush them.
	schema := newSchema()
	store, pool := openStoreIn(t, schema, func(config *pgxpool.Config) { config.MaxConns = 1 })

	const id = pgnotch.LogID("images")
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))

	// 900 bytes is the end of the distribution where a page holds several
	// entries — where images could still be paid per entry, not per checkpoint.
	const rows = 512
	payload := make([]byte, 900)
	batch := make([][]byte, rows)
	for i := range batch {
		batch[i] = payload
	}
	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno, batch))

	table := entryTableIn(ctx, t, pool, schema)
	var toast int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT reltoastrelid::bigint FROM pg_class WHERE relname = $1`, table).Scan(&toast))
	require.Zerof(t, toast,
		"%s has a toast relation (%d), so a large payload can be moved out of line into a table "+
			"with a btree under it — the STORAGE PLAIN clause is not doing its job", table, toast)

	// The claim: a batch after a checkpoint pays for its page, not its entries.
	checkpoint(ctx, t, pool)
	before := walImages(ctx, t, pool)
	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno+rows, batch))
	batchImages := walImages(ctx, t, pool) - before

	// The instrument: one append per checkpoint does pay an image every time, so
	// a zero here would mean the batch's number proves nothing.
	const rounds = 6
	var perCheckpointImages int64
	for round := range pgnotch.Seqno(rounds) {
		checkpoint(ctx, t, pool)
		mark := walImages(ctx, t, pool)
		require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno+2*rows+round, [][]byte{payload}))
		perCheckpointImages += walImages(ctx, t, pool) - mark
	}

	require.GreaterOrEqualf(t, perCheckpointImages, int64(rounds/2),
		"%d appends, each after a checkpoint of its own, wrote %d full-page images: this guard's "+
			"instrument cannot see an image, so the batch's %d proves nothing",
		rounds, perCheckpointImages, batchImages)
	require.Lessf(t, batchImages, int64(rows/64),
		"appending %d entries after one checkpoint wrote %d full-page images — a rate per entry "+
			"rather than a constant per checkpoint, so the log is paying an image for pages it "+
			"should be filling without one", rows, batchImages)
}

func checkpoint(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `CHECKPOINT`)
	require.NoError(t, err, "the guard needs a checkpoint it can force")
}

// walImages reads the cluster's full-page-image counter, having first told this
// backend to publish what it has accumulated: without the flush a measurement
// reads its own past.
func walImages(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	_, err := pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
	require.NoError(t, err)
	var images int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT wal_fpi FROM pg_stat_wal`).Scan(&images))
	return images
}

// entryTableIn finds the half of the ring the appends went to, in a schema
// holding exactly one log that has not rotated. The schema is pinned rather than
// left to `current_schema()`: every run's tables carry the same name shape, so
// an unpinned query can match another run's table, which stays green with
// STORAGE PLAIN deleted.
func entryTableIn(ctx context.Context, t *testing.T, pool *pgxpool.Pool, schema string) string {
	t.Helper()
	var name string
	err := pool.QueryRow(ctx, `
		SELECT tablename FROM pg_tables
		 WHERE schemaname = $1 AND tablename LIKE $2
		 ORDER BY tablename LIMIT 1`, schema, entryTablePattern).Scan(&name)
	require.NoError(t, err, "the log's entry table is not in the catalogue")
	return name
}

// TestAppendsCostOneRoundTrip guards that an append is one statement — the
// registry row's UPDATE and the entry rows in one CTE — rather than a
// transaction around several: a BEGIN, an UPDATE, the rows and a COMMIT are four
// waits for the network and four times as long holding the row every writer of
// that log contends for. It is counted at the socket rather than through pgx,
// whose own view would count a statement it pipelines the same as one it does
// not.
//
// The claim is the second append; the first is the instrument, because it pays
// the extra round trip of preparing a statement the connection has not seen.
func TestAppendsCostOneRoundTrip(t *testing.T) {
	ctx := testContext(t, time.Minute)

	var trips atomic.Int64
	store, _ := openStoreIn(t, newSchema(), countingPool(&trips, 0))

	const id = pgnotch.LogID("round-trips")
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))

	entry := [][]byte{make([]byte, 900)}

	before := trips.Load()
	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno, entry))
	firstTrips := trips.Load() - before

	before = trips.Load()
	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno+1, entry))
	steadyTrips := trips.Load() - before

	require.Greaterf(t, firstTrips, int64(1),
		"the first append on this connection cost %d round trips, so it did not pay for preparing "+
			"a statement the connection had not seen: this guard's instrument cannot see a round "+
			"trip, and the %d below proves nothing", firstTrips, steadyTrips)
	require.Equalf(t, int64(1), steadyTrips,
		"an append cost %d round trips rather than one, so it is no longer a single statement — "+
			"a caller waits that many network times over, and the registry row is held across all "+
			"of them", steadyTrips)
}

// TestAppendsStayOneRoundTripAcrossLogs bounds the claim above, which is true
// of one log and not automatically true of a thousand.
//
// A connection round-robining across more logs than its statement cache holds
// misses on every append, and the single round trip becomes three. The fix is
// the DSN rather than this package.
//
// What decides the outcome is the ratio of logs to capacity, so eight and four
// reproduce at millisecond cost what a thousand and 512 do.
func TestAppendsStayOneRoundTripAcrossLogs(t *testing.T) {
	const logs = 8
	const starvedCache = 4

	starved := appendRoundTrips(t, logs, starvedCache)
	sized := appendRoundTrips(t, logs, 8*starvedCache)

	require.Equalf(t, int64(1), sized,
		"an append across %d logs cost %d round trips with a statement cache sized for them, "+
			"so something other than the cache is costing a round trip per append", logs, sized)
	require.Greaterf(t, starved, sized,
		"an append across %d logs cost %d round trips with a cache that holds %d statements and "+
			"%d with one that holds them all: this guard's instrument cannot see the cache thrash, "+
			"so it would not catch a deployment paying it", logs, starved, starvedCache, sized)
}

// appendRoundTrips is the round trips one steady-state append costs on a
// connection whose statement cache holds capacity statements, round-robining
// across logs. The first pass over them is discarded: it fills the cache.
func appendRoundTrips(t *testing.T, logs, capacity int) int64 {
	t.Helper()
	ctx := testContext(t, time.Minute)

	var trips atomic.Int64
	store, _ := openStoreIn(t, newSchema(), countingPool(&trips, capacity))

	const epoch = pgnotch.Epoch(1)
	entry := [][]byte{make([]byte, 900)}
	ids := make([]pgnotch.LogID, logs)
	for i := range ids {
		ids[i] = pgnotch.LogID(fmt.Sprintf("log-%d", i))
	}
	require.NoError(t, store.CreateLogs(ctx, ids...))
	for i := range ids {
		require.NoError(t, store.Fence(ctx, ids[i], epoch))
		require.NoError(t, store.Append(ctx, ids[i], epoch, pgnotch.FirstSeqno, entry))
	}

	before := trips.Load()
	for _, id := range ids {
		require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno+1, entry))
	}
	return (trips.Load() - before) / int64(logs)
}

// countingPool tunes a pool so that its round trips land in trips, over a single
// connection so that everything counted is the caller's. A capacity of zero
// leaves the driver's statement cache at its default.
func countingPool(trips *atomic.Int64, capacity int) func(*pgxpool.Config) {
	return func(config *pgxpool.Config) {
		config.MaxConns = 1
		if capacity > 0 {
			config.ConnConfig.StatementCacheCapacity = capacity
		}
		config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := new(net.Dialer).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &countingConn{Conn: conn, trips: trips}, nil
		}
	}
}

// countingConn counts round trips: a read that follows a write is a wait for
// the server, and reads that follow reads are the rest of one answer arriving.
// It sits under pgx's own buffering, because the driver decides how many
// protocol messages go into a flush and it is the flush a caller waits for.
//
// wrote is per connection and trips is per measurement, which is why only the
// second is a pointer.
type countingConn struct {
	net.Conn
	trips *atomic.Int64
	wrote atomic.Bool
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.wrote.Store(true)
	return c.Conn.Write(p)
}

func (c *countingConn) Read(p []byte) (int, error) {
	if c.wrote.Swap(false) {
		c.trips.Add(1)
	}
	return c.Conn.Read(p)
}
