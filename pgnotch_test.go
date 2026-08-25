package pgnotch_test

import (
	"bytes"
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aromanovich/pgnotch"
)

// TestOpenRefusesASchemaNobodyMigrated pins the error identity: the refusal is
// [pgnotch.ErrNotMigrated], not a relation-does-not-exist three calls later,
// because a caller that wants the schema created matches on it and calls Migrate.
func TestOpenRefusesASchemaNobodyMigrated(t *testing.T) {
	ctx := testContext(t, time.Minute)

	// A schema nothing has migrated: the tables are what Open refuses the
	// absence of.
	pool := openPoolIn(t, newSchema(), nil)

	store, err := pgnotch.Open(ctx, pool)
	require.Nil(t, store)
	require.ErrorIs(t, err, pgnotch.ErrNotMigrated)
}

// TestALogIdIsAnyStringTheCallerLikes: a log's identifier is the caller's, and
// nothing about it reaches a table name. The ids below are the ones a naming
// scheme would get wrong, which is why a log's tables are named from an ordinal.
func TestALogIdIsAnyStringTheCallerLikes(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, time.Minute)

	ids := []pgnotch.LogID{
		"42",
		"7f3c1b9e-1d2a-4c5e-8f00-0123456789ab",
		"7f3c1b9e.1d2a.4c5e.8f00.0123456789ab",
		"tenant/acme:orders",
		"日本語のログ",
		pgnotch.LogID(strings.Repeat("x", pgnotch.MaxLogIDBytes)),
	}
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, ids...))
	for _, id := range ids {
		require.NoErrorf(t, store.Fence(ctx, id, epoch), "fencing %q", id)
		require.NoErrorf(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno, [][]byte{[]byte(id)}),
			"appending to %q", id)
	}
	for _, id := range ids {
		entries, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
		require.NoErrorf(t, err, "reading %q", id)
		require.Lenf(t, entries, 1, "%q holds somebody else's entries", id)
		require.Equalf(t, []byte(id), entries[0].Payload, "%q came back with another log's entry", id)
	}

	// The two an id cannot be, refused where they are passed rather than at
	// whatever finally overflows.
	require.Error(t, store.Fence(ctx, "", epoch))
	require.Error(t, store.Fence(ctx, pgnotch.LogID(strings.Repeat("x", pgnotch.MaxLogIDBytes+1)), epoch))
}

// TestTheThreeRefusalsAreToldApart: a writer that has lost its log re-appending
// a batch it never got an answer for is refused as fenced and not as
// already-written, because [pgnotch.ErrAlreadyWritten] is an ack and it would
// take its successor's word for its own high-water mark.
func TestTheThreeRefusalsAreToldApart(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, time.Minute)

	const id = pgnotch.LogID("refusals")
	const held, taken = pgnotch.Epoch(4), pgnotch.Epoch(5)
	entry := [][]byte{[]byte("an entry")}

	// Created and owned by nobody is a state of its own, and still
	// [pgnotch.ErrFenced]: a log with no owner is not this caller's to write.
	require.NoError(t, store.CreateLogs(ctx, id))
	require.ErrorIs(t, store.Append(ctx, id, held, pgnotch.FirstSeqno, entry), pgnotch.ErrFenced)

	require.NoError(t, store.Fence(ctx, id, held))
	require.ErrorIs(t, store.Append(ctx, id, held, pgnotch.FirstSeqno+1, entry), pgnotch.ErrGap)

	require.NoError(t, store.Append(ctx, id, held, pgnotch.FirstSeqno, entry))
	require.ErrorIs(t, store.Append(ctx, id, held, pgnotch.FirstSeqno, entry), pgnotch.ErrAlreadyWritten)

	require.NoError(t, store.Fence(ctx, id, taken))
	require.ErrorIs(t, store.Append(ctx, id, held, pgnotch.FirstSeqno, entry), pgnotch.ErrFenced)

	// An epoch *above* the log's is refused as flatly, and that direction is the
	// easy one to leave open: a writer becomes the owner by fencing, and an
	// append is not a fence. Relax the append's `epoch = $3` to `epoch <= $3` and
	// every other assertion here stays green.
	require.ErrorIs(t, store.Append(ctx, id, taken+1, pgnotch.FirstSeqno+1, entry), pgnotch.ErrFenced)

	// Nothing any of those refusals touched is in the log.
	entries, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Equal(t, []pgnotch.Entry{{Seqno: pgnotch.FirstSeqno, Epoch: held, Payload: []byte("an entry")}}, entries)
}

// TestAFenceReachesAnotherStore is the half of fencing a single Store cannot
// state: two [pgnotch.Store]s over one schema are two processes owning a log in
// turn, so the refusal must come from the database and not from a value one of
// them holds. This package keeps no per-log state in the process — every decision
// is the registry row's. Delete `AND epoch = $3` from the append and this goes red.
func TestAFenceReachesAnotherStore(t *testing.T) {
	incumbent, pool := openStoreIn(t, newSchema(), nil)
	ctx := testContext(t, time.Minute)

	successor, err := pgnotch.Open(ctx, pool)
	require.NoError(t, err)

	const id = pgnotch.LogID("handed-over")
	const held, taken = pgnotch.Epoch(4), pgnotch.Epoch(5)

	require.NoError(t, incumbent.CreateLogs(ctx, id))
	require.NoError(t, incumbent.Fence(ctx, id, held))
	require.NoError(t, incumbent.Append(ctx, id, held, pgnotch.FirstSeqno,
		[][]byte{[]byte("the incumbent's entry")}))

	require.NoError(t, successor.Fence(ctx, id, taken))

	err = incumbent.Append(ctx, id, held, pgnotch.FirstSeqno+1, [][]byte{[]byte("a zombie's entry")})
	require.ErrorIs(t, err, pgnotch.ErrFenced,
		"the ex-owner, which nobody told, appended at the epoch it still believes it holds")

	// The successor holds no mark of its own and asks the log for one, which is
	// what NextSeqno is for: a fence takes ownership and moves nothing else.
	next, err := successor.NextSeqno(ctx, id)
	require.NoError(t, err)
	require.Equal(t, pgnotch.FirstSeqno+1, next)

	// The successor inherits the log rather than starting one: it reads what the
	// incumbent was acked for and nothing the zombie wrote.
	require.NoError(t, successor.Append(ctx, id, taken, next,
		[][]byte{[]byte("the successor's entry")}))
	entries, err := successor.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Equal(t, []pgnotch.Entry{
		{Seqno: pgnotch.FirstSeqno, Epoch: held, Payload: []byte("the incumbent's entry")},
		{Seqno: pgnotch.FirstSeqno + 1, Epoch: taken, Payload: []byte("the successor's entry")},
	}, entries)
}

// TestAnEntryLargerThanAPageSurvivesTheRoundTrip is what chunking is for. The
// payload column is `bytea STORAGE PLAIN`, so PostgreSQL refuses an oversized
// row rather than moving the value out of line; remove chunking and the append
// fails with "row is too big". A silent TOAST would have made this pass and put
// a btree under the log.
func TestAnEntryLargerThanAPageSurvivesTheRoundTrip(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, 2*time.Minute)

	const id = pgnotch.LogID("big-entries")
	const epoch = pgnotch.Epoch(3)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))

	// Deterministic, so a failure is reproducible from the seed alone. The sizes
	// straddle a chunk in both directions, and the megabyte is the tail that
	// would otherwise only be met in production.
	random := rand.New(rand.NewSource(20260823))
	sizes := []int{0, 1, 500, pgnotch.MaxEntryChunk, pgnotch.MaxEntryChunk + 1, 20 << 10, 1 << 20}
	for range 8 {
		sizes = append(sizes, 500+random.Intn(20<<10-500))
	}

	want := make([]pgnotch.Entry, 0, len(sizes))
	for i, size := range sizes {
		payload := make([]byte, size)
		random.Read(payload)
		seqno := pgnotch.FirstSeqno + pgnotch.Seqno(i)
		require.NoErrorf(t, store.Append(ctx, id, epoch, seqno, [][]byte{payload}),
			"appending an entry of %d bytes", size)
		want = append(want, pgnotch.Entry{Seqno: seqno, Epoch: epoch, Payload: payload})
	}

	got, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, len(sizes))
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range want {
		require.Equalf(t, want[i].Seqno, got[i].Seqno, "entry %d came back at the wrong seqno", i)
		require.Equalf(t, want[i].Epoch, got[i].Epoch, "entry %d came back at the wrong epoch", i)
		require.Truef(t, bytes.Equal(want[i].Payload, got[i].Payload),
			"entry %d of %d bytes came back as %d bytes", i, len(want[i].Payload), len(got[i].Payload))
	}
}

// TestALogRotatesAndKeepsAnswering drives the ring past a rotation, trimming as
// it goes so the log stays short. It goes red if rotation loses the half of the
// ring a read has to union in, or if the trim watermark stops gating reads.
func TestALogRotatesAndKeepsAnswering(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, 2*time.Minute)

	const id = pgnotch.LogID("rotating")
	const epoch = pgnotch.Epoch(2)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))

	// Two full rotations' worth, in batches, trimming a long way behind the head
	// so the ring's other half is reclaimable by the time it is needed.
	const batch = 256
	const total = 10 * 1024
	payload := bytes.Repeat([]byte("x"), 900)
	for base := pgnotch.Seqno(0); base < total; base += batch {
		payloads := make([][]byte, batch)
		for i := range payloads {
			payloads[i] = payload
		}
		require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno+base, payloads))
		if base >= 2*batch {
			require.NoError(t, store.Trim(ctx, id, pgnotch.FirstSeqno+base-2*batch))
		}
	}

	// The log starts just above the last watermark and runs to the head with no
	// holes. The last trim of the loop names the entry below the first live one.
	firstLive := pgnotch.FirstSeqno + total - 3*batch + 1
	entries, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 16)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "the log came back empty after rotating")
	require.Equalf(t, firstLive, entries[0].Seqno,
		"the log starts at %d rather than just above the trim watermark", entries[0].Seqno)
	for i := range entries {
		require.Equal(t, firstLive+pgnotch.Seqno(i), entries[i].Seqno, "the log has a hole in it")
		require.Equal(t, payload, entries[i].Payload)
	}

	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno+total,
		[][]byte{[]byte("after the rotations")}))
	err = store.Append(ctx, id, epoch, pgnotch.FirstSeqno, [][]byte{[]byte("into the trimmed prefix")})
	require.ErrorIs(t, err, pgnotch.ErrAlreadyWritten,
		"a seqno a trim removed was handed out again, which is a hole in a log that promises none")
}

// TestDropLeavesTheEntryTablesOfNoLogBehind: [pgnotch.Drop] finds the tables it
// removes through the registry, so an unbounded set of per-log tables goes with
// it and nothing is left holding disk that no later call could name.
func TestDropLeavesTheEntryTablesOfNoLogBehind(t *testing.T) {
	ctx := testContext(t, time.Minute)
	schema := newSchema()
	store, pool := openStoreIn(t, schema, nil)

	require.NoError(t, store.CreateLogs(ctx, "one", "two", "three"))
	require.Equal(t, 6, tablesLike(ctx, t, pool, schema, entryTablePattern),
		"three logs should hold two entry tables each")

	require.NoError(t, pgnotch.Drop(ctx, pool))

	require.Zero(t, tablesLike(ctx, t, pool, schema, `pgnotch\_%`),
		"Drop left tables behind that nothing can enumerate any more")
}

func tablesLike(ctx context.Context, t *testing.T, pool *pgxpool.Pool, schema, pattern string) int {
	t.Helper()
	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*)::int FROM pg_tables WHERE schemaname = $1 AND tablename LIKE $2`,
		schema, pattern).Scan(&count))
	return count
}

// TestFencingALogNobodyCreatedMakesNoTables pins what the refusal on its own
// does not: the catalogue holds the same tables afterwards.
func TestFencingALogNobodyCreatedMakesNoTables(t *testing.T) {
	ctx := testContext(t, time.Minute)
	schema := newSchema()
	store, pool := openStoreIn(t, schema, nil)

	before := tablesLike(ctx, t, pool, schema, entryTablePattern)
	for _, id := range []pgnotch.LogID{"typo", "tenant/../etc", "999999"} {
		err := store.Fence(ctx, id, 1)
		require.ErrorIsf(t, err, pgnotch.ErrNoSuchLog, "fencing %q", id)
		_, err = store.NextSeqno(ctx, id)
		require.ErrorIsf(t, err, pgnotch.ErrNoSuchLog, "the next seqno of %q", id)
	}
	require.Equalf(t, before, tablesLike(ctx, t, pool, schema, entryTablePattern),
		"a refused fence left entry tables behind, so a bad id is a table nobody asked for")
}

// TestCreateLogsIsIdempotent is what lets a process run CreateLogs over its
// whole set on every start: the second pass leaves the first pass's logs as they
// are, ownership and entries included.
func TestCreateLogsIsIdempotent(t *testing.T) {
	ctx := testContext(t, time.Minute)
	schema := newSchema()
	store, pool := openStoreIn(t, schema, nil)

	ids := []pgnotch.LogID{"a", "b", "c"}
	require.NoError(t, store.CreateLogs(ctx, ids...))
	require.NoError(t, store.Fence(ctx, "a", 1))
	require.NoError(t, store.Append(ctx, "a", 1, pgnotch.FirstSeqno, [][]byte{[]byte("written before")}))
	tables := tablesLike(ctx, t, pool, schema, entryTablePattern)

	// Again, over a set that overlaps the first only partly.
	require.NoError(t, store.CreateLogs(ctx, append(ids, "d")...))

	require.Equal(t, tables+2, tablesLike(ctx, t, pool, schema, entryTablePattern),
		"a second CreateLogs made tables for logs that already had them")
	entries, err := store.ReadFrom(ctx, "a", pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "a second CreateLogs took an existing log's entries with it")
	require.NoError(t, store.Append(ctx, "a", 1, pgnotch.FirstSeqno+1, [][]byte{[]byte("after")}),
		"a second CreateLogs reset an existing log's ownership")
}

// TestMigrateIsIdempotent is the property a deploy depends on: every process may
// run Migrate, and the second must find nothing to do rather than rebuild the
// schema under the first.
func TestMigrateIsIdempotent(t *testing.T) {
	ctx := testContext(t, time.Minute)
	store, pool := openStoreIn(t, newSchema(), nil)

	const id = pgnotch.LogID("survivor")
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))
	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno, [][]byte{[]byte("written before")}))

	require.NoError(t, pgnotch.Migrate(ctx, pool))

	entries, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "a second Migrate took the log with it")
}

// TestTrimOfALogThatHeldNothingLeavesItAppendable is the case the arithmetic
// gets wrong if the watermark is not clamped to the tail. A trim names entries
// to remove, so on a log that never held them it removes nothing — and a
// watermark left past the tail hides the entry appended there afterwards
// forever, from a log that reports itself empty and accepts the append.
func TestTrimOfALogThatHeldNothingLeavesItAppendable(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, time.Minute)

	const id = pgnotch.LogID("trimmed-while-empty")
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))
	require.NoError(t, store.Trim(ctx, id, 1000))

	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno, [][]byte{[]byte("after the trim")}))
	entries, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the entry appended after the trim is hidden by the trim's watermark")
	require.Equal(t, []byte("after the trim"), entries[0].Payload)
}

func TestTheNextSeqnoIsWhereAnAppendMustGo(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, time.Minute)

	const id = pgnotch.LogID("where-next")
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))

	next, err := store.NextSeqno(ctx, id)
	require.NoError(t, err)
	require.Equal(t, pgnotch.FirstSeqno, next, "a log nothing has appended to starts at the first seqno")

	require.NoError(t, store.Append(ctx, id, epoch, next, [][]byte{[]byte("a"), []byte("b")}))
	next, err = store.NextSeqno(ctx, id)
	require.NoError(t, err)
	require.Equal(t, pgnotch.FirstSeqno+2, next)

	// It took no mark from the append it just made, so it is the whole of what a
	// writer needs to go on appending.
	require.NoError(t, store.Append(ctx, id, epoch, next, [][]byte{[]byte("c")}))
}

// TestTheNextSeqnoOutlivesTheEntries is the case a read cannot answer: a log
// every entry of which a trim has taken reads as empty, so a successor sizing it
// up by reading would start again at the first seqno and be refused all the way
// back to where it should have started.
func TestTheNextSeqnoOutlivesTheEntries(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, time.Minute)

	const id = pgnotch.LogID("trimmed-to-the-end")
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))
	require.NoError(t, store.Append(ctx, id, epoch,
		pgnotch.FirstSeqno, [][]byte{[]byte("a"), []byte("b"), []byte("c")}))
	require.NoError(t, store.Trim(ctx, id, pgnotch.FirstSeqno+2))

	entries, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Empty(t, entries, "everything the log held has been trimmed")

	next, err := store.NextSeqno(ctx, id)
	require.NoError(t, err)
	require.Equal(t, pgnotch.FirstSeqno+3, next)
	require.NoError(t, store.Append(ctx, id, epoch, next, [][]byte{[]byte("d")}),
		"the seqno it names is the one the log admits")
}

// TestAPayloadIsNobodyElsesMemory is the aliasing promise no caller can check
// for itself: the batch passed in is not retained, and the payloads handed back
// are the caller's to keep.
func TestAPayloadIsNobodyElsesMemory(t *testing.T) {
	store := openStore(t)
	ctx := testContext(t, time.Minute)

	const id = pgnotch.LogID("aliasing")
	const epoch = pgnotch.Epoch(1)
	require.NoError(t, store.CreateLogs(ctx, id))
	require.NoError(t, store.Fence(ctx, id, epoch))

	// The caller's buffer is reused for the next entry, which is what an encoder
	// with a scratch buffer does.
	scratch := []byte("the first entry ")
	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno, [][]byte{scratch}))
	copy(scratch, "OVERWRITTEN     ")
	require.NoError(t, store.Append(ctx, id, epoch, pgnotch.FirstSeqno+1, [][]byte{scratch}))

	entries, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, []byte("the first entry "), entries[0].Payload,
		"the log kept the caller's slice and read it again after Append returned")

	for i := range entries[0].Payload {
		entries[0].Payload[i] = '!'
	}
	again, err := store.ReadFrom(ctx, id, pgnotch.FirstSeqno, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("the first entry "), again[0].Payload,
		"writing into a payload the log handed out changed what the next read returns")
}
