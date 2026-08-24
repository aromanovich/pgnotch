package pgnotch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxEntryChunk is the payload one row carries. A heap tuple must fit a page
// (PostgreSQL's MaxHeapTupleSize is 8160) and the row's own columns and headers
// take roughly fifty bytes of that; the rest is left as slack so that the
// number is chosen rather than derived to the last byte.
//
// An entry larger than this becomes several rows, and a caller never learns of
// it: entries go in whole and come back whole, at any size PostgreSQL can hold.
const MaxEntryChunk = 8000

// Store is the logs in one PostgreSQL schema, which is whichever the pool's
// search_path names. It is safe for concurrent use, which is not a licence for
// two writers of one log: concurrent appends to the same log race for seqnos
// and lose.
//
// It does not own the pool it is given and never closes it.
type Store struct {
	pool *pgxpool.Pool

	// appends holds each log's append statement, and is a cache of an immutable
	// derivation rather than state: an ordinal is assigned once, when the log is
	// created, and nothing ever changes or reuses it — so the tables named
	// from it and the statement naming those tables are as fixed as it is.
	//
	// What it buys is the single round trip an append is meant to be, since
	// without it every append would look its own table name up first. Caching
	// the statement rather than the ordinal is the same argument one step on:
	// the text is rebuilt per append otherwise, and it is also the key pgx
	// looks its own prepared statement up by.
	mu      sync.RWMutex
	appends map[LogID]string
}

// Open returns a Store over the schema the pool's search_path names, which must
// already have been migrated: see [Migrate], and [ErrNotMigrated] for why this
// is not done here.
//
// It does not take ownership of pool.
func Open(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	ready, err := migrated(ctx, pool)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, fmt.Errorf("pgnotch: opening: %w — apply it with pgnotch.Migrate", ErrNotMigrated)
	}
	return &Store{pool: pool, appends: map[LogID]string{}}, nil
}

// CreateLogs brings logs into existence, and is the only thing here that ever
// creates a table. It is idempotent: ids that already exist are left exactly as
// they are, ownership and entries included, so a process may run it on every
// start over its whole set.
//
// Creating a log is separate from fencing it because a log is two PostgreSQL
// tables, and tables are not a resource to hand out on a caller's typo. A
// [LogID] is an arbitrary string; if a fence conjured a log then one bad id —
// a wrong tenant, an unescaped input, a retry loop with a counter in it — would
// leave tables behind at whatever rate it was called, and nothing in PostgreSQL
// gives them back on its own. Where the set of ids is genuinely bounded and the
// caller knows the bound, calling this ahead of the fences is the shape to
// reach for.
//
// A created log has no owner: it is fenced by nobody, so an [Store.Append] to
// it is refused with [ErrFenced] exactly as one to a log nobody created is, and
// it holds no entries until an owner writes them.
//
// Either every id in the batch exists when this returns nil, or none of the
// ones it had to create do.
func (s *Store) CreateLogs(ctx context.Context, ids ...LogID) (err error) {
	if len(ids) == 0 {
		return nil
	}
	names := make([]string, len(ids))
	for i, id := range ids {
		if err := checkLogID(id); err != nil {
			return err
		}
		names[i] = string(id)
	}
	// Registered here rather than at each site below, and after the argument
	// checks, whose errors already say everything and say it about one id.
	defer func() {
		if err != nil {
			err = fmt.Errorf("pgnotch: creating logs: %w", err)
		}
	}()

	// Registering a log and creating its tables are one transaction, which is
	// what makes "there is a row" and "there are tables" the same fact. They
	// cannot be done the other way round — the tables are named from the
	// ordinal the row hands out — and a row without tables would be a log that
	// every later call finds half-built.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// DO NOTHING returns the rows this statement inserted and no others, so
	// what comes back is exactly the logs that did not exist — and over a set
	// that is already there, nothing, which is what makes re-running this cost
	// no DDL at all. A concurrent creator of the same id is waited for here and
	// then leaves nothing to return, having created the tables itself.
	rows, err := tx.Query(ctx, `
		INSERT INTO `+registryTable+`
			(log_id, epoch, last_seqno, trim_upto, cur_slot, cur_lo, prev_hi)
		SELECT unnest($1::text[]), 0, 0, 0, 0, $2, 0
		ON CONFLICT (log_id) DO NOTHING
		RETURNING ordinal`, names, int64(FirstSeqno))
	if err != nil {
		return err
	}
	ordinals, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}

	// The row of each of these is held by this transaction, so no fence and no
	// reclaim of the same log can be between the row and its tables. The lock
	// order [Store.reclaim] is careful about cannot come apart here either: a
	// reclaim takes the entry tables and then the registry row, and a log this
	// statement created is one no reclaim has ever heard of.
	//
	// All of the DDL is one statement because provisioning a whole set is what
	// this call is for: a shard range is thousands of logs, and a round trip
	// apiece would be two thousand waits where the batch is one. Exec with no
	// arguments goes through the simple protocol, which is what admits several
	// statements in one.
	if len(ordinals) > 0 {
		var ddl strings.Builder
		for _, ordinal := range ordinals {
			for _, table := range ringTables(ordinal) {
				fmt.Fprintf(&ddl, `
					CREATE TABLE IF NOT EXISTS %s (
						seqno   bigint   NOT NULL,
						chunk   smallint NOT NULL,
						epoch   bigint   NOT NULL,
						payload bytea    STORAGE PLAIN NOT NULL
					) WITH (autovacuum_enabled = false);`, table)
			}
		}
		if _, err := tx.Exec(ctx, ddl.String()); err != nil {
			return err
		}
	}
	// The append statements these logs will want are not built here. Nothing
	// can reach one without fencing first, and a fence derives it from the
	// ordinal it gets back anyway — so warming the cache over a whole shard
	// range would only retain a statement per log for logs this process may
	// never own.
	return tx.Commit(ctx)
}

// Fence claims a log for epoch, atomically cutting off every append of a lower
// epoch, so a writer that has lost the log cannot slip an append past a
// completed Fence.
//
// It is idempotent per epoch, so a process restart without a change of
// ownership can replay the same acquire path. Fencing at a higher epoch is how
// the same owner renews; fencing at a lower one fails with [ErrFenced].
//
// A fence changes ownership and nothing else: the entries stay, and the new
// owner continues the log at the next seqno rather than starting one. A fence
// that fails changes nothing, ownership included.
//
// The log must exist: a fence of one that does not is [ErrNoSuchLog] and
// creates nothing. See [Store.CreateLogs] for why that is not this call's job.
//
// It is one statement, and so one round trip, because a log that exists has its
// tables already — there is no DDL here to wrap a transaction around.
func (s *Store) Fence(ctx context.Context, id LogID, epoch Epoch) error {
	if err := checkFence(id, epoch); err != nil {
		return err
	}

	// The WHERE is the fence: an existing epoch above this one leaves the row
	// alone and returns nothing, and `<=` rather than `<` is what makes fencing
	// idempotent per epoch.
	var ordinal int64
	err := s.pool.QueryRow(ctx, `
		UPDATE `+registryTable+` SET epoch = $2
		 WHERE log_id = $1 AND epoch <= $2
		RETURNING ordinal`,
		string(id), int64(epoch)).Scan(&ordinal)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.fenceRefused(ctx, id, epoch)
	}
	if err != nil {
		return fmt.Errorf("pgnotch: fencing %q at epoch %d: %w", id, epoch, err)
	}
	s.remember(id, ordinal)
	return nil
}

// fenceRefused says why the update matched no row, which is a log that is not
// there, the fence this package refuses, or a state it cannot be in.
func (s *Store) fenceRefused(ctx context.Context, id LogID, epoch Epoch) error {
	var held int64
	err := s.pool.QueryRow(ctx,
		`SELECT epoch FROM `+registryTable+` WHERE log_id = $1`,
		string(id)).Scan(&held)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %q, create it with CreateLogs before fencing it", ErrNoSuchLog, id)
	}
	if err != nil {
		return fmt.Errorf("pgnotch: reading the owner of %q after a refused fence: %w", id, err)
	}
	if err := fenceRefusal(id, Epoch(held), epoch); err != nil {
		return err
	}
	return fmt.Errorf("pgnotch: %q refused an admitted fence at epoch %d", id, epoch)
}

// Append writes payloads as entries at consecutive seqnos starting at first,
// under epoch, as one atomic unit. first must be at least [FirstSeqno] and the
// batch must hold at least one payload; an individual payload may be empty.
//
// The payloads stay the caller's: this package retains no slice and reads none
// after Append returns, whatever it returns, so an encoder's scratch buffer may
// be reused as soon as the call does.
//
// Returning nil means every entry up to and including the batch's last seqno is
// durable, so the caller's high-water mark becomes that seqno.
//
// Errors:
//   - [ErrFenced] when the log belongs to another epoch, or to nobody,
//   - [ErrAlreadyWritten] when any seqno in the batch is taken,
//   - [ErrGap] when the entry below first is missing.
//
// None of the three writes anything. Where more than one applies [ErrFenced]
// wins, because [ErrAlreadyWritten] is an ack and a writer that has lost its
// log would read it as one.
//
// The whole append is one statement, which is the latency decision the shape of
// everything under it is arranged around. An explicit transaction spends a
// round trip on BEGIN, one on the UPDATE, one or more on the rows and one on
// COMMIT, and holds the registry row — the row every writer of this log
// contends for — across all of them; one statement is one round trip and holds
// that row for the length of a single server-side execution.
//
// It is bought, and what it costs is heap_multi_insert. Only COPY reaches that
// path, and COPY can carry neither the predicate above nor the refusal it has
// to return, so the rows go in through INSERT and pay a WAL record apiece
// rather than one per page: measured, 41 bytes a row more at 900-byte entries
// (~4%) and nothing at all at page-sized ones, where a row is its own page and
// there is nothing to collapse. Three round trips for four percent of the small
// end of the distribution is the trade this package exists to make.
func (s *Store) Append(ctx context.Context, id LogID, epoch Epoch, first Seqno, payloads [][]byte) error {
	// Malformed arguments outrank the context, so they are answered before
	// anything reaches the pool.
	last, err := checkAppend(id, epoch, first, len(payloads))
	if err != nil {
		return err
	}
	sql, known, err := s.appendFor(ctx, id)
	if err != nil {
		return err
	}
	if !known {
		// A log nothing has fenced has no row and no tables, and the refusal
		// is the same one the statement below would have produced.
		return appendRefusal(id, epoch, first, last, appendState{})
	}
	seqnos, chunks, parts := chunkArrays(first, payloads)

	tag, err := s.pool.Exec(ctx, sql,
		int64(last), string(id), int64(epoch), int64(first-1), seqnos, chunks, parts)
	if isUndefinedTable(err) {
		return s.appendRefused(ctx, id, epoch, first, last)
	}
	if err != nil {
		return fmt.Errorf("pgnotch: appending to %q: %w", id, err)
	}
	// The UPDATE matched nothing, so neither INSERT had a slot to match and the
	// statement wrote no rows at all.
	if tag.RowsAffected() == 0 {
		return s.appendRefused(ctx, id, epoch, first, last)
	}
	return nil
}

// appendSQL is the append, once per log because it names that log's tables.
//
// The UPDATE is three checks at once: the log is this epoch's, the entry below
// the batch is there, and the batch's seqnos are free. Whichever fails, it
// matches no row, `(SELECT cur_slot FROM claim)` is NULL, both INSERTs are left
// with a false one-time filter, and the statement's own atomicity is what makes
// "the registry row is unchanged" and "no rows were written" the same fact.
//
// The lock is the one the UPDATE takes; a SELECT ... FOR UPDATE would be that
// same lock a round trip earlier. A second append to this log waits on it, and
// under READ COMMITTED an UPDATE released from such a wait re-reads the row and
// applies the predicate again — so the loser sees the winner's last_seqno and
// matches nothing, or sees the winner's rollback and proceeds. That re-check is
// what makes a seqno unwritable twice, and it is READ COMMITTED's alone: under
// REPEATABLE READ the same wait ends in 40001, which is neither a refusal nor
// something this package can distinguish from any other driver error.
//
// Both halves of the ring are named because the one to write to is the UPDATE's
// answer, and no statement can choose a table from a value it computes. The
// half that is not current inserts nothing — but it is still locked, which is
// the whole of why [Store.reclaim] takes its table lock before the registry row
// rather than after.
func (s *Store) appendSQL(ordinal int64) string {
	return `
		WITH claim AS (
			UPDATE ` + registryTable + ` SET last_seqno = $1
			 WHERE log_id = $2 AND epoch = $3 AND last_seqno = $4
			RETURNING cur_slot
		), parts AS (
			SELECT * FROM unnest($5::bigint[], $6::smallint[], $7::bytea[]) AS t(seqno, chunk, payload)
		), slot0 AS (
			INSERT INTO ` + entriesTable(ordinal, 0) + ` (seqno, chunk, epoch, payload)
			SELECT seqno, chunk, $3, payload FROM parts WHERE (SELECT cur_slot FROM claim) = 0
		), slot1 AS (
			INSERT INTO ` + entriesTable(ordinal, 1) + ` (seqno, chunk, epoch, payload)
			SELECT seqno, chunk, $3, payload FROM parts WHERE (SELECT cur_slot FROM claim) = 1
		)
		SELECT cur_slot FROM claim`
}

// appendRefused says which of the three the append was, by reading the row the
// UPDATE did not match.
//
// It is a second statement and so a second snapshot rather than the UPDATE's.
// Both columns it reads are monotonic — a fence cannot be undone and last_seqno
// never falls back — so all a later snapshot can add is a refusal outranking
// the one the UPDATE would have named, which is the direction the order wants.
// Turning a gap into an already-written would take a second writer appending at
// this epoch, which is the one thing a fence exists to prevent.
func (s *Store) appendRefused(ctx context.Context, id LogID, epoch Epoch, first, last Seqno) error {
	var have struct {
		epoch     int64
		lastSeqno int64
	}
	err := s.pool.QueryRow(ctx,
		`SELECT epoch, last_seqno FROM `+registryTable+` WHERE log_id = $1`,
		string(id)).Scan(&have.epoch, &have.lastSeqno)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return appendRefusal(id, epoch, first, last, appendState{})
	case err != nil:
		return fmt.Errorf("pgnotch: appending to %q: %w", id, err)
	}
	found := appendState{owner: Epoch(have.epoch), written: Seqno(have.lastSeqno)}
	if refusal := appendRefusal(id, epoch, first, last, found); refusal != nil {
		return refusal
	}
	// The row admits the append the statement did not make, and neither way of
	// arriving here is one of the refusals: either the row moved between the
	// statement and this read, or the statement never reached its UPDATE
	// because the log's entry tables are not there. Answering with a refusal
	// would tell the caller something false about its own log — a gap it does
	// not have, or a seqno it did not write.
	return fmt.Errorf("pgnotch: %q refused an append at seqno %d that its registry row admits: "+
		"the row moved under the refusal, or the log's entry tables are gone", id, first)
}

// chunkArrays lays the batch out as one array per column, which is what lets a
// batch of any size be three parameters rather than four per row: PostgreSQL's
// limit of 65535 parameters is reachable at ordinary batch sizes, and a
// statement whose text grew with the batch could not be a prepared statement
// the connection reuses.
func chunkArrays(first Seqno, payloads [][]byte) ([]int64, []int16, [][]byte) {
	// Counted rather than guessed at one row per entry: a batch of entries
	// above one chunk would make that guess short and pay for it by growing
	// all three slices.
	rows := 0
	for _, payload := range payloads {
		rows += max(1, (len(payload)+MaxEntryChunk-1)/MaxEntryChunk)
	}
	seqnos := make([]int64, 0, rows)
	chunks := make([]int16, 0, rows)
	parts := make([][]byte, 0, rows)
	for i, payload := range payloads {
		seqno := int64(first + Seqno(i))
		for chunk, part := range chunksOf(payload) {
			seqnos = append(seqnos, seqno)
			chunks = append(chunks, int16(chunk))
			parts = append(parts, part)
		}
	}
	return seqnos, chunks, parts
}

// chunksOf splits a payload into rows. An empty payload is one empty chunk and
// not zero of them: an empty entry is admitted, and an entry with no rows would
// read back as an entry that is not there.
func chunksOf(payload []byte) [][]byte {
	if len(payload) <= MaxEntryChunk {
		return [][]byte{payload}
	}
	parts := make([][]byte, 0, (len(payload)+MaxEntryChunk-1)/MaxEntryChunk)
	for off := 0; off < len(payload); off += MaxEntryChunk {
		parts = append(parts, payload[off:min(off+MaxEntryChunk, len(payload))])
	}
	return parts
}

// ReadFrom returns up to limit entries of the log with seqno at or above from,
// in seqno order. limit must be positive.
//
// A from below [FirstSeqno] reads from [FirstSeqno]. Fewer than limit entries
// means the log ends there, so a caller reading a whole log loops until a short
// read. A log nothing has fenced reads as an empty one.
//
// A read that follows a successful [Store.Fence] sees every entry the log held
// when the fence took it: a new owner is handed the log rather than a shorter
// one it would append over.
func (s *Store) ReadFrom(ctx context.Context, id LogID, from Seqno, limit int) ([]Entry, error) {
	from, err := checkRead(id, from, limit)
	if err != nil {
		return nil, err
	}

	st, found, err := s.readState(ctx, id)
	if err != nil || !found {
		return nil, err
	}

	// Both ends of the range are the log's own: below the watermark the entries
	// are trimmed whether or not their rows are still there, and above
	// from+limit-1 they are not asked for. Seqnos are contiguous, so bounding
	// the seqno bounds the number of entries.
	lo := max(from, st.trimUpto+1)
	hi := lo + Seqno(limit) - 1

	// The ring's two halves are disjoint by construction — everything below
	// cur_lo is in the other one — so the union needs no bound of its own, and
	// which of them is current does not change the answer once the ORDER BY has
	// run. They are named in slot order for that reason: current-half-first
	// would give a log two statement texts that alternate as the ring turns, and
	// so cost a connection a second cache entry and a re-prepare per rotation.
	tables := ringTables(st.ordinal)
	rows, err := s.pool.Query(ctx, `
		SELECT seqno, chunk, epoch, payload FROM `+tables[0]+`
		 WHERE seqno BETWEEN $1 AND $2
		UNION ALL
		SELECT seqno, chunk, epoch, payload FROM `+tables[1]+`
		 WHERE seqno BETWEEN $1 AND $2
		ORDER BY seqno, chunk`, int64(lo), int64(hi))
	if err != nil {
		return nil, fmt.Errorf("pgnotch: reading %q from %d: %w", id, from, err)
	}
	defer rows.Close()

	// The scan targets are reused row after row, which the payloads survive:
	// pgx decodes every bytea into a slice of its own, so what a row hands over
	// is nobody else's memory even though the pointer it arrived through is.
	var seqno, epoch int64
	var chunk int16
	var part []byte
	dest := []any{&seqno, &chunk, &epoch, &part}

	// Capped, because limit is the caller's: a limit of a million over a log of
	// three entries would otherwise allocate for the million.
	entries := make([]Entry, 0, min(limit, 1024))
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("pgnotch: reading %q from %d: %w", id, from, err)
		}
		// Chunks arrive in order behind their entry, so a chunk that is not the
		// first continues the entry the previous row opened.
		if chunk == 0 {
			// A payload handed out must be the caller's to keep, and a
			// zero-length one left nil would read as "no payload" where an
			// empty entry was appended.
			if part == nil {
				part = []byte{}
			}
			entries = append(entries, Entry{Seqno: Seqno(seqno), Epoch: Epoch(epoch), Payload: part})
			continue
		}
		if len(entries) == 0 || entries[len(entries)-1].Seqno != Seqno(seqno) {
			return nil, fmt.Errorf("pgnotch: %q has chunk %d of seqno %d without its first",
				id, chunk, seqno)
		}
		tail := &entries[len(entries)-1]
		tail.Payload = append(tail.Payload, part...)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgnotch: reading %q from %d: %w", id, from, err)
	}
	return entries, nil
}

// Trim removes the log's entries at or below upTo, and is how a log stays
// small: a reader of a log that is never trimmed pays for every entry ever
// written to it.
//
// Trimming entries that are not there is not an error: Trim states where the
// log should start, and repeating it is harmless. Whatever upTo says, the log
// stays appendable at the next seqno and ownership stays put.
//
// The seqnos it removed stay spent: an append at one is refused with
// [ErrAlreadyWritten] and writes nothing. Handing a trimmed seqno out again
// would put a hole in the log and ack a high-water mark below entries the log
// still holds.
//
// The watermark moves synchronously and the space comes back when it can: what
// a read returns is gated on the watermark, so the rows may outlive it. What
// cannot outlive it is their visibility.
func (s *Store) Trim(ctx context.Context, id LogID, upTo Seqno) error {
	proceed, err := checkTrim(id, upTo)
	if err != nil || !proceed {
		return err
	}
	// The watermark is clamped to the tail, and that is not tidiness: a trim
	// names entries to remove, and on a log that never held them it removes
	// nothing — so a log trimmed to seqno n while empty must still accept and
	// return an entry appended at n afterwards. A watermark past the tail would
	// hide it forever. GREATEST keeps repeated trims harmless, LEAST keeps them
	// from reaching past what was written.
	st, err := scanLogState(s.pool.QueryRow(ctx, `
		UPDATE `+registryTable+`
		   SET trim_upto = GREATEST(trim_upto, LEAST($1, last_seqno))
		 WHERE log_id = $2
		RETURNING `+logStateColumns,
		int64(upTo), string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		// Trimming a log nothing has fenced is not an error: Trim states where
		// the log should start, and a log with no entries already starts there.
		return nil
	}
	if err != nil {
		return fmt.Errorf("pgnotch: trimming %q to %d: %w", id, upTo, err)
	}

	// Reclamation is best-effort by design. It takes an AccessExclusiveLock,
	// which a backup or a pg_dump may hold off, and a Trim that failed for that
	// reason has still done what it promised.
	if err := s.reclaim(ctx, st); err != nil && !errors.Is(err, errLockUnavailable) {
		return fmt.Errorf("pgnotch: trimming %q to %d: %w", id, upTo, err)
	}
	return nil
}

func (s *Store) readState(ctx context.Context, id LogID) (logState, bool, error) {
	st, err := scanLogState(s.pool.QueryRow(ctx,
		`SELECT `+logStateColumns+` FROM `+registryTable+` WHERE log_id = $1`,
		string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return logState{}, false, nil
	}
	if err != nil {
		return logState{}, false, fmt.Errorf("pgnotch: reading the state of %q: %w", id, err)
	}
	// The ordinal is here and immutable, so a writer recovering a log it has
	// just fenced — read to the tail, then append — pays no lookup for the
	// first append.
	s.remember(id, st.ordinal)
	return st, true, nil
}

// appendFor is a log's append statement, from the cache when it can be and from
// the registry when it cannot. The second answer is remembered: the ordinal it
// is built from is assigned once and never changes, so there is nothing to
// invalidate.
//
// A log that is not there yet is reported rather than an error, because "no
// such log" is a refusal for the caller to phrase and not a failure.
func (s *Store) appendFor(ctx context.Context, id LogID) (string, bool, error) {
	s.mu.RLock()
	sql, ok := s.appends[id]
	s.mu.RUnlock()
	if ok {
		return sql, true, nil
	}

	var ordinal int64
	err := s.pool.QueryRow(ctx,
		`SELECT ordinal FROM `+registryTable+` WHERE log_id = $1`,
		string(id)).Scan(&ordinal)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("pgnotch: looking up %q: %w", id, err)
	}
	return s.remember(id, ordinal), true, nil
}

// remember derives what a log's ordinal is worth deriving once, and hands the
// result to whoever caused the derivation.
//
// The lookup first is not the caller's job to do: every fence of a log already
// owned arrives here with an ordinal that is already cached, and building the
// statement again to overwrite it with itself would rebuild ~600 bytes and take
// the exclusive lock against every concurrent append's read.
func (s *Store) remember(id LogID, ordinal int64) string {
	s.mu.RLock()
	sql, ok := s.appends[id]
	s.mu.RUnlock()
	if ok {
		return sql
	}
	sql = s.appendSQL(ordinal)
	s.mu.Lock()
	s.appends[id] = sql
	s.mu.Unlock()
	return sql
}

var errLockUnavailable = errors.New("pgnotch: the table lock was not available")

// reclaim empties the half of the ring a trim has passed, and turns the ring
// when the live half has run far enough.
//
// Both steps run inside one transaction, and the order of the two locks it
// takes is load-bearing: the tables first and the registry row second. An
// append names both halves of the ring and so locks both before it reaches the
// registry row, so a reclaim that took the row first and then asked for a table
// would be the other half of a deadlock with every append in flight. Holding
// the table lock first costs those appends the length of a TRUNCATE and nothing
// more, while the registry row taken second is still what keeps a rotation from
// changing the current slot under an append that has already been given one.
//
// Which half a truncate will name is that same row's answer, and the row is
// read after the locks — so both halves are locked, in slot order so that two
// reclaims of one log cannot deadlock against each other. Locking the half that
// is not truncated costs nothing on top: an append names both, so an ACCESS
// EXCLUSIVE on either already stands in front of every append of this log.
func (s *Store) reclaim(ctx context.Context, st logState) error {
	// What the ring wants is [logState]'s answer; everything below is about
	// when it may have it. A rotation writes to neither table and so needs no
	// table lock, which is why only the truncate decides whether one is taken.
	wantTruncate, wantRotate := st.reclaimSteps(true)
	if !wantTruncate && !wantRotate {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Waiting here would put a trim behind whatever holds the table, and the
	// point of the whole design is that a trim never blocks the log.
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '250ms'`); err != nil {
		return err
	}

	if wantTruncate {
		if _, err := tx.Exec(ctx, `LOCK TABLE `+
			strings.Join(ringTables(st.ordinal), `, `)+
			` IN ACCESS EXCLUSIVE MODE`); err != nil {
			if isLockNotAvailable(err) {
				return errLockUnavailable
			}
			return err
		}
	}

	// Ask again under the row lock: another trim may have run between the read
	// above and this transaction, and what the row says now is what decides.
	now, err := scanLogState(tx.QueryRow(ctx, `
		UPDATE `+registryTable+` SET trim_upto = trim_upto WHERE ordinal = $1
		RETURNING `+logStateColumns, st.ordinal))
	if err != nil {
		return err
	}

	// What may be done to the row as it stands, by something holding what this
	// transaction holds — `wantTruncate` being the record that the tables above
	// were locked. Both steps come from one answer so that a rotation can never
	// follow an emptying that did not happen.
	doTruncate, doRotate := now.reclaimSteps(wantTruncate)
	if doTruncate {
		if _, err := tx.Exec(ctx,
			`TRUNCATE TABLE `+entriesTable(now.ordinal, now.prevSlot())); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE `+registryTable+` SET prev_hi = 0 WHERE ordinal = $1`,
			now.ordinal); err != nil {
			return err
		}
	}

	if doRotate {
		if _, err := tx.Exec(ctx, `
			UPDATE `+registryTable+`
			   SET cur_slot = 1 - cur_slot, prev_hi = last_seqno, cur_lo = last_seqno + 1
			 WHERE ordinal = $1`, now.ordinal); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func isLockNotAvailable(err error) bool { return sqlStateIs(err, "55P03") }
func isUndefinedTable(err error) bool   { return sqlStateIs(err, "42P01") }

func sqlStateIs(err error, state string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == state
}
