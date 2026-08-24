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

// MaxEntryChunk is the payload one row carries: a heap tuple must fit a page
// (PostgreSQL's MaxHeapTupleSize is 8160), the row's other columns and headers
// take about fifty bytes, and the rest is slack. A larger entry becomes several
// rows, which a caller never sees — entries go in whole and come back whole.
const MaxEntryChunk = 8000

// Store is the logs in one PostgreSQL schema, whichever the pool's search_path
// names. It is safe for concurrent use, but two concurrent appends to the same
// log race for seqnos and lose. It does not own the pool and never closes it.
type Store struct {
	pool *pgxpool.Pool

	// appends caches each log's append statement; an ordinal is assigned once,
	// at creation, and never reused, so nothing invalidates. pgx prepares by text.
	mu      sync.RWMutex
	appends map[LogID]string
}

// Open returns a Store over the schema the pool's search_path names, which must
// already have been migrated: see [Migrate], and [ErrNotMigrated] for why this
// is not done here. It does not take ownership of pool.
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
// they are, ownership and entries included. A created log has no owner, so an
// [Store.Append] to it is refused with [ErrFenced] until someone fences it, and
// either every id in the batch exists when this returns nil, or none of the
// ones it had to create do.
//
// Creation is separate from fencing because a [LogID] is an arbitrary string: a
// fence that conjured a log would leave tables behind on one bad id.
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
	defer func() {
		if err != nil {
			err = fmt.Errorf("pgnotch: creating logs: %w", err)
		}
	}()

	// Registering a log and creating its tables are one transaction, and cannot
	// be done the other way round: the tables are named from the ordinal the row
	// hands out.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// DO NOTHING returns only the rows this statement inserted, so what comes
	// back is exactly the logs that did not exist. A concurrent creator of the
	// same id is waited for here and then leaves nothing to return.
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

	// This transaction holds each row returned, so no fence and no reclaim can
	// come between a row and its tables. It takes the rows first and the tables
	// second, the reverse of the order [Store.reclaim] calls load-bearing, and
	// that is safe only here: a log this statement created has no tables yet, so
	// no reclaim can be holding them. All the DDL is one statement: Exec with no
	// arguments goes through the simple protocol, which admits several.
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
	// Not warmed here: no append is reachable without a fence, which builds it.
	return tx.Commit(ctx)
}

// Fence claims a log for epoch, atomically cutting off every append of a lower
// epoch, so a writer that has lost the log cannot slip an append past a
// completed Fence. It is idempotent per epoch, so a restart without a change of
// ownership can replay the same acquire path; fencing at a higher epoch is how
// the same owner renews, and at a lower one fails with [ErrFenced].
//
// Ownership is all a fence changes: the entries stay and the new owner
// continues the log at the next seqno. A failed fence changes nothing, and
// fencing a log that does not exist is [ErrNoSuchLog] and creates nothing.
func (s *Store) Fence(ctx context.Context, id LogID, epoch Epoch) error {
	if err := checkFence(id, epoch); err != nil {
		return err
	}

	// The WHERE is the fence: an existing higher epoch leaves the row alone, and
	// `<=` rather than `<` is what makes fencing idempotent per epoch.
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

// fenceRefused says why the update matched no row.
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
// batch must hold at least one payload; an individual payload may be empty. The
// payloads stay the caller's: no slice is retained or read after Append
// returns. Returning nil means the batch is durable through its last seqno.
//
// Errors:
//   - [ErrFenced] when the log belongs to another epoch, or to nobody,
//   - [ErrAlreadyWritten] when any seqno in the batch is taken,
//   - [ErrGap] when the entry below first is missing.
//
// None of the three writes anything; where more than one applies [ErrFenced]
// wins, because [ErrAlreadyWritten] is an ack a lost writer would trust.
//
// It is one statement: an explicit transaction would hold the registry row
// across BEGIN, the UPDATE, the rows and COMMIT. COPY, the only path to
// heap_multi_insert, carries neither the predicate nor the refusal, so the rows
// go in by INSERT and pay a WAL record apiece rather than one per page — 41
// bytes a row more at 900-byte entries (~4%), nothing at page-sized ones, where
// a row is its own page.
func (s *Store) Append(ctx context.Context, id LogID, epoch Epoch, first Seqno, payloads [][]byte) error {
	// Malformed arguments outrank the context, so they are answered first.
	last, err := checkAppend(id, epoch, first, len(payloads))
	if err != nil {
		return err
	}
	sql, known, err := s.appendFor(ctx, id)
	if err != nil {
		return err
	}
	if !known {
		// A log nothing has fenced has no row and no tables to refuse it.
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
	// The UPDATE matched nothing, so neither INSERT had a slot and nothing went in.
	if tag.RowsAffected() == 0 {
		return s.appendRefused(ctx, id, epoch, first, last)
	}
	return nil
}

// appendSQL is the append, once per log because it names that log's tables. The
// UPDATE is three checks at once: the log is this epoch's, the entry below the
// batch is there, and its seqnos are free. Whichever fails, it matches no row,
// `(SELECT cur_slot FROM claim)` is NULL, both INSERTs get a false one-time
// filter, and the statement writes nothing.
//
// A second append waits on the UPDATE's lock, and under READ COMMITTED an
// UPDATE released from such a wait re-reads the row and applies the predicate
// again, so the loser matches nothing — which is what makes a seqno unwritable
// twice. Under REPEATABLE READ the same wait ends in 40001, indistinguishable
// here from any other driver error.
//
// Both halves of the ring are named because no statement can choose a table
// from a value it computes; the one that is not current inserts nothing but is
// still locked, which is why [Store.reclaim] locks the tables before the row.
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
// UPDATE did not match. That is a second snapshot, but both columns are
// monotonic — a fence cannot be undone, last_seqno never falls back — so it can
// only name a refusal outranking the one the UPDATE would have.
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
	// The row admits the append the statement did not make, and neither cause is
	// one of the three refusals: answering with one would tell the caller
	// something false about its own log.
	return fmt.Errorf("pgnotch: %q refused an append at seqno %d that its registry row admits: "+
		"the row moved under the refusal, or the log's entry tables are gone", id, first)
}

// chunkArrays lays the batch out as one array per column, so a batch of any
// size is three parameters: PostgreSQL's limit of 65535 is reachable at
// ordinary sizes, and a text that grew with the batch could not be reused.
func chunkArrays(first Seqno, payloads [][]byte) ([]int64, []int16, [][]byte) {
	// Counted, since a guess of one row per entry is short above one chunk.
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
// not zero of them: an entry with no rows would read back as absent.
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
// in seqno order. limit must be positive, and a from below [FirstSeqno] reads
// from [FirstSeqno]. Fewer than limit entries means the log ends there, so a
// caller reading a whole log loops until a short read.
//
// A log nothing has fenced reads as empty, and a read after a successful
// [Store.Fence] sees every entry the log held when the fence took it.
func (s *Store) ReadFrom(ctx context.Context, id LogID, from Seqno, limit int) ([]Entry, error) {
	from, err := checkRead(id, from, limit)
	if err != nil {
		return nil, err
	}

	st, found, err := s.readState(ctx, id)
	if err != nil || !found {
		return nil, err
	}

	// Below the watermark the entries are trimmed whether or not their rows are
	// still there. Seqnos are contiguous, so bounding the seqno bounds the count.
	lo := max(from, st.trimUpto+1)
	hi := lo + Seqno(limit) - 1

	// The ring's halves are disjoint — everything below cur_lo is in the other
	// one — so the union needs no bound of its own. They are named in slot order
	// so the text does not alternate as the ring turns and cost a re-prepare.
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

	// The scan targets are reused row after row, which the payloads survive: pgx
	// decodes every bytea into a slice of its own.
	var seqno, epoch int64
	var chunk int16
	var part []byte
	dest := []any{&seqno, &chunk, &epoch, &part}

	// Capped: limit is the caller's and may be far above what the log holds.
	entries := make([]Entry, 0, min(limit, 1024))
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("pgnotch: reading %q from %d: %w", id, from, err)
		}
		// Chunks arrive in order, so a non-zero chunk continues the entry above.
		if chunk == 0 {
			// A payload handed out is the caller's to keep, and a zero-length
			// one left nil would read as "no payload" on an empty entry.
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

// NextSeqno is the seqno the log's next append must start at: one past its last
// entry, and [FirstSeqno] for a log nothing has appended to. A log that does not
// exist is [ErrNoSuchLog].
//
// It is what a writer that has just fenced a log somebody else wrote needs and
// does not have. Reading the log for it works and is what a caller had to do
// before this existed, but it costs a round trip per page and the entry tables
// carry no index; this is one row, found by primary key, and it is right for a
// log whose entries a trim has all taken — which a read cannot be, there being
// nothing left to read.
//
// The value is the caller's to append at only while it owns the log: an append
// by a higher epoch moves it. That is not a race to lose, because it is not the
// mark that keeps two writers apart — an append at a stale seqno is refused
// ([ErrAlreadyWritten], [ErrGap]) rather than misplaced, and one from a fenced-
// out epoch is refused whatever seqno it names.
func (s *Store) NextSeqno(ctx context.Context, id LogID) (Seqno, error) {
	if err := checkLogID(id); err != nil {
		return 0, err
	}
	st, found, err := s.readState(ctx, id)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("pgnotch: the next seqno of %q: %w", id, ErrNoSuchLog)
	}
	return st.lastSeqno + 1, nil
}

// Trim removes the log's entries at or below upTo. Trimming entries that are
// not there is not an error: Trim states where the log should start, and
// repeating it is harmless. The log stays appendable at the next seqno,
// ownership stays put, and the seqnos removed stay spent — an append at one is
// [ErrAlreadyWritten].
//
// The watermark moves synchronously and the space comes back when it can: a
// read is gated on the watermark, so rows may outlive it, never their visibility.
func (s *Store) Trim(ctx context.Context, id LogID, upTo Seqno) error {
	proceed, err := checkTrim(id, upTo)
	if err != nil || !proceed {
		return err
	}
	// Clamped to the tail: a log trimmed to seqno n while empty must still return
	// an entry appended at n afterwards, which a watermark past the tail would
	// hide forever. GREATEST keeps repeated trims from moving it back.
	st, err := scanLogState(s.pool.QueryRow(ctx, `
		UPDATE `+registryTable+`
		   SET trim_upto = GREATEST(trim_upto, LEAST($1, last_seqno))
		 WHERE log_id = $2
		RETURNING `+logStateColumns,
		int64(upTo), string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		// A log nothing has fenced already starts where the trim says it should.
		return nil
	}
	if err != nil {
		return fmt.Errorf("pgnotch: trimming %q to %d: %w", id, upTo, err)
	}

	// Reclamation is best-effort: it takes an AccessExclusiveLock, and a Trim
	// that could not get it has still done what it promised.
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
	// The ordinal is here and immutable, so a writer that has just fenced and
	// read to the tail pays no lookup for its first append.
	s.remember(id, st.ordinal)
	return st, true, nil
}

// appendFor is a log's append statement, from the cache when it can be and from
// the registry when it cannot. A log that is not there yet is reported as
// not-found rather than as an error: "no such log" is a refusal to phrase.
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

// remember caches a log's append statement and returns it. The read under
// RLock first is not redundant: every fence of a log already owned arrives with
// its ordinal cached, and overwriting the entry with itself would take the
// exclusive lock against every concurrent append's read.
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
// when the live half has run far enough. Both steps run in one transaction, and
// the lock order is load-bearing: the tables first, the registry row second. An
// append names both halves and locks both before it reaches the row, so a
// reclaim taking the row first and then asking for a table would deadlock
// against every append in flight. The row taken second still keeps a rotation
// from changing the current slot under an append already given one.
//
// Which half to truncate is the row's answer and the row is read after the
// locks, so both halves are locked, in slot order against a second reclaim.
func (s *Store) reclaim(ctx context.Context, st logState) error {
	// A rotation writes to neither table, so only the truncate needs a lock.
	wantTruncate, wantRotate := st.reclaimSteps(true)
	if !wantTruncate && !wantRotate {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Waiting here would put a trim behind whatever holds the table.
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

	// Ask again under the row lock: another trim may have run since the read
	// above, and what the row says now is what decides.
	now, err := scanLogState(tx.QueryRow(ctx, `
		UPDATE `+registryTable+` SET trim_upto = trim_upto WHERE ordinal = $1
		RETURNING `+logStateColumns, st.ordinal))
	if err != nil {
		return err
	}

	// wantTruncate is the record that the tables above were locked. Both steps
	// come from one answer, so a rotation cannot follow an emptying that failed.
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
