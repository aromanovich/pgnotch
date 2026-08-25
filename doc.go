// Package pgnotch keeps append-only, fenced logs in stock PostgreSQL: no
// extension, no background worker, no server-side code of its own.
//
// A log is a gap-free sequence of entries under an identifier the caller
// chooses, created by [Store.CreateLogs] and never as a side effect. One writer
// owns it at a time, at an epoch [Store.Fence] takes, so a writer that has lost
// its log finds out at its next append. The owner assigns seqnos itself, which
// lets an append be a single statement.
//
// # What a caller may rely on
//
//  1. Total order per log, with no tie to break and no clock in the design.
//  2. Fencing. A completed [Store.Fence] cuts off every append of a lower
//     epoch, atomically.
//  3. Cumulative ack. An [Store.Append] that returns nil means every entry up
//     to and including the batch's last seqno is durable.
//  4. Gap-freedom. An append never skips a seqno: [Store.Trim] moves a log's
//     lower end, an append its upper end, and nothing puts a hole between.
//  5. Readback. [Store.ReadFrom] returns every entry a completed append acked
//     and no trim has removed, in seqno order, across a change of owner.
//  6. Handover. [Store.NextSeqno] names the seqno a new owner's first append
//     must start at, for a log whose entries a trim has all taken included.
//
// Payloads are opaque bytes; nothing here interprets or frames them.
//
// # How it works
//
// Per log there is one row in the registry table holding the owning epoch, the
// last seqno appended and the trim watermark. An append is one statement: an
// UPDATE whose WHERE clause is at once the fencing check, the gap check and
// the already-written check, decided under that row's lock, with the entry
// rows in the same CTE. Nothing is derived from the entry rows, so a trim can
// take all of them while [ErrAlreadyWritten] still answers for their seqnos.
//
// The entry tables carry no index: the registry row is the authority on which
// seqnos are spent, and `_bt_check_unique` reads under SnapshotDirty, so an
// index could not see the rows a trim removed. Nor a fillfactor — measured, it
// only moved the full-page image to the free-space map page, at eight times
// the space for a 900-byte entry. The payload column is `bytea STORAGE PLAIN`,
// which keeps PostgreSQL from ever creating a TOAST relation for these tables,
// at the price of a hard "row is too big" past a page, so entries are chunked
// at [MaxEntryChunk] and a caller never sees a chunk.
//
// Space comes back by TRUNCATE, never by DELETE: each log has two entry tables
// used as a ring, and the one a trim has emptied is truncated, discarding its
// dirty buffers unwritten and taking its whole vacuum debt with it.
//
// # Operating it
//
// An append is one round trip only while the driver has the statement prepared.
// It names its own log's tables, so a connection holds one per log it has
// touched; a smaller driver cache misses every append and the round trip
// becomes three. pgx caches 512, set in the DSN by `statement_cache_capacity`.
//
// [Migrate] applies the static half of the schema and [Open] refuses one nobody
// has migrated. The migrations are ordinary goose SQL files, embedded here and
// shipped in `migrations/`; [Provider] hands back the goose provider itself,
// with the warning it carries about the down. The per-log entry tables are not
// versioned and cannot be — [Store.CreateLogs] makes them in the transaction
// that registers each log, and a migration over an unbounded set of tables is
// not something this package has.
//
// Every name this package writes is unqualified, so all of them land in the
// schema the connection's search_path names: two [Store]s over one schema are
// two writers of the same logs, two over different schemas share nothing.
//
// Requires PostgreSQL 16 or newer: `bytea STORAGE PLAIN` in CREATE TABLE,
// rather than a following ALTER, arrived there.
package pgnotch
