// Package pgnotch keeps append-only, fenced logs in stock PostgreSQL: no
// extension, no background worker, no server-side code of its own.
//
// A log is a gap-free sequence of entries under an identifier the caller
// chooses. It is created by [Store.CreateLogs] and never by anything else: a
// log is two PostgreSQL tables, an identifier is an arbitrary string, and a
// call that made one on demand would turn a bad id into tables nothing gives
// back. One writer owns it at a time. Ownership is an epoch:
// [Store.Fence] takes a log at an epoch and cuts off every append of a lower
// one, so a writer that has lost its log finds out at its next append rather
// than writing over its successor. The owner assigns sequence numbers itself,
// which is what lets an append be a single statement and the order be total
// with nothing coordinating it.
//
// # What a caller may rely on
//
//  1. Total order per log. The owner assigns seqnos, so there is no
//     tie to break and no clock in the design.
//  2. Fencing. A completed [Store.Fence] cuts off every append of a lower
//     epoch, atomically.
//  3. Cumulative ack. An [Store.Append] that returns nil means every entry up
//     to and including the batch's last seqno is durable, so "confirmed ⟺
//     seqno ≤ the last acked" is inherited rather than implemented.
//  4. Gap-freedom. An append never skips a seqno, so a log is one unbroken
//     run: [Store.Trim] moves its lower end, an append its upper end, and
//     nothing puts a hole in the middle.
//  5. Readback. [Store.ReadFrom] returns every entry a completed append acked
//     and no trim has removed, in seqno order — including across a change of
//     owner, which moves the log's ownership and nothing else.
//
// Payloads are opaque bytes. Nothing here interprets, compresses or frames
// them, and nothing here decides what a log is *for*.
//
// # The registry row decides everything
//
// Per log there is one row in the registry table holding the owning epoch, the
// last seqno appended and the trim watermark. An append is one UPDATE whose
// WHERE clause is simultaneously the fencing check, the gap check and the
// already-written check, so all three are decided atomically under that row's
// lock; the three refusals are told apart by re-reading the row, and only when
// the UPDATE matched nothing. Nothing is derived from the entry rows, which is
// what lets a trim take all of them while [ErrAlreadyWritten] still answers for
// the seqnos it removed.
//
// That UPDATE and the rows are one statement, not a transaction around two. An
// append is therefore a single round trip that holds the registry row for one
// server-side execution, which is the whole of what a second writer of that log
// waits behind; [Store.Append] has what the alternative costs.
//
// One round trip is a claim about the connection pool as much as about this
// package, and whoever builds the pool owns the other half of it. The statement
// names its own log's tables, so a connection holds one prepared statement per
// log it has touched; a driver cache smaller than that misses on every append
// and the round trip becomes three. With pgx that cache is 512 by default and
// `statement_cache_capacity` in the DSN is where it is set.
//
// # The entry tables carry no index and no TOAST
//
// The writer assigns seqnos, so there is nothing to look up by identity — and a
// unique index on one would be worse than useless. It would read as the thing
// enforcing that a seqno is written once, and it is not: `_bt_check_unique`
// reads under SnapshotDirty, and the rows a trim has removed are not there to
// be seen, so a re-append into the trimmed prefix passes it. The registry row
// is the authority on which seqnos are spent, and an index would be a second
// authority that disagrees with it exactly where it matters.
//
// The payload column is declared `bytea STORAGE PLAIN`, which is what keeps
// PostgreSQL from ever creating a TOAST relation (and the btree under it) for
// these tables, at the price of a hard "row is too big" for anything that would
// not fit a page — so entries are chunked here instead, at [MaxEntryChunk]. A
// caller never sees the chunks: an entry goes in whole and comes back whole.
//
// Full-page images cost a constant per checkpoint rather than a rate per entry,
// and the table needs no option to arrange it. An append-only table only ever
// touches the page it is filling, so after a checkpoint has passed under it,
// exactly that page and its free-space map page carry an image and every
// further row on it carries none. A page-sized row lands on a freshly extended
// page, which PostgreSQL marks REGBUF_WILL_INIT — implying REGBUF_NO_IMAGE,
// which suppresses the image before the decision consults the checkpoint at
// all.
//
// `fillfactor = 10` on the entry tables looks like the way to force that
// condition for smaller entries too, and it is deliberately absent: measured,
// it removed the heap page's image and the free-space map's took its place at
// the same rate, so it bought nothing and cost eight times the space for a
// 900-byte entry.
//
// # Space comes back by TRUNCATE, never by DELETE
//
// Each log has two entry tables used as a ring, and the one a trim has emptied
// is TRUNCATEd — which discards its dirty buffers without writing them, resets
// its freeze horizon and takes its whole vacuum debt with it. A generation that
// dies before the next checkpoint never reaches the disk at all. The registry
// table is the only one in the design with real vacuum debt, and the only one
// whose full-page images are paid per checkpoint rather than amortised, which
// is why its rows are packed onto few pages and why it carries a fillfactor
// while the entry tables do not.
//
// # The schema, and which half of it goose owns
//
// The static half is versioned: [Migrate] applies it and records it in a goose
// version table of this package's own, and [Open] refuses a schema nobody has
// migrated rather than conjuring tables from under a caller. The migrations are
// ordinary goose SQL files, embedded here and shipped in `migrations/`, so the
// goose CLI applies the same directory against the same DSN. [Provider]
// hands back the goose provider itself for an operator who wants `status`, a
// targeted `up-to` or a `down` — with the warning [Provider] carries, that the
// down takes every log's entries with it and cannot do otherwise.
//
// The per-log entry tables are not versioned and cannot be: there is one pair
// per log and the set of logs is the caller's, so they are created by
// [Store.CreateLogs], inside the transaction that registers each log. A row in
// the registry therefore always has its tables, and a migration never has to
// know how many logs exist. What this costs is worth stating plainly: a schema
// change to the entry tables is a migration this package would have to apply to
// an unbounded set of tables, and there is no such migration today.
//
// It is also why creating a log is a call rather than a side effect. Tables are
// the one resource here that is unbounded, that no caller can be talked out of
// spending, and that nothing reclaims on its own — so the moment one is made is
// a moment a caller asked for, and [Store.Fence] answers [ErrNoSuchLog] rather
// than guessing.
//
// # Where the tables go
//
// Every name this package writes is unqualified, so all of them land in
// whatever schema the connection's search_path names — which is the whole of
// what separates one deployment's logs from another's in the same database, and
// is PostgreSQL's own mechanism rather than one invented here. Point the DSN at
// a schema of this deployment's own:
//
//	postgres://…/db?search_path=orders
//
// Two [Store]s over one schema are two writers of the same logs, which is what
// a failover looks like from here; two over different schemas share nothing.
//
// # Requirements
//
// PostgreSQL 16 or newer: `bytea STORAGE PLAIN` in CREATE TABLE, rather than a
// following ALTER, arrived there.
package pgnotch
