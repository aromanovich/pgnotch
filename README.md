# pgnotch

Append-only, fenced logs in stock PostgreSQL. No extension, no background
worker, no server-side code — a table, a row, and one statement per append.

```sh
go get github.com/aromanovich/pgnotch
```

A log is a gap-free sequence of entries under an identifier you choose. One
writer owns it at a time, and ownership is an epoch: fencing a log at an epoch
cuts off every append of a lower one, so a writer that has lost the log finds
out at its next append instead of writing over its successor. The owner assigns
sequence numbers itself, which is what lets an append be a single round trip and
the order be total with nothing coordinating it.

```go
// Every name this package writes is unqualified, so it lands in the schema the
// connection's search_path names. That is the whole of how one deployment's
// logs are kept apart from another's in the same database.
pool, err := pgxpool.New(ctx, "postgres://…/db?search_path=orders")
if err != nil {
    return err
}
defer pool.Close()

if err := pgnotch.Migrate(ctx, pool); err != nil {
    return err
}
store, err := pgnotch.Open(ctx, pool)
if err != nil {
    return err
}

const shipments = pgnotch.LogID("shipments")

// Create the log. This is the only call that ever makes a table, and it is
// idempotent — run it over your whole set of logs at start-up. A log you
// never created cannot be fenced: you get ErrNoSuchLog.
if err := store.CreateLogs(ctx, shipments); err != nil {
    return err
}

// Claim it. The epoch comes from whatever hands out ownership — a lease
// counter, a ZooKeeper czxid, a database sequence — and must be strictly
// greater on each new claim.
if err := store.Fence(ctx, shipments, epoch); err != nil {
    return err
}

// Append at consecutive seqnos. Returning nil means every entry up to the
// batch's last is durable.
err = store.Append(ctx, shipments, epoch, pgnotch.FirstSeqno, [][]byte{
    []byte("first"),
    []byte("second"),
})
switch {
case errors.Is(err, pgnotch.ErrFenced):
    return fmt.Errorf("the log is somebody else's now: %w", err)
case errors.Is(err, pgnotch.ErrAlreadyWritten):
    // The batch is already there — this is the ack for a previous attempt
    // that failed ambiguously, not a failure.
case err != nil:
    return err
}

// The next batch starts where this one ended: seqnos are always the caller's
// to assign, and there is no "append at the end". The owner keeps its own
// high-water mark; the three outcomes above are this call's as well.
err = store.Append(ctx, shipments, epoch, pgnotch.FirstSeqno+2, [][]byte{
    []byte("third"),
})

entries, err := store.ReadFrom(ctx, shipments, pgnotch.FirstSeqno, 100)
```

A writer that has just fenced a log somebody else wrote does not have that mark,
and `NextSeqno` is what hands it over: one registry row by primary key, and the
seqno the next append must start at. Reading the log for it works too and is
what a caller had to do before that existed, but it is a round trip per page
over tables that carry no index, and it cannot answer for a log whose entries a
trim has all taken — there is nothing left to read, and starting again at the
first seqno is not where that log goes. A replay after an
ambiguous append is the other case that looks like a new seqno and is not —
resend *the same* batch at the same seqno and read `ErrAlreadyWritten` as the
ack.

Requires PostgreSQL **16 or newer** (`bytea STORAGE PLAIN` in `CREATE TABLE`
arrived there) and [`pgx/v5`](https://github.com/jackc/pgx).

## What you can rely on

1. **Total order per log.** The owner assigns seqnos, so there is no tie to
   break and no clock in the design.
2. **Fencing.** A completed `Fence` cuts off every append of a lower epoch,
   atomically.
3. **Cumulative ack.** An `Append` that returns nil means every entry up to and
   including the batch's last seqno is durable, so "confirmed ⟺ seqno ≤ the last
   acked" is inherited rather than implemented.
4. **Gap-freedom.** An append never skips a seqno, so a log is one unbroken run:
   `Trim` moves its lower end, an append its upper end, and nothing puts a hole
   in the middle.
5. **Readback.** `ReadFrom` returns every entry a completed append acked and no
   trim has removed, in seqno order — across a change of owner included.

Payloads are opaque bytes. Nothing here interprets, compresses or frames them,
and nothing here decides what a log is *for*.

## The three refusals

`Append` has exactly three outcomes a caller is expected to handle, none of
which writes anything:

| error | means | what to do |
|---|---|---|
| `ErrFenced` | the log belongs to another epoch, or to nobody | stop writing; you are not the owner |
| `ErrAlreadyWritten` | a seqno in the batch is taken | if this is a replay of the same batch, it is the ack |
| `ErrGap` | the entry below the batch is missing | retry once the predecessor lands |

Where more than one applies, **`ErrFenced` wins**. `ErrAlreadyWritten` is an
ack, and a writer that has already lost its log would otherwise take its
successor's word for its own high-water mark.

That ranking is what makes the retry rule safe: after an append that failed
ambiguously — a dropped connection, a cancelled context — replay *the same
batch* and read `ErrAlreadyWritten` as "it landed". A batch that overlaps the
log only partly is refused whole and is a caller bug, and a writer whose epoch
grew across the ambiguity must replay under the epoch it holds now.

## How it works

**The registry row decides everything.** Per log there is one row holding the
owning epoch, the last seqno appended and the trim watermark. An append is one
`UPDATE` whose `WHERE` clause is simultaneously the fencing check, the gap check
and the already-written check, so all three are decided atomically under that
row's lock. Nothing is derived from the entry rows — which is what lets a trim
take all of them while `ErrAlreadyWritten` still answers for the seqnos it
removed.

**Two appends to one log serialise on that row, and PostgreSQL picks the
winner.** The check is the `UPDATE`'s own `WHERE`, so the lock is the one that
`UPDATE` takes and no `SELECT … FOR UPDATE` is issued anywhere. The second
append waits on it, and under READ COMMITTED an `UPDATE` released from that
wait re-reads the row it waited for and applies the predicate again: if the
winner committed, `last_seqno` has moved and the loser matches nothing, which
is its `ErrAlreadyWritten`; if the winner rolled back, the predicate holds again
and the loser proceeds as though it had been alone. Uniqueness is therefore a
property of that one row, which is why the entry tables can carry no index and
still not admit a seqno twice.

**An append is one statement and one round trip.** The `UPDATE` and the entry
rows are a single CTE, not a transaction around two. A transaction would spend a
round trip on `BEGIN`, one on the `UPDATE`, one on the rows and one on `COMMIT`,
and hold the row every writer of that log contends for across all four.

**The entry tables carry no index and no TOAST.** The writer assigns seqnos, so
there is nothing to look up by identity — and a unique index would be worse than
useless: `_bt_check_unique` reads under `SnapshotDirty`, and rows a trim has
removed are not there to be seen, so a re-append into the trimmed prefix would
pass it. The payload column is `bytea STORAGE PLAIN`, which keeps PostgreSQL
from ever building a TOAST relation and the btree under it; entries larger than
a page are chunked here instead and reassembled on read, so a caller never sees
a chunk.

**Full-page images cost a constant per checkpoint, not a rate per entry.** An
append-only table only ever touches the page it is filling, so after a
checkpoint has passed under it exactly that page and its free-space map page
carry an image and every further row carries none.

**Space comes back by `TRUNCATE`, never by `DELETE`.** Each log has two entry
tables used as a ring, and the half a trim has emptied is truncated — which
discards its dirty buffers without writing them, resets its freeze horizon and
takes its whole vacuum debt with it. A generation that dies before the next
checkpoint never reaches the disk at all.

## Operating it

**Migrations are goose, and they are plain SQL files.** They live in
[`migrations/`](migrations/) and are embedded, so `Migrate` applies them with
nothing installed; `Open` refuses a schema nobody has migrated
(`ErrNotMigrated`) rather than conjuring tables from under you.

Nothing in the files is this package's to supply, so the goose CLI applies the
very same directory against the very same DSN:

```sh
goose -dir migrations -table pgnotch_migrations \
      postgres "postgres://…/db?search_path=orders" up
```

`-table` is what keeps this schema's version out of `goose_db_version`, so your
own migrations can share a schema with these without the two disagreeing about
what version it is at. `status`, `up-to` and `down` work the same way. In
process, `Provider` hands back the goose provider itself:

```go
db := stdlib.OpenDBFromPool(pool) // closing this leaves the pool open
defer db.Close()

provider, err := pgnotch.Provider(db)
status, err := provider.Status(ctx)
```

**The down is not a gentler `Drop`.** There is one migration and it is the
registry, which is also the only thing that can enumerate the entry tables — so
rolling it back has to take every log's entries with it, or strand them where
nothing could ever name them again. `goose down` destroys exactly what `Drop`
does, and leaves goose's version table behind. If you meant "roll back a schema
change", there is not one yet; if you meant "these logs are finished", use
`Drop` — or drop the schema.

**Half the schema is not versioned, and cannot be.** There is one pair of entry
tables per log and you may create logs forever, so they are created by
`CreateLogs`, inside the transaction that registers each log — a registry row
always has its tables. The cost: a schema change to the entry tables would be a
migration against an unbounded set of tables, and there is no such migration
today.

**Size the driver's statement cache for your logs.** An append names its own
log's tables, so its statement text is that log's alone and a connection holds
one prepared statement per log it has touched. pgx caches 512 by default and
evicts by LRU: a connection round-robining across more logs than that misses on
every append, and the single round trip becomes three. It is fixed in the DSN,
not in this package:

```
postgres://…/db?statement_cache_capacity=2048
```

At 1024 logs the difference measured three round trips and 1985 bytes an append
against one and 1151 with the cache sized for the log count.

**Leave the isolation level at READ COMMITTED.** The three refusals are decided
by a predicate that is re-evaluated after a wait, and that re-evaluation is
READ COMMITTED's alone. Point this package at a DSN or a server set to
`repeatable read` and a contended append comes back `SQLSTATE 40001, could not
serialize access due to concurrent update` — an error this package wraps and
passes on, not one of the three — so the writer that would have read
`ErrAlreadyWritten` as its ack is told something it has no rule for. Nothing
here sets the level, and nothing here can tell that it was changed.

**A schema is the unit of separation, and it is PostgreSQL's own.** Every name
this package writes is unqualified — `pgnotch_logs`, `pgnotch_entries_<n>_<slot>`,
`pgnotch_migrations` — so all of them land wherever the connection's `search_path`
points. Give a deployment a schema and it shares nothing with the next one:

```
postgres://…/db?search_path=orders
```

Two `Store`s over one schema are two writers of the same logs, which is what a
failover looks like from here. This package neither creates the schema nor
assumes it is empty.

`Drop` removes the tables this package owns and leaves the schema alone. It
finds the per-log entry tables through the registry rather than by matching a
name pattern, because there is no bound on how many there are and the registry
is the only complete list. An operator giving the whole thing back wants
`DROP SCHEMA … CASCADE` instead.

**Create your logs; nothing here creates one for you.** A log is two tables, an
id is an arbitrary string you supply, and PostgreSQL does not reclaim a table
because nobody wanted it. If a fence conjured a log, one bad id — a wrong
tenant, an unescaped input, a retry loop with a counter in it — would leave
tables behind at whatever rate you called it, and a million of them is a
`pg_class` you cannot clean up after the fact. So `CreateLogs` is explicit and
`Fence` answers `ErrNoSuchLog`:

```go
// Idempotent, so run it over the whole set on every start.
err := store.CreateLogs(ctx, ids...)
```

Over a set that is already there it does no DDL at all: the insert returns the
rows it actually created, which on a restart is none. Creating a set that *is*
new costs one statement of DDL for the whole batch rather than two per log, so
provisioning a shard range at start-up is a handful of round trips and not
thousands. Where your id space is genuinely bounded and you know the bound — a
fixed shard count, a tenant list — this is the shape to reach for.

**Trim, or pay for every entry ever written.** Nothing here trims on its own:
the log does not know which entries you have finished with. `Trim` moves the
watermark synchronously and reclaims the space when it can — reclamation takes
an `ACCESS EXCLUSIVE` lock with a 250 ms timeout, so a backup holding the table
off delays the space and never the log.

## What this is not

* **not a replicated log.** Durability and availability are PostgreSQL's, which
  means whatever your replication and failover give you and nothing more. There
  is no quorum here and no leader election — fencing tells you when you have
  lost a log, it does not decide who gets it;
* **not a queue or a broker.** There are no consumer groups, no acks per reader,
  no delivery semantics. A reader tracks its own position and reads from it;
* **not multi-writer.** One epoch owns a log. Concurrent appends to one log from
  two holders of the same epoch race for seqnos and lose, and fencing cannot
  separate them — whoever hands epochs out owes a strictly greater one per
  acquire;
* **not a place for large values by default.** Entries above a page are chunked
  and reassembled, which works at any size PostgreSQL can hold, but a megabyte
  entry is a megabyte through the connection on every read of it.

## Tests

The suite needs a PostgreSQL 16 or newer and is pointed at it by environment
variable. It creates a schema of its own per test and drops it afterwards.

```sh
PGNOTCH_DSN=postgres://user:password@localhost:5432/db go test ./...
```

`make pg-up` starts one in a container for exactly this — a tmpfs data
directory, since nothing a test writes is meant to outlive the run — and
`make test` points the suite at it; `make pg-down` takes it away again.

**Without `PGNOTCH_DSN` the suite does not skip — it fails.** There is no
configuration in which `go test ./...` is green having never spoken to
PostgreSQL, and that is deliberate: this package is a claim about what
PostgreSQL does, so a run that stayed inside the process is not a weaker result,
it is a different one wearing the same colour.

`rules_test.go` holds the rules that are pure functions of values — the ranking
between two refusals that both apply, the chunk boundary either side, the ring's
arithmetic at zero. A driver test reaches one path of each per run, and some of
them only by racing two writers, so they are stated there as tables. They need
no database and they run under the same rule anyway: they are the cases the
driver tests cannot reach, not a suite of their own.

Two of them are cost guards rather than correctness tests — one for what an
append writes, one for what it waits for — and both were written by breaking the
implementation on purpose and watching them go red. They are the reason this
package is worth having over any other way of putting a log in a table, so a
change that makes either of them fail is a change to what this package is.

## Putting load on it

`cmd/pgnotch-load` is a load generator: it takes everything from its flags,
appends at the rate they name until it is interrupted, and prints what the
window did.

```sh
make load ARGS='-rps 500 -logs 8 -sizes 1k:9,32k:1'
# or, pointed anywhere yourself
go run ./cmd/pgnotch-load -dsn postgres://user:password@localhost:5432/db -rps 500
```

```
8 logs at epoch 1787607891, 500 appends/s × 1 entries = 500 entries/s, ~2.0 MiB/s
sizes 1.0 KiB×9 32.0 KiB×1, mean 4.1 KiB, 10% over the 8000-byte chunk
each log keeps 100000 entries, trimmed every 12500
[    5s]     2500 appends    500.0/s |     2500 entries |  10.1 MiB   2.0 MiB/s |     3480 rows    250 cut | p50  319µs p99  639µs max  1.5ms
```

The distribution is the point of the tool. `-sizes` takes `size:weight` classes
— `1k:9,32k:1` is one entry in ten at 32 KiB — and a class over `MaxEntryChunk`
is an entry the library has to cut into several rows, which is a different write
from a 900-byte one and is not exercised by a single size. `rows` and `cut` on
the report line are how much of that the window actually did.

| flag | |
|---|---|
| `-rps` | appends a second over all logs together |
| `-logs` | logs, which is also the number of concurrent writers: one log admits one |
| `-batch` | entries per append |
| `-sizes` | the payload-size distribution, `size:weight` with `k` and `m` suffixes |
| `-retain` | entries a log keeps before the writer trims behind itself; `0` never trims |
| `-duration` | how long to run; `0` runs until `SIGINT` |
| `-schema` | the schema to put the logs in, created if missing |

Three things follow from what a log is, rather than from the tool:

* **it owns the logs it writes.** They are created under `-prefix`, fenced at an
  epoch of the run's own — Unix seconds unless you pass `-epoch` — and a run
  that finds one taken by a higher epoch stops with `ErrFenced` rather than
  fencing it back, because a second generator racing the first would be
  measuring the race;
* **it trims behind itself**, which is what makes an unbounded run cost a
  bounded amount of disk. `-retain 0` turns that off and the tables then grow
  for as long as it runs;
* **it picks up where it left off.** A restart asks each log where its next
  append goes, the way any new owner has to, and says which seqno it continues
  at.

The rate is a schedule fixed when the run starts, not a sleep between appends,
so a slow round trip is repaid out of the slots after it rather than lowering
the rate quietly. When it cannot be repaid the slots are abandoned and counted
as `skipped`: a run that could not keep the rate you asked for says so on the
line, and so does a run whose appends are failing — the count and the last
error, since a load generator writing nothing looks exactly like one writing
everything.

## License

MIT — see [LICENSE](LICENSE).
