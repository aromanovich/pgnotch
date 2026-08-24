// Command pgnotch-load puts an unbounded append load on pgnotch logs.
//
// It takes everything from its flags, writes at the rate they name from the
// payload-size distribution they name, and keeps going until it is interrupted.
// The distribution is the point: a class above pgnotch.MaxEntryChunk is an
// entry the library has to cut into several rows, which is a different write
// from a 900-byte one and is not otherwise exercised by a fixed size.
//
//	pgnotch-load -dsn postgres://…/db -rps 500 -logs 8 -sizes 1k:9,32k:1
//
// The logs are this tool's own: it creates them, fences them at an epoch of the
// run's own, appends to them, and trims behind itself so that a run of any
// length costs a bounded amount of disk. Nothing here reads the logs back for
// their content — the suites do that; this asks what the write path costs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aromanovich/pgnotch"
)

// envDSN names the database the load is put on, and there is no default, on the
// rule this repository's suite keeps: a tool that reaches a database nobody
// named can write into tables somebody cared about.
const envDSN = "PGNOTCH_DSN"

// config is the whole of what the operator chooses, plus what is derived from
// it once: the interval a writer keeps, how far behind it may catch up, the
// stride its trims move, the pool size and the epoch it fences at.
type config struct {
	dsn      string
	schema   string
	prefix   string
	logs     int
	rps      float64
	batch    int
	sizes    *sizes
	retain   int
	stride   int
	epoch    uint64
	conns    int
	timeout  time.Duration
	catchup  time.Duration
	report   time.Duration
	duration time.Duration
	migrate  bool
	interval time.Duration
	// cache is what the DSN leaves pgx's statement cache at. A connection holds
	// one prepared append per log it has touched, so a run with more logs than
	// this — or with the cache off — measures re-preparation rather than the
	// log, and says so before it starts.
	cache int
}

func main() {
	// ErrHelp is -h: the usage has been printed and there is nothing to report.
	if err := run(context.Background(), os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(os.Stderr, "pgnotch-load: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	// The run ends on a signal or on its own deadline, and both are the same
	// end: writers stop at their next slot and the totals are printed.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	pool, err := openPool(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	store, ids, err := prepare(ctx, pool, cfg)
	if err != nil {
		return err
	}
	return drive(ctx, store, ids, cfg)
}

func parseFlags(args []string) (*config, error) {
	cfg := &config{}
	fs := flag.NewFlagSet("pgnotch-load", flag.ContinueOnError)
	spec := fs.String("sizes", "1k:9,32k:1",
		"payload sizes as `size:weight` classes, e.g. 1k:9,32k:1 — a class over 8000 B is chunked")
	fs.StringVar(&cfg.dsn, "dsn", os.Getenv(envDSN), "PostgreSQL 16+ to load, defaulting to $"+envDSN)
	fs.StringVar(&cfg.schema, "schema", "pgnotch_load",
		"`schema` to put the logs in, created if missing; empty leaves the DSN's search_path alone")
	fs.StringVar(&cfg.prefix, "prefix", "load", "`prefix` of the log ids this run writes to")
	fs.IntVar(&cfg.logs, "logs", 4, "`number` of logs, which is also the number of concurrent writers")
	fs.Float64Var(&cfg.rps, "rps", 100, "appends per second over all logs together")
	fs.IntVar(&cfg.batch, "batch", 1, "entries per append")
	fs.IntVar(&cfg.retain, "retain", 100_000,
		"entries a log keeps before the writer trims behind itself; 0 never trims")
	fs.Uint64Var(&cfg.epoch, "epoch", 0, "epoch to fence the logs at; 0 takes the current Unix time")
	fs.IntVar(&cfg.conns, "conns", 0, "pool size; 0 takes one connection per writer and one spare")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "deadline for one call to the database")
	fs.DurationVar(&cfg.report, "report", 5*time.Second, "how often to print a line")
	fs.DurationVar(&cfg.duration, "duration", 0, "how long to run; 0 runs until interrupted")
	fs.BoolVar(&cfg.migrate, "migrate", true, "apply the schema before opening it")

	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(),
			"pgnotch-load puts an unbounded append load on pgnotch logs.\n\n"+
				"Usage:\n  pgnotch-load [flags]\n\n"+
				"  # 500 appends a second over 8 logs, one entry in ten too large for a row\n"+
				"  pgnotch-load -dsn postgres://user:password@localhost:5432/db -rps 500 -logs 8\n\n"+
				"Flags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	var err error
	if cfg.sizes, err = parseSizes(*spec); err != nil {
		return nil, err
	}
	if cfg.dsn == "" {
		return nil, fmt.Errorf("no database: pass -dsn or set %s", envDSN)
	}
	switch {
	case cfg.logs < 1:
		return nil, fmt.Errorf("-logs %d: a run needs at least one log", cfg.logs)
	case cfg.rps <= 0:
		return nil, fmt.Errorf("-rps %v: the rate must be positive", cfg.rps)
	case cfg.batch < 1:
		return nil, fmt.Errorf("-batch %d: an append carries at least one entry", cfg.batch)
	case cfg.retain < 0:
		return nil, fmt.Errorf("-retain %d: negative is not a length", cfg.retain)
	case cfg.timeout <= 0 || cfg.report <= 0:
		return nil, errors.New("-timeout and -report must be positive")
	}

	// One writer's share of the rate. Kept as an interval rather than a rate so
	// that the schedule is exact arithmetic on times.
	cfg.interval = time.Duration(float64(time.Second) * float64(cfg.logs) / cfg.rps)
	if cfg.interval <= 0 {
		return nil, fmt.Errorf("-rps %v over %d logs is faster than this tool can schedule", cfg.rps, cfg.logs)
	}
	// How far behind its schedule a writer may fall and still catch up. A round
	// trip's worth of slots is a hiccup; a call's whole deadline is a database
	// that stopped answering, and issuing those slots back to back afterwards
	// would be a burst nobody asked for.
	cfg.catchup = min(cfg.timeout, time.Second)
	// Trims move by a fraction of what a log retains, since a watermark that
	// moves by one entry costs the same round trip as one that moves by many.
	cfg.stride = max(1, cfg.retain/8)
	if cfg.conns == 0 {
		cfg.conns = cfg.logs + 1
	}
	if cfg.epoch == 0 {
		// Unix seconds: strictly greater on each run, which is what fencing
		// asks of whoever hands epochs out, without a counter to keep anywhere.
		cfg.epoch = uint64(time.Now().Unix())
	}
	return cfg, nil
}

func openPool(ctx context.Context, cfg *config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("the DSN is not one pgx accepts: %w", err)
	}
	cfg.cache = poolCfg.ConnConfig.StatementCacheCapacity
	poolCfg.MaxConns = int32(cfg.conns)
	poolCfg.MinConns = int32(cfg.conns)
	if cfg.schema != "" {
		poolCfg.ConnConfig.RuntimeParams["search_path"] = cfg.schema
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to build the pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no PostgreSQL there: %w (it must be 16 or newer)", err)
	}
	return pool, nil
}

// prepare brings the schema, the logs and the store into existence. It is
// separate from the load because everything it does is idempotent and none of
// it is what the run measures.
func prepare(ctx context.Context, pool *pgxpool.Pool, cfg *config) (*pgnotch.Store, []pgnotch.LogID, error) {
	if cfg.schema != "" {
		// Naming a schema in search_path before it exists is allowed, and
		// CREATE SCHEMA names its own target rather than resolving one, so this
		// can go through the pool that will use it.
		name := pgx.Identifier{cfg.schema}.Sanitize()
		if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+name); err != nil {
			return nil, nil, fmt.Errorf("unable to create the schema %s: %w", name, err)
		}
	}
	if cfg.migrate {
		if err := pgnotch.Migrate(ctx, pool); err != nil {
			return nil, nil, err
		}
	}
	store, err := pgnotch.Open(ctx, pool)
	if err != nil {
		return nil, nil, err
	}

	ids := make([]pgnotch.LogID, cfg.logs)
	for i := range ids {
		ids[i] = pgnotch.LogID(fmt.Sprintf("%s-%04d", cfg.prefix, i))
	}
	if err := store.CreateLogs(ctx, ids...); err != nil {
		return nil, nil, err
	}
	return store, ids, nil
}

// drive claims every log, starts a writer on each and reports until the context
// ends. The claims are made here, before any writer starts, so that a run which
// cannot own its logs fails having written nothing.
func drive(ctx context.Context, store *pgnotch.Store, ids []pgnotch.LogID, cfg *config) error {
	epoch := pgnotch.Epoch(cfg.epoch)
	corpus, err := newCorpus(cfg.sizes.largest())
	if err != nil {
		return err
	}
	c := &counters{}
	writers := make([]*writer, len(ids))
	for i, id := range ids {
		writers[i] = &writer{
			store: store, cfg: cfg, c: c, corpus: corpus,
			rnd:   rand.New(rand.NewPCG(uint64(i)+1, uint64(epoch))),
			id:    id,
			epoch: epoch,
		}
		if err := writers[i].claim(ctx); err != nil {
			return err
		}
	}
	fmt.Println(plan(cfg, writers))

	// A writer that returns an error ends the run: the two it can return —
	// the log lost to another epoch, and a refusal the contract does not admit
	// — are both states in which going on would report load nobody is writing.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	var wg sync.WaitGroup
	failures := make(chan error, len(writers))
	// A writer's share of the interval apart, so the slots interleave rather
	// than every log appending at once and queuing behind itself — which would
	// land in the reported latency as the database's.
	spread := cfg.interval / time.Duration(len(writers))
	for i, w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.run(ctx, start.Add(time.Duration(i)*spread)); err != nil {
				failures <- err
				cancel()
			}
		}()
	}

	report(ctx, c, cfg, start)
	wg.Wait()

	total := c.snapshot()
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println(total.line(time.Since(start), time.Since(start)))

	close(failures)
	return <-failures
}

// report prints a line per interval until the run ends, each line covering the
// window since the one before it.
func report(ctx context.Context, c *counters, cfg *config, start time.Time) {
	ticker := time.NewTicker(cfg.report)
	defer ticker.Stop()
	previous := c.snapshot()
	for {
		select {
		case <-ticker.C:
			current := c.snapshot()
			window := current.sub(&previous)
			fmt.Println(window.line(time.Since(start), current.at.Sub(previous.at)))
			previous = current
		case <-ctx.Done():
			return
		}
	}
}

// plan is what the run is about to ask of the database, printed before it does:
// the rates a reader would otherwise have to recompute from the flags, and the
// two ways a configuration quietly measures something else.
func plan(cfg *config, writers []*writer) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d logs at epoch %d, %g appends/s × %d entries = %g entries/s, ~%s/s\n",
		len(writers), cfg.epoch, cfg.rps, cfg.batch, cfg.rps*float64(cfg.batch),
		humanBytes(uint64(cfg.rps*float64(cfg.batch)*cfg.sizes.mean())))
	fmt.Fprintf(&b, "sizes %s, mean %s, %.0f%% over the %d-byte chunk\n",
		cfg.sizes, humanBytes(uint64(cfg.sizes.mean())), 100*cfg.sizes.chunked(), pgnotch.MaxEntryChunk)
	if cfg.retain > 0 {
		fmt.Fprintf(&b, "each log keeps %d entries, trimmed every %d\n", cfg.retain, cfg.stride)
	} else {
		fmt.Fprintf(&b, "nothing is trimmed: -retain 0, so the tables grow for as long as this runs\n")
	}
	for _, w := range writers {
		if w.next > pgnotch.FirstSeqno {
			fmt.Fprintf(&b, "%s continues at seqno %d\n", w.id, w.next)
		}
	}
	switch {
	case cfg.cache <= 0:
		fmt.Fprintf(&b, "warning: the DSN turns the statement cache off, so every append re-prepares\n")
	case len(writers) > cfg.cache:
		fmt.Fprintf(&b, "warning: %d logs is over the DSN's %d prepared statements, so appends will re-prepare\n",
			len(writers), cfg.cache)
	}
	return strings.TrimRight(b.String(), "\n")
}
