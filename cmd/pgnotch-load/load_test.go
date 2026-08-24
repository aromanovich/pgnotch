package main

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aromanovich/pgnotch"
)

// What these cover is the arithmetic the load rests on and nothing that talks
// to PostgreSQL: the package's own suite is where a database belongs, and a
// generator whose distribution or schedule is wrong reports a load nobody put.

func TestSizesDrawEveryClassInProportion(t *testing.T) {
	s, err := parseSizes("100:1,32k:3")
	require.NoError(t, err)

	rnd := rand.New(rand.NewPCG(1, 2))
	drawn := map[int]int{}
	for range 10_000 {
		drawn[s.draw(rnd)]++
	}

	require.Len(t, drawn, 2, "both classes are drawn")
	// Three to one, within a margin a fair draw stays inside at ten thousand.
	ratio := float64(drawn[32<<10]) / float64(drawn[100])
	require.InDelta(t, 3, ratio, 0.3)
}

func TestSizesReportWhatTheDistributionAsksOf(t *testing.T) {
	// Nine entries of 1 KiB to one of 32 KiB: the one is over the chunk and the
	// mean is what the byte rate is computed from.
	s, err := parseSizes("1k:9,32k:1")
	require.NoError(t, err)

	require.Equal(t, 32<<10, s.largest())
	require.InDelta(t, (9*1024+32768)/10.0, s.mean(), 0.001)
	require.InDelta(t, 0.1, s.chunked(), 0.001)
	require.Greater(t, s.largest(), pgnotch.MaxEntryChunk, "the default asks for chunking")
}

func TestSizesRefuseWhatWouldLoadSomethingElse(t *testing.T) {
	for _, spec := range []string{"", "1k:0", "1k:-1", "k", "1g", "1k,,2k", "17m", "-1"} {
		_, err := parseSizes(spec)
		require.Error(t, err, "spec %q", spec)
	}
}

func TestCorpusPayloadIsTheSizeAskedForAndVaries(t *testing.T) {
	c, err := newCorpus(32 << 10)
	require.NoError(t, err)
	rnd := rand.New(rand.NewPCG(3, 4))

	require.Empty(t, c.payload(rnd, 0))
	require.Len(t, c.payload(rnd, 32<<10), 32<<10)

	// Two payloads of the same size are different bytes, so a run does not
	// hand PostgreSQL the same page over and over.
	first, second := string(c.payload(rnd, 900)), string(c.payload(rnd, 900))
	require.NotEqual(t, first, second)
}

func TestScheduleWaitsForASlotThatIsNotDue(t *testing.T) {
	due := time.Unix(100, 0)
	wait, next, skipped := schedule(due, due.Add(-30*time.Millisecond), 100*time.Millisecond, time.Second)

	require.Equal(t, 30*time.Millisecond, wait)
	require.Equal(t, due.Add(100*time.Millisecond), next, "the origin is kept")
	require.Zero(t, skipped)
}

func TestScheduleRepaysALateSlotOutOfTheNext(t *testing.T) {
	due := time.Unix(100, 0)
	// One slow append, 250 ms into a 100 ms interval: the slot is issued at
	// once and the schedule stays where it was, so the rate comes back.
	wait, next, skipped := schedule(due, due.Add(250*time.Millisecond), 100*time.Millisecond, time.Second)

	require.Zero(t, wait)
	require.Equal(t, due.Add(100*time.Millisecond), next)
	require.Zero(t, skipped)
	require.True(t, next.Before(due.Add(250*time.Millisecond)), "the next slot is already due, so it catches up")
}

func TestScheduleGivesUpOnSlotsItCannotCatchUp(t *testing.T) {
	due := time.Unix(100, 0)
	now := due.Add(5 * time.Second)
	wait, next, skipped := schedule(due, now, 100*time.Millisecond, time.Second)

	require.Zero(t, wait)
	require.Equal(t, 50, skipped, "the slots that went by are counted, not issued back to back")
	require.Equal(t, now.Add(100*time.Millisecond), next, "the schedule restarts from now")
}

func TestHistogramQuantilesBoundTheSamples(t *testing.T) {
	var h histogram
	for i := range 1000 {
		h.observe(time.Duration(i+1) * time.Microsecond)
	}
	buckets := h.snapshot()

	// A quantile is the bucket's upper bound, so it is at or above the sample
	// it names and within a quarter of it.
	for _, c := range []struct {
		q      float64
		sample time.Duration
	}{
		{0.5, 500 * time.Microsecond},
		{0.99, 990 * time.Microsecond},
		{1, 1000 * time.Microsecond},
	} {
		got := quantile(buckets, c.q)
		require.GreaterOrEqual(t, got, c.sample, "q=%v", c.q)
		require.LessOrEqual(t, got, c.sample*5/4, "q=%v", c.q)
	}
	require.Zero(t, quantile(make([]uint64, nBuckets), 0.5), "nothing observed has no quantile")
}

func TestHistogramBucketsAreOrdered(t *testing.T) {
	// Contiguity across the linear-to-octave join is what makes a quantile
	// monotonic; an overlap there would report a lower latency for a larger one.
	previous := -1
	for us := uint64(0); us < 1<<20; us = us + 1 + us/16 {
		i := bucketOf(us)
		require.GreaterOrEqual(t, i, previous, "bucket of %d µs went backwards", us)
		require.GreaterOrEqual(t, bucketBound(i), us, "bucket %d does not contain %d µs", i, us)
		previous = i
	}
}

func TestFlagsRefuseARunThatWouldMeasureNothing(t *testing.T) {
	const dsn = "-dsn=postgres://user:password@localhost:5432/db"
	for _, args := range [][]string{
		{"-rps=0", dsn},
		{"-logs=0", dsn},
		{"-batch=0", dsn},
		{"-retain=-1", dsn},
		{"-rps=100", "-dsn="}, // and no PGNOTCH_DSN, which the helper below clears
		{"-sizes=nonsense", dsn},
	} {
		t.Setenv(envDSN, "")
		_, err := parseFlags(args)
		require.Error(t, err, "args %v", args)
	}
}

func TestFlagsDeriveTheScheduleFromTheRate(t *testing.T) {
	t.Setenv(envDSN, "postgres://user:password@localhost:5432/db")
	before := time.Now().Unix()
	cfg, err := parseFlags([]string{"-rps=100", "-logs=4", "-retain=80000"})
	require.NoError(t, err)

	// An epoch nobody named is the clock, which is what makes each run's claim
	// strictly greater than the last one's.
	require.GreaterOrEqual(t, cfg.epoch, uint64(before))

	// Four writers sharing 100 appends a second is one every 40 ms each.
	require.Equal(t, 40*time.Millisecond, cfg.interval)
	require.Equal(t, 10_000, cfg.stride)
	require.Equal(t, 5, cfg.conns, "one connection per writer and a spare")
	require.Equal(t, "postgres://user:password@localhost:5432/db", cfg.dsn, "$"+envDSN+" is the default")
}
