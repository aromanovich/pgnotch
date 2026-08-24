package main

import (
	"fmt"
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// counters are what the writers add to and the reporter reads. Every field is
// cumulative: a window is the difference of two snapshots, so a reader that
// misses a tick loses resolution and never a count.
type counters struct {
	appends atomic.Uint64
	entries atomic.Uint64
	bytes   atomic.Uint64
	// rows is the entry rows the appends wrote, which is above entries by
	// exactly what the chunking of over-sized payloads cost.
	rows atomic.Uint64
	cut  atomic.Uint64
	// acks are the replays that came back ErrAlreadyWritten — an append that
	// failed ambiguously and had in fact landed. Their entries are not counted
	// above: this run did not write them, a previous attempt of it did.
	acks   atomic.Uint64
	gaps   atomic.Uint64
	trims  atomic.Uint64
	failed atomic.Uint64
	// skipped are the slots the schedule gave up on, which is how a run that
	// cannot keep its rate says so.
	skipped atomic.Uint64

	lat histogram

	// The last driver error, carried to the report so that a run failing
	// steadily says why on the line that says how often, rather than only in
	// the count.
	mu   sync.Mutex
	last string
}

func (c *counters) note(err error) {
	c.failed.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last = err.Error()
}

// totals is one reading of the counters.
type totals struct {
	at                                                            time.Time
	appends, entries, bytes, rows, cut, acks, gaps, trims, failed uint64
	skipped                                                       uint64
	lat                                                           []uint64
	last                                                          string
}

func (c *counters) snapshot() totals {
	c.mu.Lock()
	last := c.last
	c.mu.Unlock()
	return totals{
		at:      time.Now(),
		appends: c.appends.Load(),
		entries: c.entries.Load(),
		bytes:   c.bytes.Load(),
		rows:    c.rows.Load(),
		cut:     c.cut.Load(),
		acks:    c.acks.Load(),
		gaps:    c.gaps.Load(),
		trims:   c.trims.Load(),
		failed:  c.failed.Load(),
		skipped: c.skipped.Load(),
		lat:     c.lat.snapshot(),
		last:    last,
	}
}

// sub is the window between two readings. The latency buckets subtract with
// everything else, which is why the histogram is counts and not samples.
func (t *totals) sub(p *totals) totals {
	d := totals{at: t.at, last: t.last}
	d.appends, d.entries, d.bytes = t.appends-p.appends, t.entries-p.entries, t.bytes-p.bytes
	d.rows, d.cut, d.acks = t.rows-p.rows, t.cut-p.cut, t.acks-p.acks
	d.gaps, d.trims, d.failed = t.gaps-p.gaps, t.trims-p.trims, t.failed-p.failed
	d.skipped = t.skipped - p.skipped
	d.lat = make([]uint64, len(t.lat))
	for i := range t.lat {
		d.lat[i] = t.lat[i] - p.lat[i]
	}
	return d
}

// line is one report: what the window did, at what rate, and how it went wrong.
// elapsed is since the run started and window is what this reading covers.
func (t *totals) line(elapsed, window time.Duration) string {
	secs := window.Seconds()
	var b strings.Builder
	fmt.Fprintf(&b, "[%6s] %8d appends %8.1f/s | %8d entries | %9s %9s/s | %8d rows %6d cut",
		elapsed.Truncate(time.Second), t.appends, float64(t.appends)/secs, t.entries,
		humanBytes(t.bytes), humanBytes(uint64(float64(t.bytes)/secs)),
		t.rows, t.cut)
	fmt.Fprintf(&b, " | p50 %6s p99 %6s max %6s",
		shortDuration(quantile(t.lat, 0.50)), shortDuration(quantile(t.lat, 0.99)),
		shortDuration(quantile(t.lat, 1)))
	if t.acks+t.gaps+t.failed+t.skipped+t.trims != 0 {
		fmt.Fprintf(&b, " | trims %d acks %d gaps %d errors %d skipped %d",
			t.trims, t.acks, t.gaps, t.failed, t.skipped)
	}
	if t.failed > 0 && t.last != "" {
		fmt.Fprintf(&b, "\n          last error: %s", t.last)
	}
	return b.String()
}

// The latency histogram: four buckets to an octave of microseconds, so a
// quantile is at most a quarter above the sample it names. Coarse on purpose —
// the alternative is a sample buffer per writer and a merge per report, and
// what a run of this needs from a latency is its order of magnitude and whether
// it moved.
const (
	subBits  = 2
	subCount = 1 << subBits
	nBuckets = 64 * subCount
)

type histogram struct {
	buckets [nBuckets]atomic.Uint64
}

func (h *histogram) observe(d time.Duration) {
	us := max(d.Microseconds(), 0)
	h.buckets[bucketOf(uint64(us))].Add(1)
}

// snapshot copies the counts out as a slice, which is what keeps a reading of
// them cheap to pass around: the histogram itself is two kilobytes.
func (h *histogram) snapshot() []uint64 {
	out := make([]uint64, nBuckets)
	for i := range out {
		out[i] = h.buckets[i].Load()
	}
	return out
}

// bucketOf files a microsecond count. Below subCount each value is its own
// bucket, and above it the octave and the top subBits of the mantissa index
// one, which makes the buckets contiguous across the join.
func bucketOf(us uint64) int {
	if us < subCount {
		return int(us)
	}
	octave := bits.Len64(us) - 1
	sub := (us >> (octave - subBits)) & (subCount - 1)
	return (octave-subBits+1)*subCount + int(sub)
}

// bucketBound is the largest microsecond count that lands in bucket i, so a
// quantile is reported as the bound rather than as a sample nobody observed.
func bucketBound(i int) uint64 {
	if i < subCount {
		return uint64(i)
	}
	octave := i/subCount + subBits - 1
	width := uint64(1) << (octave - subBits)
	return (uint64(subCount)+uint64(i%subCount))*width + width - 1
}

// quantile is the q-th quantile of the counts, as the upper bound of the bucket
// the q-th sample falls in. An empty histogram has no quantile and reads zero.
func quantile(buckets []uint64, q float64) time.Duration {
	var total uint64
	for _, n := range buckets {
		total += n
	}
	if total == 0 {
		return 0
	}
	// Ceiling, so q=1 is the last sample and not one past it.
	target := uint64(q*float64(total-1)) + 1
	var seen uint64
	for i, n := range buckets {
		if seen += n; seen >= target {
			return time.Duration(bucketBound(i)) * time.Microsecond
		}
	}
	return time.Duration(bucketBound(nBuckets-1)) * time.Microsecond
}

func humanBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// shortDuration keeps a latency to one unit, so that a column of them lines up.
func shortDuration(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
}
