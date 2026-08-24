package main

import (
	"context"
	"time"
)

// The rate is a schedule fixed when the run starts, not a sleep between
// appends: sleeping an interval after each round trip makes the achieved rate a
// function of the latency, which is the one thing a load generator may not let
// the system under test decide.

// schedule answers when the slot that was due at due may run, and how many
// slots were given up on. Everything about the pacing is here rather than in
// [pacer] so that the arithmetic can be tested without a clock.
//
// A slot that is late is issued at once and the schedule keeps its origin, so
// one slow append is paid back out of the slots after it. Past catchup that
// repayment would be a burst of appends the operator never asked for, so the
// missed slots are abandoned and the schedule restarts from now — reported
// rather than silently smoothed away, since a run that cannot keep its rate is
// a result.
func schedule(due, now time.Time, interval, catchup time.Duration) (wait time.Duration, next time.Time, skipped int) {
	behind := now.Sub(due)
	switch {
	case behind < 0:
		return -behind, due.Add(interval), 0
	case behind <= catchup:
		return 0, due.Add(interval), 0
	default:
		return 0, now.Add(interval), int(behind / interval)
	}
}

// pacer hands out one slot per interval to one writer.
type pacer struct {
	interval time.Duration
	catchup  time.Duration
	due      time.Time
	timer    *time.Timer
}

func newPacer(start time.Time, interval, catchup time.Duration) *pacer {
	return &pacer{interval: interval, catchup: catchup, due: start}
}

// wait blocks until the next slot is due, and returns the slots abandoned to
// get to it. A cancelled context ends the wait with the context's error.
func (p *pacer) wait(ctx context.Context) (skipped int, err error) {
	wait, next, skipped := schedule(p.due, time.Now(), p.interval, p.catchup)
	p.due = next
	if wait <= 0 {
		// Still a cancellation point: a writer that never sleeps would not
		// notice the run had ended.
		select {
		case <-ctx.Done():
			return skipped, ctx.Err()
		default:
			return skipped, nil
		}
	}
	// Reused rather than time.After per slot, which at a few thousand slots a
	// second is garbage for nothing. Since Go 1.23 a Reset needs no drain: the
	// channel cannot hold a value from the previous round.
	if p.timer == nil {
		p.timer = time.NewTimer(wait)
	} else {
		p.timer.Reset(wait)
	}
	select {
	case <-p.timer.C:
		return skipped, nil
	case <-ctx.Done():
		p.timer.Stop()
		return skipped, ctx.Err()
	}
}
