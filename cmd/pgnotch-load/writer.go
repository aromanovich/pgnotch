package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/aromanovich/pgnotch"
)

// readBatch is how many entries a tail walk asks for at a time. It only runs at
// start-up and after a refusal, so the size trades nothing worth a knob.
const readBatch = 1024

// writer is the load on one log: one goroutine appending at consecutive seqnos,
// because a log admits one writer at a time and two concurrent appends to it
// race for seqnos and lose. Concurrency in this tool is therefore logs, and the
// rate a writer keeps is bounded by its own round trip — which the schedule
// reports as skipped slots rather than hiding.
type writer struct {
	store  *pgnotch.Store
	cfg    *config
	c      *counters
	corpus *corpus
	rnd    *rand.Rand

	id    pgnotch.LogID
	epoch pgnotch.Epoch
	// next is where this writer's next batch starts: its own high-water mark,
	// which nothing hands over and every owner keeps for itself.
	next pgnotch.Seqno
	// trimmed is the seqno the last trim asked for, so the trims are one per
	// stride rather than one per append.
	trimmed pgnotch.Seqno
}

// claim takes the log at this run's epoch and finds where its next append goes,
// both before any load starts: a run that cannot own the logs it was pointed at
// should fail before it has written anything.
func (w *writer) claim(ctx context.Context) error {
	fenceCtx, cancel := context.WithTimeout(ctx, w.cfg.timeout)
	defer cancel()
	if err := w.store.Fence(fenceCtx, w.id, w.epoch); err != nil {
		return err
	}
	next, err := w.tail(ctx, pgnotch.FirstSeqno)
	if err != nil {
		return err
	}
	w.next = next
	return nil
}

// tail is the first free seqno at or above from, read the way a new owner is
// expected to find one: there is no call that hands the mark over, so it reads
// until a short read. Called with [pgnotch.FirstSeqno] it walks the whole
// retained log; called with the writer's own position it costs one read, which
// is what a refusal needs to resynchronise.
func (w *writer) tail(ctx context.Context, from pgnotch.Seqno) (pgnotch.Seqno, error) {
	for {
		readCtx, cancel := context.WithTimeout(ctx, w.cfg.timeout)
		entries, err := w.store.ReadFrom(readCtx, w.id, from, readBatch)
		cancel()
		if err != nil {
			return 0, err
		}
		if len(entries) > 0 {
			from = entries[len(entries)-1].Seqno + 1
		}
		if len(entries) < readBatch {
			return from, nil
		}
	}
}

// run appends until the context ends. It returns nil for a run that was stopped
// and an error only for a state no load generator may write through: losing the
// log to another epoch, and a refusal the log's own contract does not admit.
func (w *writer) run(ctx context.Context, start time.Time) error {
	pace := newPacer(start, w.cfg.interval, w.cfg.catchup)
	batch := make([][]byte, 0, w.cfg.batch)
	// A batch is drawn once and held until it is done with, because a replay
	// after an ambiguous append has to be the same bytes at the same seqno.
	fresh := true
	for {
		skipped, err := pace.wait(ctx)
		w.c.skipped.Add(uint64(skipped))
		if err != nil {
			return nil
		}
		if fresh {
			batch = w.fill(batch)
		}
		done, err := w.append(ctx, batch)
		if err != nil {
			return err
		}
		fresh = done
		if done {
			w.trim(ctx)
		}
	}
}

// fill draws the next batch. The payloads are windows into the shared corpus,
// so a batch allocates nothing but the slice headers it reuses.
func (w *writer) fill(batch [][]byte) [][]byte {
	batch = batch[:0]
	for range w.cfg.batch {
		batch = append(batch, w.corpus.payload(w.rnd, w.cfg.sizes.draw(w.rnd)))
	}
	return batch
}

// append sends the batch once and files what happened. done is false when the
// same batch must go again: an append that failed ambiguously may have landed,
// and the contract's answer is to replay it at the same seqno and read
// [pgnotch.ErrAlreadyWritten] as the ack. Retrying costs the next slot rather
// than spinning, so a database that has gone away does not become a hot loop.
func (w *writer) append(ctx context.Context, batch [][]byte) (done bool, err error) {
	callCtx, cancel := context.WithTimeout(ctx, w.cfg.timeout)
	defer cancel()

	began := time.Now()
	err = w.store.Append(callCtx, w.id, w.epoch, w.next, batch)
	w.c.lat.observe(time.Since(began))

	switch {
	case err == nil:
		w.record(batch)
		w.next += pgnotch.Seqno(len(batch))
		return true, nil

	case errors.Is(err, pgnotch.ErrFenced):
		// Somebody else owns the log. A load generator that fenced again here
		// would be racing whoever took it, and hiding a real event behind a
		// count of retries.
		return false, fmt.Errorf("%q: %w", w.id, err)

	case errors.Is(err, pgnotch.ErrAlreadyWritten):
		// The ack for an earlier attempt, or a mark that moved while this
		// writer was not looking. One read says which and where to go on from.
		w.c.acks.Add(1)
		w.resync(ctx, w.next)
		return false, nil

	case errors.Is(err, pgnotch.ErrGap):
		// Unreachable for a single writer appending in order, so it is read as
		// a lost position rather than as a race: the walk starts from the
		// bottom because a gap means this writer is above the log's end.
		w.c.gaps.Add(1)
		w.resync(ctx, pgnotch.FirstSeqno)
		return false, nil

	default:
		if ctx.Err() != nil {
			// The run ended under the call. Counting the deadline it inherited
			// would put an error on every run that stops while writing.
			return false, nil
		}
		w.c.note(err)
		return false, nil
	}
}

// resync puts the writer back on the log's own mark. A read that fails leaves
// the position alone: the batch goes again at the next slot, is refused again,
// and comes back here — a retry loop paced like every other append.
func (w *writer) resync(ctx context.Context, from pgnotch.Seqno) {
	next, err := w.tail(ctx, from)
	if err != nil {
		w.c.note(err)
		return
	}
	w.next = next
}

func (w *writer) record(batch [][]byte) {
	rows, cut, bytes := 0, 0, 0
	for _, payload := range batch {
		bytes += len(payload)
		chunks := max(1, (len(payload)+pgnotch.MaxEntryChunk-1)/pgnotch.MaxEntryChunk)
		rows += chunks
		if chunks > 1 {
			cut++
		}
	}
	w.c.appends.Add(1)
	w.c.entries.Add(uint64(len(batch)))
	w.c.bytes.Add(uint64(bytes))
	w.c.rows.Add(uint64(rows))
	w.c.cut.Add(uint64(cut))
}

// trim keeps the log to the retained length, which is what lets the run be
// unbounded: without it an infinite append load is an infinite table. It moves
// the watermark once per stride, since a watermark that moves by one entry
// costs the same round trip as one that moves by a thousand.
func (w *writer) trim(ctx context.Context) {
	if w.cfg.retain <= 0 {
		return
	}
	written := w.next - 1
	if written <= pgnotch.Seqno(w.cfg.retain) {
		return
	}
	upTo := written - pgnotch.Seqno(w.cfg.retain)
	if upTo < w.trimmed+pgnotch.Seqno(w.cfg.stride) {
		return
	}
	trimCtx, cancel := context.WithTimeout(ctx, w.cfg.timeout)
	defer cancel()
	if err := w.store.Trim(trimCtx, w.id, upTo); err != nil {
		// A trim that failed is retried at the next stride: the load is
		// unaffected, and it is only the log's length that suffers.
		w.c.note(err)
		return
	}
	w.trimmed = upTo
	w.c.trims.Add(1)
}
