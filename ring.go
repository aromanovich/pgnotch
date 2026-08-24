package pgnotch

import "github.com/jackc/pgx/v5"

// A log's entries live in two tables used as a ring: one takes every append,
// the other holds the generation before it. Space comes back when a trim has
// passed everything the older half holds and it is TRUNCATEd — a DELETE would
// leave the pages and the vacuum debt behind.
//
// The rules are here and decidable without a database; when they may be applied
// is [Store.reclaim]'s business, and depends on locks and a re-read.

// rotateAfterEntries is how far one half may run before the ring turns. A seqno
// span rather than a size, so deciding costs arithmetic on the registry row and
// never a look at the table.
//
// Small enough that a generation stands a chance of being dropped between two
// checkpoints, which is when its pages are discarded unwritten and the whole
// reason the ring exists. Large enough that rotation is rare beside appends.
const rotateAfterEntries = 4096

// logStateColumns is the registry row in the order [scanLogState] reads it.
// Every column is an integer, so one added here and not there mis-reads the row
// silently rather than failing.
const logStateColumns = `ordinal, last_seqno, trim_upto, cur_slot, cur_lo, prev_hi`

func scanLogState(row pgx.Row) (logState, error) {
	var st logState
	err := row.Scan(&st.ordinal, &st.lastSeqno, &st.trimUpto, &st.curSlot, &st.curLo, &st.prevHi)
	return st, err
}

// logState is the registry row, which is the whole of a log's state.
type logState struct {
	ordinal   int64
	lastSeqno Seqno
	trimUpto  Seqno
	curSlot   int16
	curLo     Seqno
	prevHi    Seqno
}

// prevSlot is the half that is not current: the one a truncate empties and a
// rotation is about to make current.
func (st logState) prevSlot() int16 { return 1 - st.curSlot }

// prevEmpty reports whether a trim has passed everything the other half holds.
// prev_hi = 0 says there is no other half to empty.
func (st logState) prevEmpty() bool { return st.prevHi > 0 && st.trimUpto >= st.curLo-1 }

// ringFull reports whether the live half has run far enough to turn the ring.
//
// The span is counted from the half's own start, so a log nobody has appended
// to has last_seqno 0 against cur_lo 1 and the subtraction runs backwards
// through the unsigned range before the +1 brings it to zero. Right by two's
// complement rather than by anything a reader can see.
func (st logState) ringFull() bool { return st.lastSeqno-st.curLo+1 >= rotateAfterEntries }

// canRotate reports whether the ring may turn. The half about to become current
// must be empty, and prev_hi = 0 is the only record that it is.
func (st logState) canRotate() bool { return st.prevHi == 0 && st.ringFull() }

// reclaimSteps is what may be done to this state by something that holds the
// entry tables, or does not.
//
// A reclamation asks twice. Before opening a transaction it asks with
// mayTruncate true — "if I take the tables, is there anything to do" — because
// a truncate sets prev_hi to 0 and so admits a turn the state does not yet
// admit. Under the row lock it asks again, mayTruncate now carrying whether the
// tables were really locked.
//
// That argument is why this is one function and not two: a state that became
// truncatable after the gate ran may not be truncated, and so may not rotate
// either. Rotating into a half nothing emptied writes the log's next entries
// over its oldest, and reads of those are filtered out by the trim watermark —
// nothing downstream would say so.
func (st logState) reclaimSteps(mayTruncate bool) (truncate, rotate bool) {
	truncate = mayTruncate && st.prevEmpty()
	after := st
	if truncate {
		after.prevHi = 0
	}
	return truncate, after.canRotate()
}
