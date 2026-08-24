package pgnotch

import "github.com/jackc/pgx/v5"

// A log's entries live in two tables used as a ring: one takes every append,
// the other holds the generation before it. Space comes back when a trim has
// passed everything the older half holds and it is TRUNCATEd; a DELETE would
// leave the pages and the vacuum debt behind. When the rules below may be
// applied is [Store.reclaim]'s business, and depends on locks and a re-read.

// rotateAfterEntries is how far one half may run before the ring turns. A seqno
// span, so deciding costs only arithmetic on the registry row. Small enough that
// a generation may be dropped between two checkpoints, discarding its pages
// unwritten; large enough that rotation is rare beside appends.
const rotateAfterEntries = 4096

// logStateColumns is the registry row in [scanLogState]'s order. Every column is
// an integer, so one added here and not there mis-reads the row silently.
const logStateColumns = `ordinal, last_seqno, trim_upto, cur_slot, cur_lo, prev_hi`

func scanLogState(row pgx.Row) (logState, error) {
	var st logState
	err := row.Scan(&st.ordinal, &st.lastSeqno, &st.trimUpto, &st.curSlot, &st.curLo, &st.prevHi)
	return st, err
}

// logState is a log's whole state: the registry row.
type logState struct {
	ordinal   int64
	lastSeqno Seqno
	trimUpto  Seqno
	curSlot   int16
	curLo     Seqno
	prevHi    Seqno
}

// prevSlot is the half a truncate empties and a rotation makes current.
func (st logState) prevSlot() int16 { return 1 - st.curSlot }

// prevEmpty reports whether a trim has passed everything the other half holds.
// prev_hi = 0 says there is no other half to empty.
func (st logState) prevEmpty() bool { return st.prevHi > 0 && st.trimUpto >= st.curLo-1 }

// ringFull reports whether the live half has run far enough to turn the ring.
// The span is counted from the half's own start, so an unappended log has
// last_seqno 0 against cur_lo 1: the subtraction wraps through the unsigned
// range and the +1 brings it back to zero.
func (st logState) ringFull() bool { return st.lastSeqno-st.curLo+1 >= rotateAfterEntries }

// canRotate reports whether the ring may turn. The half about to become current
// must be empty, and prev_hi = 0 is the only record that it is.
func (st logState) canRotate() bool { return st.prevHi == 0 && st.ringFull() }

// reclaimSteps is what may be done to this state, given whether the caller may
// truncate the entry tables. Both steps come from one answer because a state
// that may not be truncated may not rotate either: rotating into a half nothing
// emptied writes the log's newest entries over its oldest, and no read would
// say so.
func (st logState) reclaimSteps(mayTruncate bool) (truncate, rotate bool) {
	truncate = mayTruncate && st.prevEmpty()
	after := st
	if truncate {
		after.prevHi = 0
	}
	return truncate, after.canRotate()
}
