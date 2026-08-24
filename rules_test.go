package pgnotch

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// The rules below are pure functions of values, and this file is the only place
// they are reachable: every other test here drives a live PostgreSQL, which
// takes one path of each per test.

// TestAppendRefusalRanksFencedFirst is the promise [Store.Append] documents:
// where more than one refusal applies, [ErrFenced] wins, because
// [ErrAlreadyWritten] is an ack and a writer that has lost its log would read it
// as one. The last three rows are that ranking and nothing else reaches them.
func TestAppendRefusalRanksFencedFirst(t *testing.T) {
	const held = Epoch(4)

	cases := []struct {
		name        string
		first, last Seqno
		found       appendState
		want        error
	}{
		{"nobody has fenced it", FirstSeqno, FirstSeqno, appendState{}, ErrFenced},
		{"fenced at another epoch", 5, 5, appendState{owner: held + 1, written: 4}, ErrFenced},
		{"seqno taken", 5, 5, appendState{owner: held, written: 5}, ErrAlreadyWritten},
		{"predecessor missing", 5, 5, appendState{owner: held, written: 3}, ErrGap},
		{"may proceed", 5, 7, appendState{owner: held, written: 4}, nil},
		{"a log's first entry has no predecessor", FirstSeqno, FirstSeqno, appendState{owner: held}, nil},

		// Both refusals hold in each of these.
		{"fenced outranks already-written", 5, 5, appendState{owner: held + 1, written: 9}, ErrFenced},
		{"fenced outranks a gap", 5, 5, appendState{owner: held + 1, written: 1}, ErrFenced},
		{"unfenced outranks already-written", 5, 5, appendState{owner: 0, written: 9}, ErrFenced},

		// A batch overlapping the log only partly is refused whole.
		{"a batch straddling the tail", 5, 9, appendState{owner: held, written: 6}, ErrAlreadyWritten},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := appendRefusal("a-log", held, c.first, c.last, c.found)
			if c.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, c.want)
		})
	}
}

// TestFenceRefusalAdmitsTheSameEpochTwice is what lets a restart replay its
// acquire: fencing at the epoch a log already carries is a no-op, not a failure.
func TestFenceRefusalAdmitsTheSameEpochTwice(t *testing.T) {
	cases := []struct {
		name         string
		held, taking Epoch
		refused      bool
	}{
		{"a higher epoch takes the log", 4, 5, false},
		{"the same epoch is a replayed acquire", 4, 4, false},
		{"a lower epoch is refused", 5, 4, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := fenceRefusal("a-log", c.held, c.taking)
			if c.refused {
				require.ErrorIs(t, err, ErrFenced)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestChunksOfSplitsAtThePageBoundary pins what keeps a row inside a page: with
// `bytea STORAGE PLAIN` a row above the ceiling is refused by PostgreSQL rather
// than moved out of line.
func TestChunksOfSplitsAtThePageBoundary(t *testing.T) {
	cases := []struct {
		name  string
		size  int
		parts int
	}{
		{"an empty entry is one empty chunk, not none", 0, 1},
		{"under the ceiling", 1, 1},
		{"at the ceiling", MaxEntryChunk, 1},
		{"one byte over", MaxEntryChunk + 1, 2},
		{"an exact multiple", 3 * MaxEntryChunk, 3},
		{"a remainder", 3*MaxEntryChunk + 1, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// A ramp rather than zeroes: chunks reordered or duplicated would
			// satisfy a length check and an all-zero comparison both.
			payload := make([]byte, c.size)
			for i := range payload {
				payload[i] = byte(i)
			}

			parts := chunksOf(payload)
			require.Len(t, parts, c.parts)
			for i, part := range parts {
				require.LessOrEqualf(t, len(part), MaxEntryChunk,
					"chunk %d is over the %d a row may hold", i, MaxEntryChunk)
			}
			// bytes.Equal rather than require.Equal: joining one empty chunk
			// gives nil, which is not a difference from empty.
			require.True(t, bytes.Equal(payload, bytes.Join(parts, nil)),
				"the payload did not survive the split")
		})
	}
}

// TestChunkArraysNumbersEveryRow: an entry above one chunk must not shift the
// seqno of the entry after it, and chunk indexes restart per entry —
// [Store.ReadFrom] reassembles on exactly those two facts.
func TestChunkArraysNumbersEveryRow(t *testing.T) {
	seqnos, chunks, parts := chunkArrays(10, [][]byte{
		make([]byte, 5),
		make([]byte, 2*MaxEntryChunk+1), // three rows
		nil,                             // one row, empty
	})

	require.Equal(t, []int64{10, 11, 11, 11, 12}, seqnos)
	require.Equal(t, []int16{0, 0, 1, 2, 0}, chunks)
	require.Len(t, parts, len(seqnos))
}

// TestRingFullCountsFromTheHalfsOwnStart matters most at zero: a log nobody has
// appended to has last_seqno 0 against cur_lo 1, so the subtraction runs
// backwards through the unsigned range before the +1 brings it to zero. Right by
// two's complement, and by nothing a reader can see.
func TestRingFullCountsFromTheHalfsOwnStart(t *testing.T) {
	cases := []struct {
		name string
		st   logState
		full bool
	}{
		{"a log nobody has appended to", logState{lastSeqno: 0, curLo: FirstSeqno}, false},
		{"one entry short", logState{lastSeqno: rotateAfterEntries - 1, curLo: FirstSeqno}, false},
		{"exactly a half's worth", logState{lastSeqno: rotateAfterEntries, curLo: FirstSeqno}, true},
		{"counted from the half, not the log", logState{lastSeqno: 10 * rotateAfterEntries, curLo: 10*rotateAfterEntries - 2}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.full, c.st.ringFull())
		})
	}
}

// TestPrevEmptyNeedsAnOtherHalfToEmpty states the zero-value: prev_hi = 0 says
// there is no other half, not that it is empty and ready to truncate.
func TestPrevEmptyNeedsAnOtherHalfToEmpty(t *testing.T) {
	cases := []struct {
		name  string
		st    logState
		empty bool
	}{
		{"there is no other half yet", logState{prevHi: 0, trimUpto: 9999, curLo: FirstSeqno}, false},
		{"the trim has not reached it", logState{prevHi: 100, trimUpto: 98, curLo: 101}, false},
		{"the trim names its last entry", logState{prevHi: 100, trimUpto: 100, curLo: 101}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.empty, c.st.prevEmpty())
		})
	}
}

// TestCanRotateNeedsTheOtherHalfEmptied: prev_hi = 0 is the only record that the
// half about to become current holds nothing.
func TestCanRotateNeedsTheOtherHalfEmptied(t *testing.T) {
	full := Seqno(rotateAfterEntries)
	require.True(t, logState{lastSeqno: full, curLo: FirstSeqno, prevHi: 0}.canRotate())
	require.False(t, logState{lastSeqno: full, curLo: FirstSeqno, prevHi: 40}.canRotate(),
		"rotating into a half that still holds a generation writes over it")
}

// TestReclaimStepsNeverRotatesOnAnEmptyingThatDidNotHappen covers the wiring a
// driver test cannot reach: one trim advancing the watermark between another
// trim's read and that trim's transaction.
//
// The last row is the one that matters. Without the tables locked there is no
// truncate, and without the truncate there must be no turn — the entries it
// would write over are all below the trim watermark, so every read filters them
// out and nothing downstream would ever say so.
func TestReclaimStepsNeverRotatesOnAnEmptyingThatDidNotHappen(t *testing.T) {
	full := Seqno(rotateAfterEntries)
	cases := []struct {
		name             string
		mayTruncate      bool
		st               logState
		truncate, rotate bool
	}{
		{"nothing to do", true, logState{lastSeqno: 10, curLo: FirstSeqno}, false, false},
		{"the other half is trimmed away, the live one is short", true,
			logState{lastSeqno: 10, curLo: 6, prevHi: 5, trimUpto: 5}, true, false},
		{"emptying the other half is what admits the turn", true,
			logState{lastSeqno: full + 5, curLo: 6, prevHi: 5, trimUpto: 5}, true, true},
		{"full, but nothing has trimmed the other half", true,
			logState{lastSeqno: full + 5, curLo: 6, prevHi: 5}, false, false},
		{"no lock needed: the other half is already gone", false,
			logState{lastSeqno: full, curLo: FirstSeqno, prevHi: 0}, false, true},
		{"no lock, and the state became truncatable since the gate ran", false,
			logState{lastSeqno: full + 5, curLo: 6, prevHi: 5, trimUpto: 5}, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			truncate, rotate := c.st.reclaimSteps(c.mayTruncate)
			require.Equal(t, c.truncate, truncate, "truncate")
			require.Equal(t, c.rotate, rotate, "rotate")
		})
	}
}
