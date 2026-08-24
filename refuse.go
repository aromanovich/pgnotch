package pgnotch

import "fmt"

// The checks made before anything reaches the database, and the diagnosis given
// once the database has said what it found. They are separated from the
// statements that find it out because the *order* of the three refusals is a
// promise to the caller rather than an implementation detail, and an order
// re-derived at each call site is one that drifts at one of them.

// checkFence refuses a fence whose arguments this package does not admit.
func checkFence(id LogID, epoch Epoch) error {
	if err := checkLogID(id); err != nil {
		return err
	}
	if epoch == 0 {
		return fmt.Errorf("pgnotch: fencing %q: %w", id, ErrZeroEpoch)
	}
	return nil
}

// fenceRefusal diagnoses a fence against the epoch the log is already fenced
// at, and returns nil when the fence may proceed. Equal epochs proceed: fencing
// at the epoch a log already carries is a no-op rather than a failure, so a
// process restart without a change of ownership can replay its acquire.
func fenceRefusal(id LogID, held, epoch Epoch) error {
	if held > epoch {
		return fmt.Errorf("%w: %q is fenced at epoch %d, cannot fence at %d",
			ErrFenced, id, held, epoch)
	}
	return nil
}

// checkAppend refuses an append whose arguments this package does not admit and
// returns the batch's last seqno.
func checkAppend(id LogID, epoch Epoch, first Seqno, payloads int) (last Seqno, err error) {
	if err := checkLogID(id); err != nil {
		return 0, err
	}
	switch {
	case epoch == 0:
		return 0, fmt.Errorf("pgnotch: appending to %q: %w", id, ErrZeroEpoch)
	case first < FirstSeqno:
		return 0, fmt.Errorf("pgnotch: appending to %q: seqno %d is below the first seqno %d",
			id, first, FirstSeqno)
	case payloads == 0:
		return 0, fmt.Errorf("pgnotch: appending to %q: no payloads", id)
	}
	return first + Seqno(payloads) - 1, nil
}

// appendState is what has been found out about an append that was not
// performed, which is the registry row and nothing else.
type appendState struct {
	// owner is the epoch the log is fenced at. Zero means nobody has fenced it.
	owner Epoch
	// written is the log's last seqno, and answers the other two refusals
	// both: a batch overlaps what is written when it starts at or below this,
	// and the entry below the batch is missing when this does not reach it. A
	// batch starting at [FirstSeqno] has no predecessor to miss, which needs no
	// case of its own — seqno zero is not an entry, and nothing is below it.
	written Seqno
}

// appendRefusal diagnoses what was found, and returns nil when the append may
// proceed.
//
// The order is the promise: [ErrFenced] outranks the rest because
// [ErrAlreadyWritten] is an ack, and a writer that has already lost its log
// would take the word of the writer which took it as its own high-water mark.
func appendRefusal(id LogID, epoch Epoch, first, last Seqno, found appendState) error {
	switch {
	case found.owner == 0:
		return fmt.Errorf("%w: %q is not fenced, cannot append at epoch %d",
			ErrFenced, id, epoch)
	case found.owner != epoch:
		return fmt.Errorf("%w: %q is fenced at epoch %d, cannot append at %d",
			ErrFenced, id, found.owner, epoch)
	case found.written >= first:
		return fmt.Errorf("%w: %q is written to seqno %d, and the batch [%d..%d] starts at or below it",
			ErrAlreadyWritten, id, found.written, first, last)
	case found.written < first-1:
		return fmt.Errorf("%w: %q has no seqno %d, cannot append at %d",
			ErrGap, id, first-1, first)
	}
	return nil
}

// checkTrim answers whether a trim has entries to reach. An upTo below
// [FirstSeqno] reaches none: what lives down there is this package's own
// bookkeeping, and a trim may never take it.
func checkTrim(id LogID, upTo Seqno) (proceed bool, err error) {
	if err := checkLogID(id); err != nil {
		return false, err
	}
	return upTo >= FirstSeqno, nil
}

// checkRead refuses a read whose arguments this package does not admit and
// returns the seqno it starts at: a from below [FirstSeqno] reads from
// [FirstSeqno], which is a clamp rather than a refusal because seqnos below it
// are not entries.
func checkRead(id LogID, from Seqno, limit int) (Seqno, error) {
	if err := checkLogID(id); err != nil {
		return 0, err
	}
	if limit <= 0 {
		return 0, fmt.Errorf("pgnotch: reading %q from %d: limit %d is not positive", id, from, limit)
	}
	return max(from, FirstSeqno), nil
}
