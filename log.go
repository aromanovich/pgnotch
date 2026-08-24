package pgnotch

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// LogID names one log. It is the caller's to choose and this package never
// interprets it: a queue name, a tenant's UUID, the decimal form of a partition
// number are all one thing here, an opaque key. Logs under different ids share
// nothing.
//
// It is stored as `text` and never appears in an identifier, so the only
// constraints are PostgreSQL's own: [MaxLogIDBytes] at most, not empty, and no
// NUL — see [Store.CreateLogs] for what a rejected one costs. The tables a log
// is kept in are named from an ordinal this package assigns when the log is
// created, which is what keeps a 200-byte id off a 63-byte identifier.
type LogID string

// MaxLogIDBytes bounds a [LogID]. It is a bound on the registry table's primary
// key rather than on anything this package needs, and it is stated so that an
// id too long is refused where it is passed rather than at the first append
// that overflows an index entry.
const MaxLogIDBytes = 255

// Seqno is the position of an entry in one log: an LSN the owner assigns
// itself. Total order within a log, no gaps.
type Seqno uint64

// FirstSeqno is the seqno of a log's first entry. Seqnos below it are not
// entries; they are reserved for whatever bookkeeping this package needs.
const FirstSeqno Seqno = 1

// Epoch is the ownership token every append carries. It may grow without an
// ownership change, so a writer renewing its claim fences again at a higher
// epoch and keeps its log.
//
// Zero is not a valid epoch; see [ErrZeroEpoch].
//
// Whoever hands epochs out owes this package a strictly greater epoch per
// acquire: fencing cannot separate two writers holding the same epoch, and they
// race for seqnos and lose. Anything that answers "who is the owner" with a
// number that only grows will do — a lease counter, a ZooKeeper czxid, a
// database sequence.
type Epoch uint64

// Entry is one record of a log.
type Entry struct {
	// Seqno is the entry's position in its log.
	Seqno Seqno
	// Epoch is the epoch its writer held when it appended the entry. Epochs
	// are non-decreasing along a log.
	Epoch Epoch
	// Payload is the bytes the caller appended, and is never nil for an entry
	// a read returns — an entry appended with an empty payload comes back with
	// an empty one, not a missing one.
	//
	// The bytes are the reader's to keep: a payload this package hands out
	// aliases neither its own state nor another entry of the same read, so a
	// caller may hold it past the next call and decode into it in place.
	Payload []byte
}

// Errors a caller is expected to handle. Anything else is an ordinary error and
// means a programming mistake or an infrastructure failure, both of which the
// caller can only report.
//
// Match with [errors.Is]; the errors returned wrap these with context.
var (
	// ErrFenced means the log is not the caller's to write: some other epoch
	// has fenced it, or the caller never fenced it at its own epoch. Ownership
	// is gone, or was never taken, and the caller must stop writing.
	ErrFenced = errors.New("pgnotch: log is not fenced at this epoch")

	// ErrAlreadyWritten means a seqno the append asked for is taken. After an
	// append that failed ambiguously — a dropped connection, a cancelled
	// context — it is the answer to "did it land?": it did, so retrying an
	// append is safe and this error is the retry's success signal.
	//
	// Reading it that way requires the retry to be the same batch: same first
	// seqno, same number of payloads, replayed before anything new is added to
	// it. A batch that overlaps the log only partly is refused whole and is a
	// caller bug. [ErrFenced] outranks it, so a writer whose epoch grew across
	// the ambiguity must replay under the epoch it holds now, or read the log,
	// to learn whether the first attempt landed.
	ErrAlreadyWritten = errors.New("pgnotch: seqno already written")

	// ErrGap means the append would leave a hole: the entry below the batch is
	// missing. It is the expected outcome of a pipelined append that arrived
	// out of order; retry once the predecessor lands.
	ErrGap = errors.New("pgnotch: predecessor seqno is missing")

	// ErrNoSuchLog means the log has not been created. [Store.Fence] returns it
	// rather than creating one, because a log is two PostgreSQL tables and an
	// id is an arbitrary string: see [Store.CreateLogs].
	ErrNoSuchLog = errors.New("pgnotch: no such log")

	// ErrZeroEpoch is what a caller that forgot to set an epoch gets, rather
	// than an [ErrFenced] that reads like a lost log. Epoch 0 is the "nobody
	// owns this" reading of an absent fence, so nothing can be claimed with it.
	ErrZeroEpoch = errors.New("pgnotch: epoch 0 is not a valid epoch")

	// ErrNotMigrated means the schema has no tables of this package in it yet. [Open]
	// returns it rather than creating tables, so that a process which is not
	// the one that deploys cannot become the one that migrates; a caller that
	// wants the other behaviour calls [Migrate] on it and opens again.
	ErrNotMigrated = errors.New("pgnotch: schema is not migrated")
)

// checkLogID refuses an id PostgreSQL could not store as the registry's key.
func checkLogID(id LogID) error {
	switch {
	case id == "":
		return errors.New("pgnotch: the log id is empty")
	case len(id) > MaxLogIDBytes:
		return fmt.Errorf("pgnotch: the log id is %d bytes, over the %d this package admits",
			len(id), MaxLogIDBytes)
	case strings.ContainsRune(string(id), 0):
		return fmt.Errorf("pgnotch: the log id %q has a NUL in it, which `text` cannot hold", string(id))
	case !utf8.ValidString(string(id)):
		return errors.New("pgnotch: the log id is not valid UTF-8")
	}
	return nil
}
