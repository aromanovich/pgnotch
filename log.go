package pgnotch

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// LogID names one log: an opaque key this package never interprets. Logs under
// different ids share nothing.
//
// It is stored as `text` and never appears in an identifier — a log's tables
// are named from an ordinal assigned at creation — so the constraints are only
// that it be at most [MaxLogIDBytes], not empty, valid UTF-8 and free of NUL.
type LogID string

// MaxLogIDBytes bounds a [LogID], so an id too long is refused where it is
// passed rather than at the first append that overflows an index entry.
const MaxLogIDBytes = 255

// Seqno is the position of an entry in one log: an LSN the owner assigns
// itself. Total order within a log, no gaps.
type Seqno uint64

// FirstSeqno is the seqno of a log's first entry. Seqnos below it are not
// entries; they are reserved for this package's own bookkeeping.
const FirstSeqno Seqno = 1

// Epoch is the ownership token every append carries. It may grow without an
// ownership change: a writer renewing its claim fences again at a higher epoch
// and keeps its log.
//
// Zero is not a valid epoch; see [ErrZeroEpoch]. Whoever hands epochs out owes
// this package a strictly greater epoch per acquire, since fencing cannot
// separate two writers holding the same one.
type Epoch uint64

// Entry is one record of a log.
type Entry struct {
	// Seqno is the entry's position in its log.
	Seqno Seqno
	// Epoch is the epoch its writer held when it appended the entry. Epochs
	// are non-decreasing along a log.
	Epoch Epoch
	// Payload is the bytes the caller appended, and is never nil for an entry
	// a read returns: an empty payload comes back empty, not missing. The
	// bytes alias neither this package's state nor another entry of the same
	// read, so the caller may keep them and decode into them in place.
	Payload []byte
}

// Errors a caller is expected to handle; anything else it can only report.
// Match with [errors.Is]; the errors returned wrap these with context.
var (
	// ErrFenced means the log is not the caller's to write: some other epoch
	// has fenced it, or the caller never fenced it at its own epoch. The caller
	// must stop writing.
	ErrFenced = errors.New("pgnotch: log is not fenced at this epoch")

	// ErrAlreadyWritten means a seqno the append asked for is taken. After an
	// append that failed ambiguously it says the write landed, and so is a
	// retry's success signal — provided the retry is the same batch: same first
	// seqno, same number of payloads, nothing added to it. A batch overlapping
	// the log only partly is refused whole. [ErrFenced] outranks it, so a
	// writer whose epoch grew across the ambiguity must replay under the epoch
	// it holds now, or read the log, to learn whether the first attempt landed.
	ErrAlreadyWritten = errors.New("pgnotch: seqno already written")

	// ErrGap means the append would leave a hole: the entry below the batch is
	// missing. Expected of a pipelined append that arrived out of order; retry
	// once the predecessor lands.
	ErrGap = errors.New("pgnotch: predecessor seqno is missing")

	// ErrNoSuchLog means the log has not been created. [Store.Fence] returns it
	// rather than creating one: see [Store.CreateLogs].
	ErrNoSuchLog = errors.New("pgnotch: no such log")

	// ErrZeroEpoch is what a caller that forgot to set an epoch gets, rather
	// than an [ErrFenced] that reads like a lost log: epoch 0 means "nobody
	// owns this".
	ErrZeroEpoch = errors.New("pgnotch: epoch 0 is not a valid epoch")

	// ErrNotMigrated means the schema has no tables of this package in it yet.
	// [Open] returns it rather than creating tables, so a process which is not
	// the one that deploys cannot become the one that migrates; call [Migrate]
	// and open again.
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
