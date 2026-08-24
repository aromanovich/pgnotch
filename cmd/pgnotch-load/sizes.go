package main

import (
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"

	"github.com/aromanovich/pgnotch"
)

// maxPayload bounds one entry, so a mistyped size is refused where it is passed
// rather than at the append that tries to send a gigabyte through the driver.
const maxPayload = 16 << 20

// sizeClass is one payload length and how often it is drawn against the others.
type sizeClass struct {
	bytes  int
	weight int
}

// sizes is the payload-size distribution a run draws every entry's length from:
// the point of the tool being that a log's entries are not all one size, and
// that some of them are over [pgnotch.MaxEntryChunk] and so become several rows.
type sizes struct {
	classes []sizeClass
	// cum holds the running weight totals, so a draw is one search.
	cum   []int
	total int
}

// parseSizes reads a distribution written as `size[:weight]` classes separated
// by commas, with `k` and `m` suffixes for KiB and MiB — `1k:9,32k:1` being one
// entry in ten of 32 KiB. A class without a weight weighs 1.
func parseSizes(spec string) (*sizes, error) {
	s := &sizes{}
	for field := range strings.SplitSeq(spec, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, fmt.Errorf("empty size class in %q", spec)
		}
		size, weight, _ := strings.Cut(field, ":")
		bytes, err := parseBytes(size)
		if err != nil {
			return nil, err
		}
		n := 1
		if weight != "" {
			if n, err = strconv.Atoi(strings.TrimSpace(weight)); err != nil || n <= 0 {
				return nil, fmt.Errorf("the weight in %q is not a positive number", field)
			}
		}
		s.total += n
		s.classes = append(s.classes, sizeClass{bytes: bytes, weight: n})
		s.cum = append(s.cum, s.total)
	}
	return s, nil
}

func parseBytes(text string) (int, error) {
	text = strings.TrimSpace(text)
	unit := 1
	switch {
	case strings.HasSuffix(text, "k"), strings.HasSuffix(text, "K"):
		unit, text = 1<<10, text[:len(text)-1]
	case strings.HasSuffix(text, "m"), strings.HasSuffix(text, "M"):
		unit, text = 1<<20, text[:len(text)-1]
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("the size %q is not a number of bytes", text)
	}
	// Compared before the multiplication, which a mistyped `9999999999m` would
	// otherwise overflow into a small number of bytes.
	if n > maxPayload/unit {
		return 0, fmt.Errorf("the size %q is over the %d bytes this tool admits per entry", text, maxPayload)
	}
	return n * unit, nil
}

// draw is the length of the next entry.
func (s *sizes) draw(rnd *rand.Rand) int {
	// The first class whose running total is above the draw owns it.
	i, _ := slices.BinarySearch(s.cum, rnd.IntN(s.total)+1)
	return s.classes[i].bytes
}

func (s *sizes) largest() int {
	largest := 0
	for _, c := range s.classes {
		largest = max(largest, c.bytes)
	}
	return largest
}

// mean is the expected entry length, which with the rate is the byte rate the
// run is asking of the database.
func (s *sizes) mean() float64 {
	sum := 0
	for _, c := range s.classes {
		sum += c.bytes * c.weight
	}
	return float64(sum) / float64(s.total)
}

// chunked is the share of entries that will not fit one row, which is the part
// of the load the operator asked for by naming a class above the chunk size.
func (s *sizes) chunked() float64 {
	over := 0
	for _, c := range s.classes {
		if c.bytes > pgnotch.MaxEntryChunk {
			over += c.weight
		}
	}
	return float64(over) / float64(s.total)
}

func (s *sizes) String() string {
	parts := make([]string, 0, len(s.classes))
	for _, c := range s.classes {
		parts = append(parts, fmt.Sprintf("%s×%d", humanBytes(uint64(c.bytes)), c.weight))
	}
	return strings.Join(parts, " ")
}

// corpus is the bytes every payload is a window into: drawn once at start-up so
// that generating them is never what limits the rate, and read-only afterwards
// so that every writer can share the one buffer. pgnotch treats payloads as
// opaque and PostgreSQL stores them uncompressed, so what is in them decides
// nothing — but zeros would compress in the WAL, and a load whose bytes cost
// less than the operator's would be a lie.
type corpus struct{ buf []byte }

// newCorpus holds enough bytes that entries of the largest class still start at
// many different offsets, so no two consecutive entries are the same bytes.
func newCorpus(largest int) (*corpus, error) {
	size := max(1<<20, 2*largest)
	buf := make([]byte, size)
	if _, err := crand.Read(buf); err != nil {
		return nil, fmt.Errorf("unable to draw the payload corpus: %w", err)
	}
	return &corpus{buf: buf}, nil
}

// payload is n bytes of the corpus, empty n included. The result is the
// caller's to send and not to modify: pgnotch copies what it encodes and keeps
// nothing.
func (c *corpus) payload(rnd *rand.Rand, n int) []byte {
	off := rnd.IntN(len(c.buf) - n + 1)
	return c.buf[off : off+n]
}
