package hazedb

// Segment files. The WAL is a directory of immutable born-sealed segments
// (seg-<n>.wal), written by wal.flushLocked via temp-file + atomic rename. This
// file owns segment naming, listing, and the replay / reclamation helpers the
// recovery and drain paths use. There is no "active" segment: every seg-*.wal is
// complete, so listing and replay never have to skip a file being written.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	segPrefix    = "seg-"
	segSuffix    = ".wal"
	segTmpSuffix = ".wal.tmp" // flushLocked writes seg-<n>.wal.tmp, then renames to seg-<n>.wal
)

// segPath returns the file path for segment number n.
func (w *wal) segPath(n uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s%010d%s", segPrefix, n, segSuffix))
}

// listSegments returns the existing segment numbers in dir, ascending. A *.tmp
// (a flush in progress, or a crash leftover) does not end in segSuffix and is
// ignored.
func listSegments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []uint64
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		n, err := strconv.ParseUint(name[len(segPrefix):len(name)-len(segSuffix)], 10, 64)
		if err != nil {
			continue // foreign file — ignore
		}
		segs = append(segs, n)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

// scanMaxSeg returns the highest existing segment number, or 0 if none.
func scanMaxSeg(dir string) (uint64, error) {
	segs, err := listSegments(dir)
	if err != nil {
		return 0, err
	}
	if len(segs) == 0 {
		return 0, nil
	}
	return segs[len(segs)-1], nil
}

// removeStaleTemps deletes leftover segment temp files (seg-<n>.wal.tmp) from a
// flush interrupted by a crash. Their bytes were never renamed into a seg-*.wal,
// so they belong to no committed segment and are safe — required — to drop before
// reopening. Only our own temp shape is removed; the SQLite companion and any
// foreign file sharing the directory are left untouched.
func removeStaleTemps(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir() || !isSegmentTemp(e.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// isSegmentTemp reports whether name is a segment temp file (seg-<digits>.wal.tmp)
// as written by flushLocked — the same numbering rigor as listSegments, so only a
// file this WAL could have created is ever a deletion candidate.
func isSegmentTemp(name string) bool {
	if !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segTmpSuffix) {
		return false
	}
	num := name[len(segPrefix) : len(name)-len(segTmpSuffix)]
	_, err := strconv.ParseUint(num, 10, 64)
	return err == nil
}

// replayFrom replays only segments numbered above minSeg, ascending. Used by
// SQLite-backed recovery: segments at or below the drained cursor are already in
// the mirror and must not be re-applied to memory.
func (w *wal) replayFrom(minSeg uint64, apply func(recType uint8, payload []byte) error, onCorrupt func(seg uint64, err error)) error {
	return w.replaySegments(minSeg, apply, onCorrupt)
}

// replaySegments replays segments with number > minSeg, ascending. The tail must
// be contiguous from minSeg+1: a missing number is errWALMissingSegment and
// aborts Open, because the higher segments depend on the lost one. A segment
// whose framing is bit-rot (bad magic / CRC mismatch, i.e. errWALFraming) applies
// its good prefix, reports the break via onCorrupt, and is skipped from that point
// on — recovery continues with the next segment instead of aborting. Every other
// error is fatal: a version mismatch, an unknown record type, or an undecodable
// but CRC-valid payload all carry intact, intentional bytes, so aborting Open is
// correct — silently dropping a committed record would be a data-loss bug.
func (w *wal) replaySegments(minSeg uint64, apply func(recType uint8, payload []byte) error, onCorrupt func(seg uint64, err error)) error {
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	expected := minSeg + 1
	for _, n := range segs {
		if n <= minSeg {
			continue
		}
		if n != expected {
			return fmt.Errorf("%w: missing segment %d before %d in undrained tail", errWALMissingSegment, expected, n)
		}
		expected = n + 1
		f, err := os.Open(w.segPath(n))
		if err != nil {
			return err
		}
		err = w.replayFile(f, apply)
		f.Close()
		switch {
		case errors.Is(err, errWALFraming):
			if onCorrupt != nil {
				onCorrupt(n, err)
			}
		case err != nil:
			return err
		}
	}
	return nil
}

// removeDrainedSegments deletes segments at or below minSeg — boot housekeeping
// for the crash window between a drain commit and the file delete.
func (w *wal) removeDrainedSegments(minSeg uint64) error {
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, n := range segs {
		if n <= minSeg {
			if err := os.Remove(w.segPath(n)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// sealedSegments returns every segment safe to drain. With born-sealed segments
// there is no active file being appended to, so that is simply all of them,
// ascending.
func (w *wal) sealedSegments() ([]uint64, error) {
	return listSegments(w.dir)
}
