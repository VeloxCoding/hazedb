package hazedb

// Segmented WAL — opt-in (Options.WALRotateInterval > 0). The WAL becomes a
// directory of sealed segment files (seg-<n>.wal) plus one active segment that
// appends land in. A background ticker rotates: it seals the active segment and
// opens the next, so a drainer can consume sealed segments without ever touching
// the file being written. Single-file mode (dir == "") is unchanged.
//
// "Replay before append" is preserved: existing segments are replayed (each via
// its own read handle) before the active segment is opened, so the append path
// never shares a handle or a position with a replay reader. See docs/durability.md.

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	segPrefix = "seg-"
	segSuffix = ".wal"
)

// openWALSegmented opens the WAL in segmented mode. It creates dir and scans for
// the highest existing segment so the next active segment is opened after it
// (never re-appending into a segment replay will read). It does NOT open the
// active file or start tickers — the caller replays first, then calls
// startActiveSegment + the tickers, mirroring single-file openWAL.
func openWALSegmented(dir string, sync, syncPerWrite bool) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}
	maxSeg, err := scanMaxSeg(dir)
	if err != nil {
		return nil, err
	}
	return &wal{
		scratch:      make([]byte, 0, 4096),
		sync:         sync,
		syncPerWrite: syncPerWrite,
		dir:          dir,
		seg:          maxSeg, // active segment opens at maxSeg+1
	}, nil
}

// segPath returns the file path for segment number n.
func (w *wal) segPath(n uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s%010d%s", segPrefix, n, segSuffix))
}

// listSegments returns the existing segment numbers in dir, ascending.
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

// startActiveSegment opens a fresh active segment (seg+1) for appending. Called
// once after replay so appends never land in a segment replay read.
func (w *wal) startActiveSegment() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seg++
	f, err := os.OpenFile(w.segPath(w.seg), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open segment %q: %w", w.segPath(w.seg), err)
	}
	w.f = f
	if w.bw == nil {
		w.bw = bufio.NewWriterSize(f, 64<<10)
	} else {
		w.bw.Reset(f)
	}
	w.segHasData = false
	return nil
}

// replayAll replays every existing segment in ascending order (segmented mode)
// or the single file (single-file mode) into apply. Each segment is read through
// its own short-lived handle, so replay never disturbs the append handle.
func (w *wal) replayAll(apply func(recType uint8, payload []byte) error) error {
	if w.dir == "" {
		return w.replay(apply)
	}
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, n := range segs {
		f, err := os.Open(w.segPath(n))
		if err != nil {
			return err
		}
		err = w.replayFile(f, apply)
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// replayFrom replays only segments numbered above minSeg, ascending. Used by
// SQLite-backed recovery: segments at or below the drained cursor are already in
// the mirror and must not be re-applied to memory.
func (w *wal) replayFrom(minSeg uint64, apply func(recType uint8, payload []byte) error) error {
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, n := range segs {
		if n <= minSeg {
			continue
		}
		f, err := os.Open(w.segPath(n))
		if err != nil {
			return err
		}
		err = w.replayFile(f, apply)
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// removeDrainedSegments deletes sealed segments at or below minSeg — boot
// housekeeping for the crash window between a drain commit and the file delete.
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

// rotate seals the active segment (flush, fsync when WALSync, close) and opens
// the next. No-op in single-file mode, on a sticky error, or when the active
// segment holds no records (so idle ticks create no empty segments). Takes w.mu,
// so it serialises with appends — a rotation never splits a record.
func (w *wal) rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dir == "" || w.err != nil || !w.segHasData {
		return w.err
	}
	if err := w.bw.Flush(); err != nil {
		w.err = fmt.Errorf("wal: rotate flush: %w", err)
		return w.err
	}
	if w.sync {
		if err := w.f.Sync(); err != nil {
			w.err = fmt.Errorf("wal: rotate sync: %w", err)
			return w.err
		}
		w.dirtySinceSync = false
	}
	if err := w.f.Close(); err != nil {
		w.err = fmt.Errorf("wal: rotate close: %w", err)
		return w.err
	}
	w.seg++
	f, err := os.OpenFile(w.segPath(w.seg), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		w.err = fmt.Errorf("wal: rotate open %q: %w", w.segPath(w.seg), err)
		return w.err
	}
	w.f = f
	w.bw.Reset(f)
	w.segHasData = false
	return nil
}

// sealedSegments returns the numbers of segments that are sealed (closed) and
// therefore safe to read without touching the open active segment: every
// segment with a number strictly below the active one. Ascending order.
func (w *wal) sealedSegments() ([]uint64, error) {
	w.mu.Lock()
	active := w.seg
	w.mu.Unlock()
	segs, err := listSegments(w.dir)
	if err != nil {
		return nil, err
	}
	out := segs[:0:0]
	for _, n := range segs {
		if n < active {
			out = append(out, n)
		}
	}
	return out, nil
}

// startRotateTicker launches the background rotation goroutine. interval <= 0
// (or single-file mode) leaves a single growing active segment.
func (w *wal) startRotateTicker(interval time.Duration) {
	if interval <= 0 || w.dir == "" {
		return
	}
	w.rotateStop = make(chan struct{})
	w.wg.Add(1)
	go w.rotateLoop(interval)
}

func (w *wal) rotateLoop(interval time.Duration) {
	defer w.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-w.rotateStop:
			return
		case <-t.C:
			_ = w.rotate() // a sticky error surfaces on the next append
		}
	}
}
