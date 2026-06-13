package hazedb

import (
	"errors"
	"path/filepath"
	"testing"
)

// applyDefaults always gives the companion a file path: inside WALPath when WAL
// is on, else "hazedb.db" in the working directory. Never empty, never in-memory.
func TestCompanionDefaultResolution(t *testing.T) {
	walOn := Options{WALPath: "/var/lib/hz/wal"}
	walOn.applyDefaults()
	if want := filepath.Join("/var/lib/hz/wal", "hazedb.db"); walOn.CompanionPath != want {
		t.Fatalf("WAL-on companion: got %q, want %q", walOn.CompanionPath, want)
	}

	noWAL := Options{}
	noWAL.applyDefaults()
	if noWAL.CompanionPath != defaultCompanionFile {
		t.Fatalf("no-WAL companion: got %q, want %q (a file in the working dir)", noWAL.CompanionPath, defaultCompanionFile)
	}

	// An explicit path is left untouched.
	explicit := Options{WALPath: "/x/wal", CompanionPath: "/data/ops.db"}
	explicit.applyDefaults()
	if explicit.CompanionPath != "/data/ops.db" {
		t.Fatalf("explicit companion path overwritten: %q", explicit.CompanionPath)
	}
}

// The companion must be on disk: validate rejects every in-memory SQLite DSN, so
// it can never silently become a volatile store. An empty path is fine — it is a
// real file after applyDefaults.
func TestCompanionRejectsInMemory(t *testing.T) {
	rejected := []string{
		":memory:",
		"file::memory:",
		"file::memory:?cache=shared",
		"file:hazedb?mode=memory&cache=shared",
	}
	for _, p := range rejected {
		o := Options{CompanionPath: p}
		if err := o.validate(); !errors.Is(err, ErrCompanionInMemory) {
			t.Errorf("CompanionPath=%q: got err %v, want ErrCompanionInMemory", p, err)
		}
	}

	accepted := []string{"", "hazedb.db", "/var/lib/hz/companion.db", "./data/ops.sqlite"}
	for _, p := range accepted {
		o := Options{CompanionPath: p}
		if err := o.validate(); err != nil {
			t.Errorf("CompanionPath=%q: unexpected err %v", p, err)
		}
	}
}
