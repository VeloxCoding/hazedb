package hazedb

import (
	"path/filepath"
	"testing"
)

// applyDefaults gives the companion a file path inside WALPath when WAL is on,
// and leaves it empty (no companion) when there is no WAL — never in-memory.
func TestCompanionDefaultResolution(t *testing.T) {
	walOn := Options{WALPath: "/var/lib/hz/wal"}
	walOn.applyDefaults()
	if want := filepath.Join("/var/lib/hz/wal", "hazedb.db"); walOn.CompanionPath != want {
		t.Fatalf("WAL-on companion: got %q, want %q", walOn.CompanionPath, want)
	}

	noWAL := Options{}
	noWAL.applyDefaults()
	if noWAL.CompanionPath != "" {
		t.Fatalf("no-WAL companion: got %q, want empty (no companion, no in-memory)", noWAL.CompanionPath)
	}

	// An explicit path is left untouched.
	explicit := Options{WALPath: "/x/wal", CompanionPath: "/data/ops.db"}
	explicit.applyDefaults()
	if explicit.CompanionPath != "/data/ops.db" {
		t.Fatalf("explicit companion path overwritten: %q", explicit.CompanionPath)
	}
}
