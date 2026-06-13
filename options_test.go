package hazedb

import (
	"path/filepath"
	"testing"
)

// applyDefaults resolves an empty CompanionPath to a FILE: inside WALPath when WAL
// is on, and to the working-directory default otherwise. (The test init sets the
// no-WAL default to ":memory:", so that branch resolves to ":memory:" here; the
// production default is the real file asserted below.)
func TestCompanionDefaultResolution(t *testing.T) {
	walOn := Options{WALPath: "/var/lib/hz/wal"}
	walOn.applyDefaults()
	if want := filepath.Join("/var/lib/hz/wal", "hazedb.db"); walOn.CompanionPath != want {
		t.Fatalf("WAL-on companion: got %q, want %q", walOn.CompanionPath, want)
	}

	walOff := Options{}
	walOff.applyDefaults()
	if walOff.CompanionPath != noWALCompanionDefault {
		t.Fatalf("WAL-off companion: got %q, want the no-WAL default %q", walOff.CompanionPath, noWALCompanionDefault)
	}

	// Production's no-WAL default is a real file, not in-memory.
	if defaultCompanionFile != "hazedb.db" {
		t.Fatalf("default companion file: got %q, want hazedb.db", defaultCompanionFile)
	}

	// An explicit path is left untouched.
	explicit := Options{CompanionPath: "/data/ops.db"}
	explicit.applyDefaults()
	if explicit.CompanionPath != "/data/ops.db" {
		t.Fatalf("explicit companion path overwritten: %q", explicit.CompanionPath)
	}
}
