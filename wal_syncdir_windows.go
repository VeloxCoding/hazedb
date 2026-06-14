//go:build windows

package hazedb

// fsyncDir is a no-op on Windows: FlushFileBuffers rejects a directory handle, so
// directory-entry durability cannot be requested there. The atomic rename still
// gives crash-consistency for a segment's contents; only the entry's survival
// across power loss is unbacked.
func fsyncDir(string) error { return nil }
