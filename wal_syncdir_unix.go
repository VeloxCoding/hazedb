//go:build !windows

package hazedb

import "os"

// fsyncDir fsyncs a directory so a freshly renamed entry inside it survives power
// loss. On Unix this is a supported, meaningful operation, so the error is
// returned for the caller to act on (the WAL makes it sticky).
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
