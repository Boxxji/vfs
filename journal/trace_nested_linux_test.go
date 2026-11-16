package journal_test

import (
	"path/filepath"
	"testing"
)

// LOW. Files makes the entry of the journal's directory durable by syncing its
// parent, but MkdirAll may have created several levels: the entries of the
// ancestors it made are not synced. POSIX gives no durability for them; ext4
// and xfs happen to commit them with the next fsync.
func TestNestedDirectoryEntriesAreSynced(t *testing.T) {
	t.Parallel()
	base, err := filepath.EvalSymlinks(scratch(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(base, "x", "y", "z")
	_, calls := tracedRun(t, dir, nil)
	for _, parent := range []string{filepath.Join(base, "x", "y"), filepath.Join(base, "x"), base} {
		if index(calls, 0, func(c call) bool { return c.name == "fsync" && c.path == parent }) < 0 {
			t.Errorf("Files created a directory whose entry is in %s, and never synced it", parent)
		}
	}
}
