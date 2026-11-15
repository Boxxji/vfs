package vfs_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/hanzoai/sqlite"

	"github.com/hanzoai/vfs"
)

// TestSQLiteRoundTrip writes a real SQLite database through a Volume the way
// SQLite does, in page-sized writes at page offsets and out of order, commits,
// reads it back from a fresh read-only Open, and checks SQLite itself accepts the
// result: the same row count and a clean integrity_check.
func TestSQLiteRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	refPath := filepath.Join(tmp, "ref.db")

	db, err := sql.Open("sqlite", refPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE coins (sym TEXT PRIMARY KEY, name TEXT, decimals INT)`); err != nil {
		t.Fatal(err)
	}
	for i := range 20000 {
		if _, err := db.Exec(`INSERT INTO coins VALUES (?, ?, ?)`, fmt.Sprintf("SYM_%d", i), strings.Repeat("name ", 1+i%9), i%19); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ref, err := os.ReadFile(refPath)
	if err != nil {
		t.Fatal(err)
	}
	const page = 4096
	if len(ref) < 100*page {
		t.Fatalf("reference database is only %d bytes", len(ref))
	}
	t.Logf("reference database: %d bytes, %d pages", len(ref), len(ref)/page)

	be, c, v := newMem(t)
	in, err := v.Create(ctx, vfs.RootInode, "test.db", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f, err := v.Open(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pages := len(ref) / page
	for i := range pages { // backwards, as a b-tree's writes land
		p := pages - 1 - i
		if _, err := f.WriteAt(ctx, ref[p*page:(p+1)*page], int64(p*page)); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, v)

	got := readAll(t, reopen(t, be, c), in.ID)
	mustEqual(t, "database bytes", got, ref)

	rebuilt := filepath.Join(tmp, "rebuilt.db")
	if err := os.WriteFile(rebuilt, got, 0o644); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM coins`).Scan(&count); err != nil || count != 20000 {
		t.Fatalf("count = %d, %v; want 20000", count, err)
	}
	var ok string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&ok); err != nil || !strings.EqualFold(ok, "ok") {
		t.Fatalf("integrity_check = %q, %v", ok, err)
	}
}
