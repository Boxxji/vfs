//go:build fuse && (linux || darwin)

package sqlitecheck

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/hanzoai/sqlite"
	"github.com/luxfi/age"

	"github.com/hanzoai/vfs/internal/mounttest"
)

// TestMountChild is the server process of the mount tests (see internal/mounttest).
func TestMountChild(t *testing.T) { mounttest.Child(t) }

// TestFUSESQLite runs a real SQLite database directly on the mount: create,
// insert, close, release the mount, mount again, and the database is whole.
func TestFUSESQLite(t *testing.T) {
	mounttest.NeedFUSE(t)
	store, mp := mounttest.NewMountpoint(t)
	key, _ := age.GenerateHybridIdentity()
	dbPath := filepath.Join(mp, "test.db")

	m := mounttest.MountAt(t, store, mp, key, 1)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE coins (sym TEXT PRIMARY KEY, name TEXT, decimals INT)`); err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := db.Exec(`INSERT INTO coins VALUES (?, ?, ?)`, fmt.Sprintf("USDC_%d", i), "USD Coin", 6); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	m.Stop(t)

	m = mounttest.MountAt(t, store, mp, key, 2)
	defer m.Stop(t)
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ok string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&ok); err != nil || !strings.EqualFold(ok, "ok") {
		t.Fatalf("integrity_check = %q, %v", ok, err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM coins`).Scan(&n); err != nil || n != 100 {
		t.Fatalf("count = %d, %v; want 100", n, err)
	}
	t.Logf("%d rows survived release and remount, integrity_check=%s", n, ok)
}
