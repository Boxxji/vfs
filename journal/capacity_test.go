package journal_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
)

func dirBytes(t *testing.T, dir string) (total int64, files int) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		info, err := e.Info()
		if os.IsNotExist(err) { // a Create renamed its temporary file between ReadDir and Info
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
		files++
	}
	return
}

// On the real file layer: fill the journal to ErrFull, then drain it,
// restart it twice, and check that the bytes on disk never exceed Capacity.
func TestCapacityOnRealFiles(t *testing.T) {
	dir := scratch(t)
	const seg = 256 << 10
	opt := journal.Options{Capacity: 8 * seg, Segment: seg}
	for round := 0; round < 3; round++ {
		j, err := journal.Open(dir, opt)
		if err != nil {
			t.Fatal(err)
		}
		rec := make([]byte, 40<<10)
		var full bool
		deadline := time.Now().Add(20 * time.Second)
		for !full && time.Now().Before(deadline) {
			if _, err := j.Append(ctx, rec); errors.Is(err, journal.ErrFull) {
				full = true
			} else if err != nil {
				t.Fatal(err)
			}
			if b, _ := dirBytes(t, dir); b > opt.Capacity {
				t.Fatalf("round %d: %d bytes on disk, capacity %d", round, b, opt.Capacity)
			}
		}
		if !full {
			t.Fatal("never full")
		}
		b, n := dirBytes(t, dir)
		t.Logf("round %d: full with %d bytes in %d files (capacity %d)", round, b, n, opt.Capacity)
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		if round == 1 { // a restart that finds a full journal and a Head that has caught up
			opt.Published = 0
		}
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(matches) != 0 {
		t.Fatalf("temporary files left: %v", matches)
	}
}
