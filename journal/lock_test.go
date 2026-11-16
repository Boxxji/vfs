package journal_test

import (
	"errors"
	"testing"

	"github.com/hanzoai/vfs/journal"
)

// Nothing may run two journals on one directory: a restarted daemon beside one
// that is still shutting down, a double mount, a zombie after a takeover. Each
// would number the same sequences, and one of the two logs would later be
// discarded together with the records it acknowledged as durable. The second
// Open fails, and the lock is gone with the first journal.
func TestSecondJournalOnALiveDirectory(t *testing.T) {
	dir := scratch(t)
	opt := journal.Options{Capacity: 8 * 4096, Segment: 4096}
	j1, err := journal.Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	var last uint64
	for i := 0; i < 5; i++ {
		last = mustAppend(t, j1, []byte("one"))
	}
	mustSync(t, j1, last)

	j2, err := journal.Open(dir, opt)
	if err == nil {
		j2.Close()
		t.Fatal("a second journal opened on a live directory")
	}
	if !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("second Open: %v, want ErrLocked", err)
	}
	if err := j1.Close(); err != nil {
		t.Fatal(err)
	}
	j3, err := journal.Open(dir, opt)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	defer j3.Close()
	if got := replayed(t, j3); len(got) != 5 {
		t.Fatalf("records after the refusals: %v", seqs(got))
	}
}
