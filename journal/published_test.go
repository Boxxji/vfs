package journal_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// A wrong Published (zero from a code path that failed to read the Head, a
// stale Head, the Head of another volume) must not destroy synced, unpublished
// records. Open refuses it and touches nothing, so a later mount with the right
// Head still replays 7 to 9.
func TestWrongPublishedDoesNotDestroyAcknowledgedRecords(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9) // 1 to 9 acknowledged durable
	publish(t, j, 6)
	if err := j.Reclaim(); err != nil { // the segments of 1 to 6 are gone; 7 to 9 are unpublished
		t.Fatal(err)
	}
	closeWithin(t, j)

	// The Volume mounts with the wrong Head sequence (0 is the classic).
	if j2, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrMismatch) {
		if err == nil {
			j2.Close()
		}
		t.Fatalf("Open with the store at 0: %v, want ErrMismatch", err)
	}

	// The operator fixes the Head and mounts again: 7, 8, 9 must still be there.
	j3 := open(t, small(d, newClock(), 6))
	defer j3.Close()
	if got, want := seqs(replayed(t, j3)), []uint64{7, 8, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acknowledged records 7 to 9 after a mount with a wrong Head and a correct one: %v", got)
	}
}

// Sequence numbers mean something only inside one lineage of the journal. Node
// A acknowledged 1 to 10 (synced) under round 1 and published up to 5. A
// takeover let node B continue from the Head at 5 and publish up to 8 with its
// own records. When the volume comes back to A, A opens its old journal with
// the Head at 8, under round 2, and must not replay its records 9 and 10 on top
// of B's state.
func TestStaleLineageIsNotReplayedOnANewerHead(t *testing.T) {
	a := crash.New()
	first := small(a, newClock(), 0)
	first.Lineage = []byte("volume 7 round 1")
	j := open(t, first)
	var last uint64
	for i := 1; i <= 10; i++ {
		last = mustAppend(t, j, []byte{byte('A'), byte(i)})
	}
	mustSync(t, j, last)
	closeWithin(t, j)

	// The Head says 8: B committed 6, 7, 8. A's journal knows nothing of B.
	second := small(a, newClock(), 8)
	second.Lineage = []byte("volume 7 round 2")
	if j, err := journal.Open("", second); !errors.Is(err, journal.ErrLineage) {
		if err == nil {
			closeWithin(t, j)
		}
		t.Fatalf("A's records were opened under the newer round: %v", err)
	}
}
