package journal_test

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// The crash tests pin a mark every round; production writes one every sixteenth
// round or megabyte, and a journal that is never closed leaves the last of them
// unwritten. Explore a workload that syncs after every append, with the
// production cadence and with others, so that the order of a mark and the sync
// that carries it, and the lag of the proof, are in the model.
func TestCrashExplorationWithProductionCadence(t *testing.T) {
	for _, every := range []int{16, 5, 1000} {
		t.Run(fmt.Sprintf("every%d", every), func(t *testing.T) {
			// MarkEvery is read by journals opened after the call: the exploration
			// opens them too, so the cadence stays set until the test ends.
			defer journal.MarkEvery(every)()
			r := newRun()
			j := open(t, r.opts(0))
			rng := rand.New(rand.NewSource(1))
			var visible, published uint64
			for i := 0; i < 90; i++ {
				visible = r.append(t, j, payload(uint64(rng.Intn(1000))))
				r.sync(t, j, visible)
				switch rng.Intn(12) {
				case 0, 1:
					published += uint64(rng.Intn(int(visible-published))) + 1
					r.publish(t, j, published)
				case 2, 3:
					r.reclaim(t, j)
				}
			}
			if err := closeBounded(t, j); err != nil {
				t.Fatal(err)
			}
			r.explore(t, 2)
		})
	}
}

// Marks are spaced out by rounds, but not by more than a megabyte: a journal
// that syncs big rounds, rarely, writes one anyway, so the proof does not lag
// by more than that however slowly the rounds come.
func TestAMarkIsWrittenAfterAMegabyteWhateverTheRounds(t *testing.T) {
	defer journal.MarkEvery(1000)()
	d := crash.New()
	j := open(t, journal.Options{Capacity: 8 << 20, Segment: 2 << 20, Disk: d, Clock: newClock()})
	big := make([]byte, 300<<10)
	for i := 1; i <= 4; i++ {
		mustSync(t, j, mustAppend(t, j, big))
	}
	// The fourth round brought the synced volume past a megabyte: its mark says
	// what the three rounds before it had made durable.
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost}) // no Close: only what was synced
	_, first, through, _ := preamble(t, img, "000000.seg")
	if first != 1 || through != 3 {
		t.Fatalf("after four rounds of 300 KiB the mark says through %d (first %d), want 3", through, first)
	}
	closeWithin(t, j)
}

// The order of segments that claim the same first sequence is by name however
// many segments there are: a sort that is not stable would order a tie by
// chance once there are enough of them.
func TestManySegmentsWithCopiesKeepTheLowerName(t *testing.T) {
	d := crash.New()
	opt := journal.Options{Capacity: 60 * 4096, Segment: 4096, Disk: d, Clock: newClock()}
	j := open(t, opt)
	var last uint64
	for i := 1; i <= 60; i++ {
		last = mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, last)
	closeWithin(t, j)
	names, _ := d.List()
	firsts := map[string]uint64{}
	for _, name := range names {
		if live, first, _, _ := preamble(t, d, name); live {
			firsts[name] = first
		}
	}
	if len(firsts) < 15 {
		t.Fatalf("only %d segments hold records", len(firsts))
	}
	for name := range firsts {
		var id int
		fmt.Sscanf(name, "%06d.seg", &id)
		src, _ := d.Open(name)
		data := make([]byte, src.Size())
		src.ReadAt(data, 0)
		twin, err := d.Create(fmt.Sprintf("%06d.seg", 500+id), src.Size())
		if err != nil {
			t.Fatal(err)
		}
		twin.WriteAt(data, 0)
		twin.Sync()
	}
	opt.Disk = d
	j = open(t, opt)
	defer closeWithin(t, j)
	for name, first := range firsts {
		if live, got, _, _ := preamble(t, d, name); !live || got != first {
			t.Errorf("%s began at %d and the copy of it was kept instead (live %v, now %d)", name, first, live, got)
		}
	}
}

// A sync of the floor that fails is not tried again: the system may have
// dropped what it could not write, and a second sync would then succeed
// without the mark. The journal fails, and no header is wiped.
func TestFailedFloorSyncFailsTheJournal(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 9; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 9)
	publish(t, j, 6)
	anchor := segmentStarting(t, d, 7) // the segment that keeps the floor
	var failed bool
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "sync" && e.Name == anchor && !failed {
			failed = true
			return errors.New("EIO")
		}
		return nil
	})
	if err := j.Reclaim(); err == nil || !failed {
		t.Fatalf("Reclaim after a failed sync of the floor: %v (sync failed: %v)", err, failed)
	}
	d.Intercept(nil)
	if err := j.Reclaim(); err == nil {
		t.Fatal("the journal went on after the sync of its floor failed")
	}
	if _, err := j.Append(ctx, []byte("x")); err == nil {
		t.Fatal("Append succeeded on a failed journal")
	}
	segmentStarting(t, d, 1) // the older segments are still there: neither was wiped
	segmentStarting(t, d, 4)
}
