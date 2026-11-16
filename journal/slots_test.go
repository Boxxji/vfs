package journal_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// markSlots reads the two mark slots of a segment as the format defines them:
// whether each validates under the segment's seed, and what it says.
func markSlots(t testing.TB, d *crash.Disk, name string) (valid [2]bool, through, floor [2]uint64) {
	t.Helper()
	seed := seedOfSegment(t, d, name, nil)
	b := peek(t, d, name, headerSize, 2*markSize)
	for i := 0; i < 2; i++ {
		m := b[i*markSize : (i+1)*markSize]
		if crc32.Update(seed, crc32.MakeTable(crc32.Castagnoli), m[4:]) == binary.LittleEndian.Uint32(m) {
			valid[i] = true
			through[i], floor[i] = binary.LittleEndian.Uint64(m[4:]), binary.LittleEndian.Uint64(m[12:])
		}
	}
	return
}

// isMarkWrite recognises the write of a mark by its place and size.
func isMarkWrite(e crash.Event) bool {
	return e.Kind == "write" && e.Len == markSize && (e.Off == headerSize || e.Off == headerSize+markSize)
}

// overNewest reports whether the mark about to be written goes over the slot
// that holds the newest one, which a tear of the write would then take away.
func overNewest(t testing.TB, d *crash.Disk, e crash.Event) (string, bool) {
	valid, th, fl := markSlots(t, d, e.Name)
	target := int((e.Off - headerSize) / markSize)
	o := 1 - target
	if valid[target] && (!valid[o] || th[target] > th[o] || fl[target] > fl[o]) {
		return fmt.Sprintf("%s: a mark written over slot %d, which holds the newest (through %d floor %d; the other slot valid %v through %d floor %d)",
			e.Name, target, th[target], fl[target], valid[o], th[o], fl[o]), true
	}
	return "", false
}

// A mark is written to the slot that does not hold the newest one, so that a
// torn write leaves the newest standing: through segments that are recycled
// several times, at two cadences.
func TestAMarkNeverOverwritesTheNewest(t *testing.T) {
	for _, every := range []int{1, 3} {
		t.Run(fmt.Sprint(every), func(t *testing.T) {
			defer journal.MarkEvery(every)()
			d := crash.New()
			var bad []string
			d.Intercept(func(e crash.Event) error {
				if isMarkWrite(e) {
					if msg, hit := overNewest(t, d, e); hit {
						bad = append(bad, msg)
					}
				}
				return nil
			})
			j := open(t, small(d, newClock(), 0))
			for i := 1; i <= 30; i++ {
				last := mustAppend(t, j, bigRecord(i))
				mustSync(t, j, last)
				if i%6 == 0 {
					publish(t, j, last-3)
					j.Reclaim()
				}
			}
			closeWithin(t, j)
			if len(bad) > 0 {
				t.Fatalf("%d mark writes hit the newest slot, first: %s", len(bad), bad[0])
			}
		})
	}
}

// A segment recovered by Open must know which slot holds its newest mark: the
// first mark written to it afterwards (as the anchor of a Reclaim) goes to the
// other one, however many marks the segment had been given.
func TestRecoveredAnchorMarkNeverOverwritesTheNewest(t *testing.T) {
	for _, every := range []int{1, 2} {
		for syncs := 1; syncs <= 3; syncs++ { // how many rounds the segment gets before the restart: odd and even
			t.Run(fmt.Sprint(every, "/", syncs), func(t *testing.T) {
				defer journal.MarkEvery(every)()
				d := crash.New()
				j := open(t, small(d, newClock(), 0))
				for i := 1; i <= 3; i++ {
					mustAppend(t, j, bigRecord(i))
				}
				mustSync(t, j, 3)
				for i := 4; i <= 6; i++ {
					seq := mustAppend(t, j, bigRecord(i))
					if i-3 <= syncs {
						mustSync(t, j, seq)
					}
				}
				for i := 7; i <= 9; i++ {
					mustAppend(t, j, bigRecord(i))
				}
				mustSync(t, j, 9)
				closeWithin(t, j)
				var bad []string
				var marks int
				d.Intercept(func(e crash.Event) error {
					if isMarkWrite(e) {
						marks++
						if msg, hit := overNewest(t, d, e); hit {
							bad = append(bad, msg)
						}
					}
					return nil
				})
				j = open(t, small(d, newClock(), 3))
				publish(t, j, 3)
				if err := j.Reclaim(); err != nil {
					t.Fatal(err)
				}
				closeWithin(t, j)
				if marks == 0 {
					t.Fatal("no mark was written after the restart")
				}
				if len(bad) > 0 {
					t.Fatalf("after a restart a mark was written over the newest: %v", bad)
				}
			})
		}
	}
}

// A mark written by a sync round that fails is a failed write: the journal
// stops, as it does for any other.
func TestFailedMarkWriteInASyncRoundFailsTheJournal(t *testing.T) {
	defer journal.MarkEvery(1)()
	d := crash.New()
	var armed atomic.Bool
	var once sync.Once
	d.Intercept(func(e crash.Event) error {
		if armed.Load() && isMarkWrite(e) {
			var err error
			once.Do(func() { err = errors.New("EIO") })
			return err
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	mustSync(t, j, mustAppend(t, j, bigRecord(1)))
	armed.Store(true)
	seq := mustAppend(t, j, bigRecord(2))
	c, cancel := within()
	defer cancel()
	err := j.Sync(c, seq)
	if err == nil || !strings.Contains(err.Error(), "EIO") {
		t.Fatalf("Sync in a round whose mark write failed: %v, want the write error", err)
	}
	if _, err := j.Append(ctx, []byte("x")); err == nil {
		t.Fatal("Append succeeded after the journal failed")
	}
}

// A mark that proves one record more than the log holds is corruption: the
// boundary is the record after the last, not the one after that.
func TestThroughOneBeyondTheLogIsCorruption(t *testing.T) {
	d := cleanJournal(t, 10)
	seed := seedOfSegment(t, d, "000000.seg", nil)
	putMark(t, d, "000000.seg", 0, seed, 11, 0, true)
	putMark(t, d, "000000.seg", 1, seed, 3, 0, true)
	if j, err := journal.Open("", small(d, newClock(), 0)); !errors.Is(err, journal.ErrCorrupt) {
		if err == nil {
			closeWithin(t, j)
		}
		t.Fatalf("a mark proving record 11 durable with the log ending at 10: %v, want ErrCorrupt", err)
	}
}

// Two writes of marks to one slot are never in flight together: a sync round
// that is writing the mark of the tail, and a Reclaim that anchors on the tail.
func TestMarkWritesAreSerialized(t *testing.T) {
	defer journal.MarkEvery(1)()
	d := crash.New()
	var armed, held atomic.Bool
	var inflight, violations atomic.Int32
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	d.Intercept(func(e crash.Event) error {
		if !armed.Load() || !isMarkWrite(e) {
			return nil
		}
		if inflight.Add(1) > 1 {
			violations.Add(1)
		}
		defer inflight.Add(-1)
		if held.CompareAndSwap(false, true) {
			entered <- struct{}{}
			<-gate
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	released := sync.OnceFunc(func() { close(gate) })
	defer released()          // before the Close, on every way out
	for i := 1; i <= 4; i++ { // the first segment full, the second begun
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 4)
	publish(t, j, 3) // the first segment is published: a Reclaim anchors on the tail
	armed.Store(true)
	seq := mustAppend(t, j, bigRecord(5))
	sync := make(chan error, 1)
	go func() { c, cancel := within(); defer cancel(); sync <- j.Sync(c, seq) }()
	select {
	case <-entered: // the sync round is inside the write of a mark to the tail
	case <-time.After(5 * time.Second):
		t.Fatal("the sync round wrote no mark")
	}
	reclaim := make(chan error, 1)
	go func() { reclaim <- j.Reclaim() }()
	time.Sleep(100 * time.Millisecond) // a wait for something that must not happen: the Reclaim's mark write beside the first
	if v := violations.Load(); v > 0 {
		t.Fatalf("%d mark writes were in flight at once", v)
	}
	released()
	for _, c := range []chan error{sync, reclaim} {
		if err := <-c; err != nil {
			t.Fatal(err)
		}
	}
}

// A wipe that fails is returned, the next Reclaim finishes the job, and nothing
// is written again that was written: the floor is in place and is not rewritten.
func TestFailedWipeIsReturnedAndRetried(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	for i := 1; i <= 7; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 7)
	publish(t, j, 6)
	var failed atomic.Bool
	var marks atomic.Int32
	d.Intercept(func(e crash.Event) error {
		switch {
		case e.Kind == "write" && e.Off == 0 && e.Len == headerSize && failed.CompareAndSwap(false, true):
			return errors.New("EIO")
		case isMarkWrite(e):
			marks.Add(1)
		}
		return nil
	})
	if err := j.Reclaim(); err == nil || !failed.Load() {
		t.Fatalf("Reclaim with a wipe that fails: %v", err)
	}
	if marks.Load() != 1 {
		t.Fatalf("%d mark writes in the first Reclaim, want the floor's one", marks.Load())
	}
	if err := j.Reclaim(); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if marks.Load() != 1 {
		t.Fatalf("the retry wrote the floor again (%d mark writes)", marks.Load())
	}
	// what is on the disk now opens, with the Head the store holds
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost})
	jj, err := journal.Open("", small(img, newClock(), 6))
	if err != nil {
		t.Fatalf("Open after the retry: %v", err)
	}
	closeWithin(t, jj)
}

// A mark never takes away what an earlier one said, and a sync round records the
// store's sequence in the segment it wrote to as well as what it made durable.
func TestAMarkKeepsWhatItHadAndRecordsTheStore(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	defer closeWithin(t, j)
	for i := 1; i <= 6; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 6)
	mustSync(t, j, mustAppend(t, j, bigRecord(7))) // the segment of 7 to 9 begins: its preamble says nothing of a store
	publish(t, j, 6)
	// a round after the Publish: its mark, in the segment it wrote to, says the store holds 6
	mustSync(t, j, mustAppend(t, j, bigRecord(8)))
	img := d.Image(d.Len(), crash.Variant{Kind: crash.Lost})
	_, _, th, fl := preamble(t, img, segmentStarting(t, img, 7))
	if fl != 6 || th != 7 {
		t.Fatalf("the segment of 7 to 9 says through %d, floor %d; want 7 and the store's 6", th, fl)
	}
	mustSync(t, j, mustAppend(t, j, bigRecord(9)))
	// Reclaim anchors the floor on the segment that stays, and what it proved stays too.
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	img = d.Image(d.Len(), crash.Variant{Kind: crash.Lost})
	_, _, th2, fl2 := preamble(t, img, segmentStarting(t, img, 7))
	if fl2 != 6 || th2 != 8 {
		t.Fatalf("the anchor says through %d, floor %d after the Reclaim; want the floor 6 and the proof of 8 it had", th2, fl2)
	}
}
