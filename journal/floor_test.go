package journal_test

// Fault injection on the floor and mark writes: a mark that fails, a wipe that
// fails, a sync that fails, in a workload that recycles segments. Whatever the
// fault, in every crash state a header is wiped only once the floor that says
// why is durable in a segment that stays, and a stale Head never meets a success
// that dropped acknowledged records.

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// faultDisk wraps a crash.Disk. hook sees every write and sync before it
// happens: keep < 0 lets it through; keep >= 0 with an error writes the first
// keep bytes of a write and fails it.
type faultDisk struct {
	*crash.Disk
	hook func(kind, name string, off int64, n int) (keep int, err error)
}

func (d faultDisk) wrap(f journal.File, err error, name string) (journal.File, error) {
	if err != nil {
		return nil, err
	}
	return faultFile{f, name, d.hook}, nil
}
func (d faultDisk) Create(name string, size int64) (journal.File, error) {
	f, err := d.Disk.Create(name, size)
	return d.wrap(f, err, name)
}
func (d faultDisk) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	return d.wrap(f, err, name)
}

type faultFile struct {
	journal.File
	name string
	hook func(kind, name string, off int64, n int) (int, error)
}

func (f faultFile) WriteAt(p []byte, off int64) (int, error) {
	if keep, err := f.hook("write", f.name, off, len(p)); err != nil {
		if keep > 0 {
			f.File.WriteAt(p[:keep], off)
		}
		return keep, err
	}
	return f.File.WriteAt(p, off)
}
func (f faultFile) Sync() error {
	if _, err := f.hook("sync", f.name, 0, 0); err != nil {
		return err
	}
	return f.File.Sync()
}

type kindOfOp int

const (
	opMarkWrite kindOfOp = iota
	opWipeWrite
	opAnySync
)

func classify(kind string, off int64, n int) (kindOfOp, bool) {
	switch {
	case kind == "write" && n == markSize && (off == headerSize || off == headerSize+markSize):
		return opMarkWrite, true
	case kind == "write" && off == 0 && n == headerSize:
		return opWipeWrite, true
	case kind == "sync":
		return opAnySync, true
	}
	return 0, false
}

// floorRun is a workload on a faulty crash disk.
type floorRun struct {
	*run
	fd    faultDisk
	mu2   sync.Mutex
	count map[kindOfOp]int
	after []string // operations seen after the fault fired
	fired bool
	tb    testing.TB
	bad   []string
}

func newFloorRun(want kindOfOp, k int, keep int, failAll bool) *floorRun {
	fr := &floorRun{run: newRun(), count: map[kindOfOp]int{}}
	fr.fd = faultDisk{Disk: fr.run.d, hook: func(kind, name string, off int64, n int) (int, error) {
		c, ok := classify(kind, off, n)
		if kind == "write" && off == 0 && n == headerSize {
			fr.atWipe(name)
		}
		fr.mu2.Lock()
		defer fr.mu2.Unlock()
		if fr.fired && (kind == "write" || kind == "sync") {
			fr.after = append(fr.after, fmt.Sprintf("%s %s @%d len %d", kind, name, off, n))
		}
		if !ok || c != want {
			return -1, nil
		}
		fr.count[c]++
		if k >= 0 && (fr.count[c]-1 == k || (failAll && fr.count[c]-1 > k)) {
			fr.fired = true
			if kind == "write" {
				return keep, errors.New("EIO")
			}
			return -1, errors.New("EIO")
		}
		return -1, nil
	}}
	return fr
}

// atWipe runs as a header is about to be zeroed: on the media as it stands if
// the machine lost power now (only what a completed sync covered), the highest
// floor must already reach the end of the segment about to go.
func (fr *floorRun) atWipe(name string) {
	img := fr.d.Image(fr.d.Len(), crash.Variant{Kind: crash.Lost})
	names, _ := img.List()
	var mine uint64
	var firsts []uint64
	var floor uint64
	for _, n := range names {
		live, f, _, fl := preamble(fr.tb, img, n)
		if !live {
			continue
		}
		floor = max(floor, fl)
		if n == name {
			mine = f
		} else {
			firsts = append(firsts, f)
		}
	}
	if mine == 0 {
		return
	}
	var succ uint64
	for _, f := range firsts {
		if f > mine && (succ == 0 || f < succ) {
			succ = f
		}
	}
	if succ == 0 {
		return
	}
	if floor < succ-1 {
		fr.mu2.Lock()
		fr.bad = append(fr.bad, fmt.Sprintf("%s (records %d to %d) wiped while the durable floor is %d", name, mine, succ-1, floor))
		fr.mu2.Unlock()
	}
}

func (fr *floorRun) opts(published uint64) journal.Options {
	return small(fr.fd, ticking{newClock(), 3e6}, published)
}

// work appends, syncs, publishes and reclaims, tolerating errors the way a
// caller does: a failed Reclaim is tried again at the next publish, a failed
// journal ends the run.
func (fr *floorRun) work(t *testing.T, j *journal.Journal) {
	pub := func() {
		seq := j.Marks().Durable
		if seq > j.Marks().Published {
			fr.d.Mark(stored(seq))
			if err := j.Publish(seq); err != nil {
				return
			}
		}
	}
	for i := 1; i <= 18; i++ {
		var seq uint64
		var err error
		for try := 0; try < 10; try++ {
			if seq, err = j.Append(ctx, bigRecord(i)); !errors.Is(err, journal.ErrFull) {
				break
			}
			c, cancel := within()
			serr := j.Sync(c, j.Marks().Visible)
			cancel()
			if serr != nil {
				return
			}
			fr.d.Mark(acked(j.Marks().Visible))
			pub()
			j.Reclaim()
		}
		if err != nil {
			return
		}
		fr.mu.Lock()
		fr.data[seq] = bigRecord(i)
		fr.mu.Unlock()
		if i%3 == 0 {
			c, cancel := within()
			serr := j.Sync(c, seq)
			cancel()
			if serr != nil {
				return
			}
			fr.d.Mark(acked(seq))
		}
		if i%6 == 0 {
			pub()
			j.Reclaim()
			j.Reclaim() // the next publish
		}
	}
	c, cancel := within()
	if err := j.Sync(c, j.Marks().Visible); err == nil {
		fr.d.Mark(acked(j.Marks().Visible))
	}
	cancel()
	pub()
	j.Reclaim()
	j.Reclaim()
}

// checkFloor is the invariant: a header is wiped only once the floor that says
// why is durable in a segment that stays; and a stale Head never meets a
// success that dropped acknowledged records.
func (fr *floorRun) checkFloor(img *crash.Disk, n int, base uint64) error {
	if err := fr.run.check(img, n); err != nil {
		return fmt.Errorf("recovery with the Head the store holds: %w", err)
	}
	s := fr.state(img, n)
	for _, stale := range []uint64{base, s.pub / 2, s.pub - min(s.pub, 1)} {
		if stale >= s.pub {
			continue
		}
		cp := img.Image(img.Len(), crash.Variant{Kind: crash.All})
		j, err := journal.Open("", small(cp, ticking{newClock(), 3e6}, stale))
		if err != nil {
			if errors.Is(err, journal.ErrMismatch) || errors.Is(err, journal.ErrCorrupt) {
				continue
			}
			return fmt.Errorf("Open(stale %d): %v", stale, err)
		}
		got, rerr := replay(j)
		j.Close()
		if rerr != nil {
			return fmt.Errorf("Replay(stale %d): %v", stale, rerr)
		}
		var last uint64
		if len(got) > 0 {
			last = got[len(got)-1].Seq
			if got[0].Seq != stale+1 {
				return fmt.Errorf("Open(stale %d) succeeded and replays from %d", stale, got[0].Seq)
			}
		}
		if max(last, stale) < s.ack {
			return fmt.Errorf("Open(stale %d) succeeded, kept through %d, and Sync had returned for %d (dropped records the store did not hold)", stale, max(last, stale), s.ack)
		}
	}
	return nil
}

func TestFloorFaults(t *testing.T) {
	for _, every := range []int{1, 16} {
		for _, tc := range []struct {
			name string
			want kindOfOp
			keep int
		}{
			{"markwrite-error", opMarkWrite, 0},
			{"markwrite-half", opMarkWrite, 10},
			{"wipewrite-error", opWipeWrite, 0},
			{"sync-error", opAnySync, 0},
		} {
			t.Run(fmt.Sprintf("every%d/%s", every, tc.name), func(t *testing.T) {
				defer journal.MarkEvery(every)()
				// pass 0 counts the events of the class
				probe := newFloorRun(tc.want, -1, 0, false)
				probe.tb = t
				j := open(t, probe.opts(0))
				probe.work(t, j)
				closeBounded(t, j)
				total := probe.count[tc.want]
				if total == 0 {
					t.Skipf("no %s in the workload", tc.name)
				}
				// the first, one in the middle, the last
				for _, k := range []int{0, total / 2, total - 1} {
					fr := newFloorRun(tc.want, k, tc.keep, k == total/2)
					fr.tb = t
					j := open(t, fr.opts(0))
					fr.work(t, j)
					closeBounded(t, j)
					if len(fr.bad) > 0 {
						t.Fatalf("fault at %s #%d: %v", tc.name, k, fr.bad)
					}
					if !fr.fired {
						continue
					}
					seen := map[state]bool{}
					err := fr.d.Explore(0, 3, func(img *crash.Disk, n int, v crash.Variant) error {
						if _, again := fr.novel(seen, img, n); again {
							return nil
						}
						return fr.checkFloor(img, n, 0)
					})
					if err != nil {
						t.Fatalf("fault at %s #%d: %v", tc.name, k, err)
					}
				}
			})
		}
	}
}
