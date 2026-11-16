package journal_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// Crash during recovery. The PR explores the crash states of a workload
// and recovers each once; it never crashes the recovery itself. Here every
// crash state S1 (strided) is recovered on a logging image, and every crash
// state S2 of that recovery is recovered again and held to the same
// invariants (prefix, every acknowledged record, second replay equal).
func TestCrashDuringRecovery(t *testing.T) {
	for _, c := range []struct {
		seed  int64
		every int
	}{{1, 1}, {2, 16}} {
		t.Run(fmt.Sprintf("every%d", c.every), func(t *testing.T) {
			defer journal.MarkEvery(c.every)()
			crashDuringRecovery(t, c.seed, 80)
		})
	}
}

func crashDuringRecovery(t *testing.T, seed int64, ops int) {
	r := newRun()
	j := open(t, r.opts(0))
	rng := rand.New(rand.NewSource(seed))
	var visible, published uint64
	for i := 0; i < ops; i++ {
		seq := r.append(t, j, payload(uint64(rng.Intn(900))))
		visible = seq
		switch rng.Intn(8) {
		case 0, 1, 2:
			r.sync(t, j, seq)
		case 3:
			published += uint64(rng.Intn(int(visible-published))) + 1
			r.publish(t, j, published)
		case 4:
			r.reclaim(t, j)
		}
	}
	r.sync(t, j, visible)
	j.Close()

	states, second := 0, 0
	distinct := map[state]bool{}
	err := r.d.Explore(1, 1, func(img1 *crash.Disk, n int, v crash.Variant) error {
		s, again := r.novel(distinct, img1, n)
		if again {
			return nil
		}
		states++
		ack, pub := s.ack, s.pub
		// Recover S1 on the logging image; the recovery's own writes are then in
		// img1's log, and every crash state of those is recovered in turn.
		j1, err := journal.Open("", r.opts2(img1, pub))
		if err != nil {
			return fmt.Errorf("S1: Open: %w", err)
		}
		j1.Close()
		return img1.Explore(1, 5, func(img2 *crash.Disk, n2 int, v2 crash.Variant) error {
			second++
			return r.recovered(img2.Image(img2.Len(), crash.Variant{Kind: crash.All}), pub, ack, pub)
		})
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, r.d.Log(min(r.d.Len(), 40)))
	}
	t.Logf("%d first crashes, %d second crashes", states, second)
}

func (r *run) opts2(d *crash.Disk, published uint64) journal.Options {
	return small(d, ticking{newClock(), 3e6}, published)
}

// The Head can be ahead of what the journal was told with Publish (the
// Head landed, the process died before Publish). Recover every crash state
// with a Published larger than the Publish marks said, by 1, 5 and "all".
func TestHeadAheadOfPublish(t *testing.T) {
	defer journal.MarkEvery(16)()
	r := newRun()
	j := open(t, r.opts(0))
	rng := rand.New(rand.NewSource(5))
	var visible, published uint64
	for i := 0; i < 90; i++ {
		seq := r.append(t, j, payload(uint64(rng.Intn(900))))
		visible = seq
		switch rng.Intn(8) {
		case 0, 1, 2:
			r.sync(t, j, seq)
		case 3:
			published += uint64(rng.Intn(int(visible-published))) + 1
			r.publish(t, j, published)
		case 4, 5:
			r.reclaim(t, j)
		}
	}
	r.sync(t, j, visible)
	j.Close()

	distinct := map[state]bool{}
	err := r.d.Explore(1, 3, func(img *crash.Disk, n int, v crash.Variant) error {
		s, again := r.novel(distinct, img, n)
		if again {
			return nil
		}
		pub, seen := s.pub, s.ack
		for _, extra := range []uint64{1, 5, 1 << 20} {
			// The Head landed and the process died before Publish. The Volume
			// publishes only what was durable when it did, so the Head can be
			// ahead of the Publish marks by no more than the records a Sync had
			// returned for.
			g := min(pub+extra, max(pub, seen))
			if g > visible {
				g = visible
			}
			if err := r.recovered(img.Image(img.Len(), crash.Variant{Kind: crash.All}), g, seen, g); err != nil {
				return fmt.Errorf("Published %d (marks said %d): %w", g, pub, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%v\n%s", err, r.d.Log(min(r.d.Len(), 40)))
	}
}

// Damage fuzz: flip, zero or randomise bytes of the segments of a synced
// journal. Open must not panic. It either refuses (the damage lies before
// something a mark proved) or Replay returns a gap-free run after Published
// whose every payload is the one appended.
func TestDamageFuzz(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	for round := 0; round < 1000; round++ {
		d := crash.New()
		j := open(t, small(d, newClock(), 0))
		want := map[uint64][]byte{}
		var last uint64
		for i := 0; i < 40; i++ {
			p := payload(uint64(rng.Intn(500)))
			s, err := j.Append(ctx, p)
			if err != nil { // full: publish all and reclaim
				j.Publish(j.Marks().Visible)
				j.Reclaim()
				s, err = j.Append(ctx, p)
				if err != nil {
					t.Fatal(err)
				}
			}
			want[s] = p
			last = s
		}
		mustSync(t, j, last)
		j.Close()
		names, _ := d.List()
		for k := 0; k < 1+rng.Intn(4); k++ {
			name := names[rng.Intn(len(names))]
			f, _ := d.Open(name)
			off := int64(rng.Intn(int(f.Size())))
			n := 1 + rng.Intn(64)
			if off+int64(n) > f.Size() {
				n = int(f.Size() - off)
			}
			buf := make([]byte, n)
			switch rng.Intn(3) {
			case 0:
				rng.Read(buf)
			case 1: // zeros (a lost sector)
			case 2:
				f.ReadAt(buf, off)
				buf[rng.Intn(n)] ^= 1 << uint(rng.Intn(8))
			}
			f.WriteAt(buf, off)
		}
		pubd := uint64(rng.Intn(int(last) + 1))
		j2, err := journal.Open("", small(d, newClock(), pubd))
		if errors.Is(err, journal.ErrCorrupt) || errors.Is(err, journal.ErrMismatch) || errors.Is(err, journal.ErrFormat) {
			continue // damage before what a mark proved, to the magic of a header, or a store the journal cannot meet: refused, and nothing changed
		}
		if err != nil {
			t.Fatalf("round %d: Open: %v", round, err)
		}
		var prev uint64
		err = j2.Replay(func(r journal.Record) error {
			if prev != 0 && r.Seq != prev+1 {
				return fmt.Errorf("gap %d -> %d", prev, r.Seq)
			}
			if prev == 0 && r.Seq != pubd+1 {
				return fmt.Errorf("first %d after published %d", r.Seq, pubd)
			}
			if !bytes.Equal(r.Data, want[r.Seq]) {
				return fmt.Errorf("record %d corrupt", r.Seq)
			}
			prev = r.Seq
			return nil
		})
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		j2.Close()
	}
}
