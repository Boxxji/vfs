package journal_test

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

const (
	dmgNone = iota
	dmgPayload
	dmgHeader
)

// log12 is records 1..12 in 000000..000003, three to a segment, closed.
func log12(t testing.TB, marksLost bool) *crash.Disk {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 12; i++ {
		mustAppend(t, j, bigRecord(i))
	}
	mustSync(t, j, 12)
	closeWithin(t, j)
	if marksLost {
		forgetMarks(t, d)
	}
	return d
}

func damageLog(t testing.TB, d *crash.Disk, dm map[int]int) {
	for seq, kind := range dm {
		seg := fmt.Sprintf("%06d.seg", (seq-1)/3)
		off := recordOffset((seq-1)%3, len(big))
		switch kind {
		case dmgPayload:
			corrupt(t, d, seg, off+40)
		case dmgHeader:
			corrupt(t, d, seg, off+10)
		}
	}
}

// modelOf is what the docs say: the valid prefix, the ranges given up (one
// per segment after it, from where reading starts to the last record that can be
// reached and is valid), and the range Discard holds.
func modelOf(dm map[int]int) (p int, dropped []journal.Range, held journal.Range, orphanValid bool) {
	for p < 12 && dm[p+1] == dmgNone {
		p++
	}
	for k := 0; k < 4; k++ {
		lo, hi := 3*k+1, 3*k+3
		if hi <= p {
			continue
		}
		start := max(lo, p+1)
		last := 0
		for s := start; s <= hi; s++ {
			if dm[s] == dmgHeader {
				break
			}
			if dm[s] == dmgNone {
				last = s
			}
		}
		if last >= start {
			dropped = append(dropped, journal.Range{First: uint64(start), Last: uint64(last)})
			// the chain tail's own range counts to held; every other is an orphan's
			if !(p >= lo && p < hi) {
				orphanValid = true
			}
		}
	}
	if p > 0 {
		held = journal.Range{First: 1, Last: uint64(p)}
		for _, r := range dropped {
			if int(r.First) == p+1 && p%3 != 0 { // behind, in the tail segment of the chain
				held.Last = r.Last
			}
		}
	}
	return
}

func sameRanges(a, b []journal.Range) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func damageShapes() []map[int]int {
	var out []map[int]int
	every := 0
	out = append(out, map[int]int{})
	kinds := []int{dmgPayload, dmgHeader}
	for i := 1; i <= 12; i++ {
		for _, k := range kinds {
			out = append(out, map[int]int{i: k})
		}
	}
	for i := 1; i <= 12; i++ {
		for j := i + 1; j <= 12; j++ {
			for _, a := range kinds {
				for _, b := range kinds {
					if every++; every%6 == 0 {
						out = append(out, map[int]int{i: a, j: b}) // a sample of the pairs: all of them take a second
					}
				}
			}
		}
	}
	// a few triples
	for i := 1; i <= 12; i += 4 {
		for j := i + 1; j <= 12; j += 4 {
			for l := j + 1; l <= 12; l += 4 {
				out = append(out, map[int]int{i: dmgPayload, j: dmgPayload, l: dmgPayload}, map[int]int{i: dmgPayload, j: dmgHeader, l: dmgPayload})
			}
		}
	}
	return out
}

func TestDroppedMatchesTheModel(t *testing.T) {
	cfgs := damageShapes()
	bad := 0
	for _, dm := range cfgs {
		d := log12(t, true)
		damageLog(t, d, dm)
		p, want, _, _ := modelOf(dm)
		j, err := journal.Open("", small(d, newClock(), 0))
		if err != nil {
			bad++
			t.Errorf("%v: Open %v", dm, err)
			continue
		}
		r := j.Recovery()
		got := append([]journal.Range(nil), r.Dropped...)
		sort.Slice(got, func(a, b int) bool { return got[a].First < got[b].First })
		if int(r.Last) != p || !sameRanges(got, want) {
			bad++
			if bad < 12 {
				t.Errorf("damage %v: Last %d Dropped %v; model Last %d Dropped %v", dm, r.Last, got, p, want)
			}
		}
		if es := replayed(t, j); len(es) != p {
			t.Errorf("damage %v: replay %v, want 1..%d", dm, seqs(es), p)
		}
		closeWithin(t, j)
		// Open gave them up: a second Open lists nothing and warns of nothing.
		j2, err := journal.Open("", small(d, newClock(), 0))
		if err != nil {
			t.Errorf("%v: second Open %v", dm, err)
			continue
		}
		// The orphans are wiped by the first Open; the records behind a damaged
		// payload in the last segment of the log stay on disk, unread, and are
		// listed again until a new record is placed in a later segment.
		var tail []journal.Range
		for _, r := range want {
			if int(r.First) == p+1 && p%3 != 0 {
				tail = append(tail, r)
			}
		}
		if r2 := j2.Recovery(); !sameRanges(r2.Dropped, tail) || len(r2.Warnings) != 0 || int(r2.Last) != p {
			t.Errorf("%v: second Open reports %+v, want Dropped %v", dm, r2, tail)
		}
		closeWithin(t, j2)
	}
	t.Logf("%d damage shapes, %d disagreements with the model", len(cfgs), bad)
}

// With the marks present, any damage in 1..12 is damage to a record a mark
// proves durable: ErrCorrupt, and nothing written.
func TestMarksPresentRefuseAndWriteNothing(t *testing.T) {
	for _, dm := range damageShapes() {
		if len(dm) == 0 {
			continue
		}
		d := log12(t, false)
		damageLog(t, d, dm)
		before, n0 := d.Fingerprint(), d.Len()
		j, err := journal.Open("", small(d, newClock(), 0))
		if err == nil {
			j.Close()
			t.Errorf("%v: Open succeeded over damage the marks prove", dm)
			continue
		}
		if !errors.Is(err, journal.ErrCorrupt) {
			t.Errorf("%v: %v", dm, err)
		}
		if d.Len() != n0 || d.Fingerprint() != before {
			t.Errorf("%v: a refusal wrote", dm)
		}
	}
}

// Discard against the model, with the marks present and lost: it succeeds for
// exactly the held range when nothing else valid lies behind a hole, and for
// no other range; a refusal changes nothing; a success leaves a journal that
// opens empty.
func TestDiscardIsExact(t *testing.T) {
	for _, lost := range []bool{true, false} {
		for _, dm := range damageShapes() {
			p, _, held, orphanValid := modelOf(dm)
			// the damage is to payloads or headers alike: held is the same
			try := func(first, last uint64) error {
				d := log12(t, lost)
				damageLog(t, d, dm)
				before, n0 := d.Fingerprint(), d.Len()
				err := journal.Discard("", small(d, newClock(), 0), first, last)
				if err != nil {
					if !errors.Is(err, journal.ErrRange) {
						return fmt.Errorf("Discard(%d,%d): %w", first, last, err)
					}
					if d.Len() != n0 || d.Fingerprint() != before {
						return fmt.Errorf("Discard(%d,%d) failed and wrote", first, last)
					}
					return err
				}
				// success: nothing live is left
				j, oerr := journal.Open("", small(d, newClock(), 0))
				if oerr != nil {
					return fmt.Errorf("Open after Discard: %w", oerr)
				}
				defer j.Close()
				if r := j.Recovery(); r.Last != 0 || len(r.Dropped) != 0 {
					return fmt.Errorf("after Discard(%d,%d) the journal still holds %+v", first, last, r)
				}
				return nil
			}
			shouldSucceed := held != (journal.Range{}) && !orphanValid
			if p == 0 {
				shouldSucceed = false
				// chain empty: held is zero; only (0,0) can match and it refuses when orphans hold records
				shouldSucceed = !orphanValid && false
			}
			err := try(held.First, held.Last)
			switch {
			case shouldSucceed && err != nil:
				t.Errorf("lost=%v damage %v: Discard(%d,%d) of the held range: %v", lost, dm, held.First, held.Last, err)
			case !shouldSucceed && err == nil && held != (journal.Range{}):
				t.Errorf("lost=%v damage %v: Discard(%d,%d) succeeded with valid records behind a hole", lost, dm, held.First, held.Last)
			}
			if held == (journal.Range{}) {
				continue
			}
			for _, wrong := range []journal.Range{
				{First: held.First, Last: held.Last - 1}, {First: held.First, Last: held.Last + 1},
				{First: held.First + 1, Last: held.Last}, {First: 0, Last: held.Last}, {First: 1, Last: 12},
			} {
				if wrong == held {
					continue
				}
				if err := try(wrong.First, wrong.Last); err == nil {
					t.Errorf("lost=%v damage %v: Discard(%d,%d) succeeded; the journal holds %v", lost, dm, wrong.First, wrong.Last, held)
				}
			}
		}
	}
}

const dmgAbsent = 3

// A hole (a segment whose header is gone) plus one or two damaged records
// elsewhere: Dropped is still the model's.
func TestDroppedWithAHole(t *testing.T) {
	n, bad := 0, 0
	for hole := 0; hole < 4; hole++ {
		for _, dm0 := range damageShapes() {
			if len(dm0) > 2 {
				continue
			}
			dm := map[int]int{}
			skip := false
			for s, k := range dm0 {
				if (s-1)/3 == hole {
					skip = true
				}
				dm[s] = k
			}
			if skip {
				continue
			}
			for i := 1; i <= 3; i++ {
				dm[3*hole+i] = dmgAbsent
			}
			d := log12(t, true)
			damageLog(t, d, dm)
			f, _ := d.Open(fmt.Sprintf("%06d.seg", hole))
			f.WriteAt(make([]byte, headerSize), 0)
			p, want, _, _ := modelOf(dm)
			j, err := journal.Open("", small(d, newClock(), 0))
			n++
			if err != nil {
				bad++
				t.Errorf("hole %d damage %v: %v", hole, dm0, err)
				continue
			}
			got := append([]journal.Range(nil), j.Recovery().Dropped...)
			sort.Slice(got, func(a, b int) bool { return got[a].First < got[b].First })
			if int(j.Recovery().Last) != p || !sameRanges(got, want) {
				bad++
				if bad < 10 {
					t.Errorf("hole %d damage %v: Last %d Dropped %v; model Last %d Dropped %v", hole, dm0, j.Recovery().Last, got, p, want)
				}
			}
			closeWithin(t, j)
		}
	}
	t.Logf("%d shapes with a hole, %d disagreements", n, bad)
}
