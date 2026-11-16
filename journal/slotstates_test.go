package journal_test

import (
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

type slotState struct {
	valid          bool
	through, floor uint64
	zero           bool
}

// The recovered mark counter, over the states the two slots of the anchor can be
// in: the first mark written to the anchor after a restart goes to the slot that
// does not hold the newest, the newest being the higher through and, with equal
// throughs, the higher floor; and a mark torn in flight leaves the highest of each
// there was.
func TestRecoveredMarkCounterAllSlotStates(t *testing.T) {
	cases := map[string][2]slotState{
		"slot0 only":               {{valid: true, through: 5, floor: 1}, {zero: true}},
		"slot1 only":               {{zero: true}, {valid: true, through: 5, floor: 1}},
		"slot0 newer":              {{valid: true, through: 6, floor: 2}, {valid: true, through: 5, floor: 1}},
		"slot1 newer":              {{valid: true, through: 5, floor: 1}, {valid: true, through: 6, floor: 2}},
		"equal":                    {{valid: true, through: 5, floor: 1}, {valid: true, through: 5, floor: 1}},
		"same through, floor0":     {{valid: true, through: 5, floor: 2}, {valid: true, through: 5, floor: 1}},
		"same through, floor1":     {{valid: true, through: 5, floor: 1}, {valid: true, through: 5, floor: 2}},
		"slot0 newest, slot1 torn": {{valid: true, through: 6, floor: 2}, {}},
		"slot1 newest, slot0 torn": {{}, {valid: true, through: 6, floor: 2}},
		"neither":                  {{}, {}},
	}
	for name, slots := range cases {
		t.Run(name, func(t *testing.T) {
			defer journal.MarkEvery(1)()
			d0 := closedJournal(t, 9)
			seed := seedOfSegment(t, d0, "000001.seg", nil)
			var wantT, wantF uint64
			for i, s := range slots {
				switch {
				case s.zero:
					f, _ := d0.Open("000001.seg")
					f.WriteAt(make([]byte, markSize), int64(headerSize+i*markSize))
				case s.valid:
					putMark(t, d0, "000001.seg", i, seed, s.through, s.floor, true)
					wantT, wantF = max(wantT, s.through), max(wantF, s.floor)
				default:
					putMark(t, d0, "000001.seg", i, seed, 1, 1, false)
				}
			}
			d := d0.Image(d0.Len(), crash.Variant{Kind: crash.All})
			var bad []string
			var marks int
			d.Intercept(func(e crash.Event) error {
				if isMarkWrite(e) && e.Name == "000001.seg" {
					marks++
					if msg, hit := overNewest(t, d, e); hit {
						bad = append(bad, msg)
					}
				}
				return nil
			})
			j := open(t, small(d, newClock(), 3))
			publish(t, j, 3)
			if err := j.Reclaim(); err != nil {
				t.Fatal(err)
			}
			closeWithin(t, j)
			if len(bad) > 0 {
				t.Fatalf("a mark written over the newest: %v", bad)
			}
			if marks == 0 {
				t.Fatal("the Reclaim wrote no mark to the anchor")
			}
			valid, th, fl := markSlots(t, d, "000001.seg")
			var gotT, gotF uint64
			for i := 0; i < 2; i++ {
				if valid[i] {
					gotT, gotF = max(gotT, th[i]), max(gotF, fl[i])
				}
			}
			if gotT < wantT || gotF < wantF {
				t.Fatalf("the anchor says through %d floor %d, there was %d and %d", gotT, gotF, wantT, wantF)
			}
		})
	}
}
