package journal_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// One flipped bit in each of the 80 bytes of the preamble of each segment of a
// journal closed cleanly: the outcome is one of the documented ones. A flip in
// the magic is refused as a foreign file; a flip in the rest of a header loses
// that segment's records and says so, or is refused when a mark in a later
// segment proves them; a flip in a mark changes nothing that was whole in the
// other.
func TestEveryBitOfEveryHeader(t *testing.T) {
	const nrec = 9
	closed := closedJournal(t, nrec)
	count := map[string]int{}
	for seg := 0; seg < nrec/3; seg++ {
		name := fmt.Sprintf("%06d.seg", seg)
		for off := 0; off < dataStart; off++ {
			bits := []int{3}
			if off >= 4 && off < 8 {
				bits = []int{0, 1, 2, 3, 4, 5, 6, 7} // the magic: every bit
			}
			for _, bit := range bits {
				d := closed.Image(closed.Len(), crash.Variant{Kind: crash.All})
				f, _ := d.Open(name)
				b := make([]byte, 1)
				f.ReadAt(b, int64(off))
				b[0] ^= 1 << bit
				f.WriteAt(b, int64(off))
				j, err := journal.Open("", small(d, newClock(), 0))
				key := ""
				switch {
				case errors.Is(err, journal.ErrFormat):
					key = "ErrFormat"
					if off < 4 || off >= 8 {
						t.Errorf("%s byte %d bit %d: ErrFormat outside the magic", name, off, bit)
					}
				case errors.Is(err, journal.ErrCorrupt):
					key = "ErrCorrupt"
				case err != nil:
					t.Errorf("%s byte %d bit %d: %v", name, off, bit, err)
					continue
				default:
					r := j.Recovery()
					warned := false
					for _, w := range r.Warnings {
						if strings.Contains(w, name) && strings.Contains(w, "4096 bytes") {
							warned = true
						}
					}
					switch {
					case int(r.Last) == nrec && len(r.Warnings) == 0:
						key = "intact"
						if off < headerSize {
							t.Errorf("%s byte %d bit %d: a header flip changed nothing?", name, off, bit)
						}
					case int(r.Last) < nrec && warned:
						key = "lost+warned"
					case int(r.Last) < nrec:
						key = "LOST-QUIET"
						t.Errorf("%s byte %d bit %d: Last %d of %d with Warnings %v", name, off, bit, r.Last, nrec, r.Warnings)
					default:
						t.Errorf("%s byte %d bit %d: nothing lost, and Warnings %v", name, off, bit, r.Warnings)
					}
					closeWithin(t, j)
				}
				count[key]++
			}
		}
	}
	if count["lost+warned"] == 0 || count["ErrFormat"] == 0 || count["intact"] == 0 {
		t.Fatalf("outcomes %v: every kind should have occurred", count)
	}
}
