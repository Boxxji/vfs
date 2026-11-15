package journal_test

import (
	"reflect"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

func TestAppendSyncReplayReopen(t *testing.T) {
	dir := scratch(t)
	j, err := journal.Open(dir, journal.Options{Capacity: 4 << 20, Segment: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var want []entry
	for i := 1; i <= 100; i++ {
		p := payload(uint64(i))
		seq := mustAppend(t, j, p)
		if seq != uint64(i) {
			t.Fatalf("seq %d, want %d", seq, i)
		}
		want = append(want, entry{seq, string(p)})
	}
	mustSync(t, j, 100)
	if m := j.Marks(); m.Visible != 100 || m.Durable < 100 || m.Published != 0 {
		t.Fatalf("marks %+v", m)
	}
	if got := replayed(t, j); !reflect.DeepEqual(got, want) {
		t.Fatalf("replay in the same run: %v records, want %v", len(got), len(want))
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j, err = journal.Open(dir, journal.Options{Capacity: 4 << 20, Segment: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if got := replayed(t, j); !reflect.DeepEqual(got, want) {
		t.Fatalf("replay after reopen: %d records, want %d", len(got), len(want))
	}
	if again := replayed(t, j); !reflect.DeepEqual(again, want) {
		t.Fatal("second replay differs from the first")
	}
	if seq := mustAppend(t, j, []byte("next")); seq != 101 {
		t.Fatalf("sequence after reopen %d, want 101", seq)
	}
}

func TestRotationReclaimAndNumbering(t *testing.T) {
	d := crash.New()
	opt := small(d, ticking{newClock(), 2e6}, 0)
	j := open(t, opt)
	var last uint64
	for i := 0; i < 60; i++ {
		last = mustAppend(t, j, payload(uint64(i+1)))
		if i%7 == 0 {
			mustSync(t, j, last)
		}
	}
	publish(t, j, last-10)
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// everything survives a clean close: replay is everything after the published mark
	j = open(t, small(d, newClock(), last-10))
	defer j.Close()
	got := replayed(t, j)
	if len(got) != 10 || got[0].Seq != last-9 || got[9].Seq != last {
		t.Fatalf("replay after publish %v", seqs(got))
	}
	if seq := mustAppend(t, j, []byte("x")); seq != last+1 {
		t.Fatalf("numbering broke across reclaim: %d, want %d", seq, last+1)
	}
}
