package journal_test

import (
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// layered is a Disk that knows its file system is a risk.
type layered struct{ journal.Disk }

func (layered) Warnings() []string { return []string{"this is an overlay"} }

// Open passes on what the file layer says about its file system.
func TestRecoveryCarriesTheDiskWarnings(t *testing.T) {
	j := open(t, small(layered{crash.New()}, newClock(), 0))
	defer j.Close()
	if got := j.Recovery().Warnings; !reflect.DeepEqual(got, []string{"this is an overlay"}) {
		t.Fatalf("warnings %v", got)
	}
}

// A disk that cannot make a segment is a full disk, not a dead journal: the
// Append that needs the segment gets ErrFull, the journal goes on taking what
// fits, and when the disk has room again the next Append rotates and succeeds.
func TestSegmentCreationFailureIsFullNotFatal(t *testing.T) {
	d := crash.New()
	var fail atomic.Bool
	d.Intercept(func(e crash.Event) error {
		if e.Kind == "create" && fail.Load() {
			return errors.New("no space left on device")
		}
		return nil
	})
	j := open(t, small(d, newClock(), 0)) // the first segment exists, the spare is made at once
	defer j.Close()
	fail.Store(true)
	// Fill the tail and the spare that already exists, then need one more.
	var sawFull bool
	var last uint64
	for i := 0; i < 60 && !sawFull; i++ {
		seq, err := j.Append(ctx, bigRecord(i+1))
		switch {
		case errors.Is(err, journal.ErrFull):
			sawFull = true
		case err != nil:
			t.Fatalf("Append %d: %v", i, err)
		default:
			last = seq
		}
	}
	if !sawFull {
		t.Fatal("Append never needed the segment it could not make")
	}
	if s := j.State(); s.Err != nil {
		t.Fatalf("the journal failed over a full disk: %v", s.Err)
	}
	mustSync(t, j, last) // what was appended is safe
	// Room again: drain what is there, and the journal takes records.
	fail.Store(false)
	publish(t, j, last)
	if err := j.Reclaim(); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, bigRecord(99)); err != nil {
		t.Fatalf("Append after the disk had room: %v", err)
	}
}

// shorting is a File that writes less than it was asked to and says nothing.
type shorting struct {
	journal.File
	on *atomic.Bool
}

func (f shorting) WriteAt(p []byte, off int64) (int, error) {
	if f.on.Load() && len(p) > 1 {
		n, err := f.File.WriteAt(p[:len(p)/2], off)
		return n, err
	}
	return f.File.WriteAt(p, off)
}

type shortDisk struct {
	journal.Disk
	on *atomic.Bool
}

func (d shortDisk) Create(name string, size int64) (journal.File, error) {
	f, err := d.Disk.Create(name, size)
	return shorting{f, d.on}, err
}

func (d shortDisk) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	return shorting{f, d.on}, err
}

// The File contract is a full write. A short one with no error is a broken file
// layer; the journal does not take it for a success.
func TestShortWriteFailsTheJournal(t *testing.T) {
	var on atomic.Bool
	j := open(t, small(shortDisk{crash.New(), &on}, newClock(), 0))
	defer j.Close()
	mustSync(t, j, mustAppend(t, j, []byte("fine")))
	on.Store(true)
	seq := mustAppend(t, j, []byte("this write comes up short"))
	if err := j.Sync(ctx, seq); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Sync after a short write: %v, want io.ErrShortWrite", err)
	}
	if _, err := j.Append(ctx, []byte("x")); err == nil {
		t.Fatal("Append after a short write succeeded")
	}
}

// Two segment files can claim the same first sequence: a copy, or a restore of
// an old backup. The one with the lower name is the log's and the other is not,
// every time.
func TestSegmentsClaimingTheSameFirstAreOrdered(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 3; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	mustSync(t, j, 3)
	j.Close()

	src, err := d.Open("000000.seg")
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, src.Size())
	if _, err := src.ReadAt(data, 0); err != nil {
		t.Fatal(err)
	}
	twin, err := d.Create("000042.seg", src.Size())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := twin.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	for try := 0; try < 3; try++ {
		j = open(t, small(d, newClock(), 0))
		if got := seqs(replayed(t, j)); !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
			t.Fatalf("try %d: replay %v", try, got)
		}
		if r := j.Recovery(); !reflect.DeepEqual(r.Dropped, []journal.Range{{First: 1, Last: 3}}) && try == 0 {
			t.Fatalf("recovery %+v: the twin was not reported", r)
		}
		j.Close()
		// The log is the lower name's: its header still says it begins at 1.
		f, err := d.Open("000000.seg")
		if err != nil {
			t.Fatal(err)
		}
		h := make([]byte, 16)
		if _, err := f.ReadAt(h, 0); err != nil {
			t.Fatal(err)
		}
		if first := binary.LittleEndian.Uint64(h[8:]); first != 1 {
			t.Fatalf("try %d: the lower name begins at %d, so the other was kept", try, first)
		}
	}
}

// The mark a journal leaves is what Open reports as proven.
func TestRecoveryReportsWhatWasProven(t *testing.T) {
	d := crash.New()
	j := open(t, small(d, newClock(), 0))
	for i := 1; i <= 5; i++ {
		mustAppend(t, j, []byte{byte('0' + i)})
	}
	mustSync(t, j, 5)
	j.Close()
	j = open(t, small(d, newClock(), 0))
	defer j.Close()
	if r := j.Recovery(); r.First != 1 || r.Last != 5 || r.Proven != 5 || len(r.Dropped) != 0 {
		t.Fatalf("recovery %+v", r)
	}
}
