package journal_test

import (
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

type readCounter struct {
	journal.Disk
	n *atomic.Int64
}

type readCounterFile struct {
	journal.File
	n *atomic.Int64
}

func (d readCounter) Open(name string) (journal.File, error) {
	f, err := d.Disk.Open(name)
	return readCounterFile{f, d.n}, err
}
func (f readCounterFile) ReadAt(p []byte, off int64) (int, error) {
	f.n.Add(int64(len(p)))
	return f.File.ReadAt(p, off)
}

// LOW. scan reads the payload a record header announces before it checks the
// header's own checksum, so bytes at the end of the valid log that look like a
// header with a long length (stale data of an earlier use of the file, a torn
// header) make recovery read and hold up to a whole segment. With the default
// 64 MiB that is bounded; Segment may be 4 GiB.
func TestScanReadsWhatAnUnverifiedHeaderAnnounces(t *testing.T) {
	const seg = 8 << 20
	d := crash.New()
	opt := journal.Options{Capacity: 4 * seg, Segment: seg, Disk: d}
	j, err := journal.Open("", opt)
	if err != nil {
		t.Fatal(err)
	}
	var last uint64
	for i := 0; i < 5; i++ {
		last, _ = j.Append(ctx, []byte("data"))
	}
	j.Sync(ctx, last)
	j.Close()
	// Plant, right after the last record, a header whose checksum is wrong
	// and whose length says 7 MiB.
	f, _ := d.Open("000000.seg")
	end := int64(dataStart + 5*(28+4))
	h := make([]byte, 28)
	binary.LittleEndian.PutUint32(h[4:], 7<<20)
	f.WriteAt(h, end)
	var n atomic.Int64
	j, err = journal.Open("", journal.Options{Capacity: 4 * seg, Segment: seg, Disk: readCounter{d, &n}})
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	if n.Load() > 4<<20 {
		t.Fatalf("recovery read %d MiB to find that a 28-byte header with a bad checksum ends the log", n.Load()>>20)
	}
}
