// Copyright 2026 Hanzo AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package journal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
)

// scrapMemFile is an in-memory journal.File that tracks physical writes,
// physical bytes written, syncs, and alignment for unit tests and benchmarks.
type scrapMemFile struct {
	mu           sync.Mutex
	data         []byte
	writes       uint64
	alignedWrite uint64
	writtenBytes uint64
	reads        uint64
	syncs        uint64
	failWrite    error
	failSync     error
}

func newScrapMemFile(size int) *scrapMemFile {
	return &scrapMemFile{data: make([]byte, size)}
}

func (f *scrapMemFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if off < 0 || off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *scrapMemFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWrite != nil {
		return 0, f.failWrite
	}
	if off < 0 || off+int64(len(p)) > int64(len(f.data)) {
		return 0, fmt.Errorf("write out of bounds [%d..%d) > %d", off, off+int64(len(p)), len(f.data))
	}
	copy(f.data[off:], p)
	f.writes++
	if off%journal.PageSize == 0 && len(p)%journal.PageSize == 0 {
		f.alignedWrite++
	}
	f.writtenBytes += uint64(len(p))
	return len(p), nil
}

func (f *scrapMemFile) Writeback(int64, int64) error { return nil }

func (f *scrapMemFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSync != nil {
		return f.failSync
	}
	f.syncs++
	return nil
}

func (f *scrapMemFile) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.data))
}

func (f *scrapMemFile) Close() error { return nil }

// baselineCopyFile simulates an unbuffered/page-copy baseline that allocates
// and copies every write into a 4 KiB page-aligned RMW staging buffer before
// writing to the backing file.
type baselineCopyFile struct {
	f            journal.File
	logicalBytes uint64
	pageCopies   uint64
	physWrites   uint64
	physBytes    uint64
}

func (b *baselineCopyFile) WriteAt(p []byte, off int64) (int, error) {
	b.logicalBytes += uint64(len(p))
	pos := 0
	for pos < len(p) {
		curOff := off + int64(pos)
		pageOff := curOff &^ (journal.PageSize - 1)
		inLo := int(curOff & (journal.PageSize - 1))
		inHi := min(journal.PageSize, inLo+(len(p)-pos))
		chunk := inHi - inLo

		// Naive copy path: always stages into a 4 KiB page buffer.
		page := make([]byte, journal.PageSize)
		if chunk < journal.PageSize {
			_, _ = b.f.ReadAt(page, pageOff)
		}
		copy(page[inLo:inHi], p[pos:pos+chunk])
		b.pageCopies += journal.PageSize
		n, err := b.f.WriteAt(page, pageOff)
		b.physWrites++
		b.physBytes += uint64(n)
		if err != nil {
			return pos, err
		}
		pos += chunk
	}
	return len(p), nil
}

func TestScrapPageAlignment(t *testing.T) {
	for _, size := range []int{1, 64, 4096, 8192, 16384} {
		b := journal.AllocAligned(size)
		if len(b) != size {
			t.Fatalf("AllocAligned(%d) len = %d", size, len(b))
		}
		if !journal.IsPageAligned(b) {
			t.Fatalf("AllocAligned(%d) is not 4 KiB aligned", size)
		}
	}
	if journal.IsPageAligned(nil) {
		t.Fatal("IsPageAligned(nil) = true, want false")
	}
	unaligned := journal.AllocAligned(8192)[1:]
	if journal.IsPageAligned(unaligned) {
		t.Fatal("unaligned slice reported aligned")
	}
}

func TestScrapAbsorbsSmallUnalignedWritesAndCoalesces(t *testing.T) {
	mem := newScrapMemFile(64 * journal.PageSize)
	sf := journal.NewScrapFile(mem, journal.ScrapOptions{MaxPages: 16, AppendOnly: true})
	defer sf.Close()

	// Sixteen 256-byte writes fill page 0 cleanly without any intermediate disk write.
	want := make([]byte, journal.PageSize)
	for i := range 16 {
		chunk := bytes.Repeat([]byte{byte('a' + i)}, 256)
		off := int64(i * 256)
		copy(want[off:], chunk)
		n, err := sf.WriteAt(chunk, off)
		if err != nil || n != 256 {
			t.Fatalf("WriteAt(%d): n=%d err=%v", off, n, err)
		}
	}

	if mem.writes != 0 {
		t.Fatalf("expected 0 physical writes while absorbing into scrap buffer, got %d", mem.writes)
	}

	// ReadAt sees buffered writes immediately (Visible).
	got := make([]byte, journal.PageSize)
	if n, err := sf.ReadAt(got, 0); err != nil || n != journal.PageSize {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("ReadAt mismatch before Sync")
	}

	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if mem.writes != 1 || mem.alignedWrite != 1 {
		t.Fatalf("expected 1 coalesced 4 KiB aligned write, got writes=%d aligned=%d", mem.writes, mem.alignedWrite)
	}
	if !bytes.Equal(mem.data[:journal.PageSize], want) {
		t.Fatal("backing file bytes mismatch after Sync")
	}
	st := sf.Stats()
	if st.SmallWrites != 16 || st.ScrapHits != 15 || st.WriteAmp() != 1.0 {
		t.Fatalf("unexpected scrap stats: %+v writeAmp=%.2f", st, st.WriteAmp())
	}
}

func TestScrapPreservesNeighborsOnPartialRandomOverwrite(t *testing.T) {
	mem := newScrapMemFile(8 * journal.PageSize)
	for i := range mem.data {
		mem.data[i] = byte(0xA0 + (i % 13))
	}
	want := append([]byte(nil), mem.data...)

	sf := journal.NewScrapFile(mem, journal.ScrapOptions{MaxPages: 4})
	defer sf.Close()

	// Cross-page unaligned partial writes + disjoint intra-page writes.
	edits := []struct {
		off int64
		len int
		val byte
	}{
		{100, 300, 0x11},
		{500, 200, 0x22},
		{3584, 1024, 0x33}, // straddles page 0 -> page 1
		{2*journal.PageSize + 77, 1500, 0x44},
	}
	for _, ed := range edits {
		buf := bytes.Repeat([]byte{ed.val}, ed.len)
		copy(want[ed.off:], buf)
		if _, err := sf.WriteAt(buf, ed.off); err != nil {
			t.Fatal(err)
		}
	}

	// Verify live ReadAt mixes dirty scrap bytes and clean underlying bytes accurately.
	got := make([]byte, len(want))
	if _, err := sf.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("live ReadAt diverged from expected bytes")
	}

	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mem.data, want) {
		t.Fatal("backing file after Sync lost neighboring bytes or missed edits")
	}
}

func TestScrapBypassesLargeAlignedWrites(t *testing.T) {
	mem := newScrapMemFile(32 * journal.PageSize)
	sf := journal.NewScrapFile(mem, journal.ScrapOptions{MaxPages: 8})
	defer sf.Close()

	// Dirty page 2 partially first, then overwrite page 2 completely with an aligned 4 KiB write.
	if _, err := sf.WriteAt([]byte("stale-partial"), 2*journal.PageSize+17); err != nil {
		t.Fatal(err)
	}
	aligned4K := journal.AllocAligned(journal.PageSize)
	for i := range aligned4K {
		aligned4K[i] = byte(i*7 + 3)
	}
	if _, err := sf.WriteAt(aligned4K, 2*journal.PageSize); err != nil {
		t.Fatal(err)
	}

	// Aligned 16 KiB write at page 4..8.
	aligned16K := journal.AllocAligned(4 * journal.PageSize)
	for i := range aligned16K {
		aligned16K[i] = byte(i*13 + 5)
	}
	if _, err := sf.WriteAt(aligned16K, 4*journal.PageSize); err != nil {
		t.Fatal(err)
	}

	// Mixed write: unaligned head + 2 aligned pages + unaligned tail.
	mixed := bytes.Repeat([]byte{0x5C}, 10000)
	if _, err := sf.WriteAt(mixed, 10*journal.PageSize+1000); err != nil {
		t.Fatal(err)
	}

	if err := sf.Sync(); err != nil {
		t.Fatal(err)
	}
	st := sf.Stats()
	if st.BypassWrites != 3 {
		t.Fatalf("BypassWrites = %d, want 3", st.BypassWrites)
	}
	if st.DiscardedPages != 1 {
		t.Fatalf("DiscardedPages = %d, want 1 (stale partial page superseded)", st.DiscardedPages)
	}
	if wantBypass := uint64(journal.PageSize + 4*journal.PageSize + journal.PageSize); st.BypassBytes != wantBypass {
		t.Fatalf("BypassBytes = %d, want %d", st.BypassBytes, wantBypass)
	}
	if !bytes.Equal(mem.data[2*journal.PageSize:3*journal.PageSize], aligned4K) {
		t.Fatal("aligned 4 KiB bypass write mismatched or clobbered by discarded scrap")
	}
	if !bytes.Equal(mem.data[4*journal.PageSize:8*journal.PageSize], aligned16K) {
		t.Fatal("aligned 16 KiB bypass write mismatched")
	}
	if !bytes.Equal(mem.data[10*journal.PageSize+1000:10*journal.PageSize+1000+10000], mixed) {
		t.Fatal("mixed head+aligned+tail write mismatched")
	}
}

func TestScrapZeroAllocSteadyState(t *testing.T) {
	mem := newScrapMemFile(64 * journal.PageSize)
	sf := journal.NewScrapFile(mem, journal.ScrapOptions{MaxPages: 32, AppendOnly: true})
	defer sf.Close()

	// Warm up pool & map entry.
	small := make([]byte, 256)
	aligned := journal.AllocAligned(journal.PageSize)
	_, _ = sf.WriteAt(small, 0)
	_ = sf.Sync()
	_, _ = sf.WriteAt(aligned, 4*journal.PageSize)
	_ = sf.Sync()

	allocsSmall := testing.AllocsPerRun(200, func() {
		if _, err := sf.WriteAt(small, 512); err != nil {
			t.Fatal(err)
		}
	})
	if allocsSmall != 0 {
		t.Fatalf("small scrap absorption allocated %.1f allocs/op, want 0", allocsSmall)
	}

	allocsBypass := testing.AllocsPerRun(200, func() {
		if _, err := sf.WriteAt(aligned, 8*journal.PageSize); err != nil {
			t.Fatal(err)
		}
	})
	if allocsBypass != 0 {
		t.Fatalf("aligned 4K bypass allocated %.1f allocs/op, want 0", allocsBypass)
	}
}

func TestScrapLogDurabilityWatermarks(t *testing.T) {
	d := crash.New()
	f, err := d.Create("000000.seg", 16*journal.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	log, err := journal.OpenScrapLog(f, []byte("vol-1:round-1"), 0, journal.ScrapOptions{MaxPages: 16})
	if err != nil {
		t.Fatal(err)
	}

	// Append small records (< 4 KiB): Visible advances, Durable stays 0, Publish refused with ErrAhead.
	var seqs []uint64
	for i := 1; i <= 10; i++ {
		rec := bytes.Repeat([]byte{byte('0' + i)}, 100+i)
		seq, err := log.Append(ctx, rec)
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	m := log.Marks()
	if m.Visible != 10 || m.Durable != 0 || m.Published != 0 {
		t.Fatalf("unexpected watermarks before Sync: %+v", m)
	}
	if err := log.Publish(1); !errors.Is(err, journal.ErrAhead) {
		t.Fatalf("Publish(1) before Sync = %v, want ErrAhead", err)
	}
	if err := log.Sync(ctx, 11); !errors.Is(err, journal.ErrAhead) {
		t.Fatalf("Sync(11) ahead of Visible = %v, want ErrAhead", err)
	}

	// Replay sees buffered Visible records prior to Sync.
	var count int
	if err := log.Replay(func(r journal.Record) error {
		count++
		if r.Seq != uint64(count) {
			t.Fatalf("replay seq %d, want %d", r.Seq, count)
		}
		return nil
	}); err != nil || count != 10 {
		t.Fatalf("pre-sync Replay count=%d err=%v", count, err)
	}

	// Sync makes 1..10 Durable; Publish(6) succeeds; Publish(11) fails.
	if err := log.Sync(ctx, 10); err != nil {
		t.Fatal(err)
	}
	m = log.Marks()
	if m.Durable != 10 || m.Visible != 10 || m.Published != 0 {
		t.Fatalf("watermarks after Sync: %+v", m)
	}
	if err := log.Publish(6); err != nil {
		t.Fatal(err)
	}
	if err := log.Publish(11); !errors.Is(err, journal.ErrAhead) {
		t.Fatalf("Publish(11) > Durable = %v, want ErrAhead", err)
	}
	if m := log.Marks(); m.Published != 6 || m.Durable != 10 || m.Visible != 10 {
		t.Fatalf("watermarks after Publish(6): %+v", m)
	}

	// Append 2 more unsynced records, then take a crash.Lost image: 1..10 survive, 11..12 do not.
	if _, err := log.Append(ctx, []byte("unsynced-11")); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, []byte("unsynced-12")); err != nil {
		t.Fatal(err)
	}
	lost := d.Image(d.Len(), crash.Variant{Kind: crash.Lost})
	_ = log.Close()

	rf, err := lost.Open("000000.seg")
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := journal.OpenScrapLog(rf, []byte("vol-1:round-1"), 6, journal.ScrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	rm := recovered.Marks()
	if rm.Published != 6 || rm.Durable != 10 || rm.Visible != 10 {
		t.Fatalf("recovered watermarks = %+v, want {10 10 6}", rm)
	}
	var gotSeqs []uint64
	if err := recovered.Replay(func(r journal.Record) error {
		gotSeqs = append(gotSeqs, r.Seq)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(gotSeqs) != 4 || gotSeqs[0] != 7 || gotSeqs[3] != 10 {
		t.Fatalf("recovered replay seqs = %v, want [7 8 9 10]", gotSeqs)
	}

	// Wrong lineage must fail closed.
	rf2, _ := lost.Open("000000.seg")
	if _, err := journal.OpenScrapLog(rf2, []byte("other-lineage"), 6, journal.ScrapOptions{}); !errors.Is(err, journal.ErrLineage) {
		t.Fatalf("wrong lineage err = %v, want ErrLineage", err)
	}
}

func TestScrapLogFailureIsFinal(t *testing.T) {
	mem := newScrapMemFile(8 * journal.PageSize)
	log, err := journal.OpenScrapLog(mem, []byte("fail-test"), 0, journal.ScrapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s1, err := log.Append(ctx, []byte("ok"))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Sync(ctx, s1); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("nvme io error")
	mem.mu.Lock()
	mem.failSync = boom
	mem.mu.Unlock()

	s2, err := log.Append(ctx, []byte("fails-on-sync"))
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Sync(ctx, s2); !errors.Is(err, boom) {
		t.Fatalf("Sync after failure = %v, want %v", err, boom)
	}
	if _, err := log.Append(ctx, []byte("after-failure")); !errors.Is(err, boom) {
		t.Fatalf("Append after failure = %v, want %v", err, boom)
	}
	if err := log.Sync(ctx, s1); err != nil {
		t.Fatalf("already-durable Sync(%d) failed: %v", s1, err)
	}
	_ = log.Close()
}

func TestJournalWithScrapDisk(t *testing.T) {
	base := crash.New()
	sd := journal.NewScrapDisk(base, journal.ScrapOptions{MaxPages: 16, AppendOnly: true})
	j, err := journal.Open("", journal.Options{Capacity: 8 * 8192, Segment: 8192, Disk: sd, Clock: newClock()})
	if err != nil {
		t.Fatal(err)
	}
	var want [][]byte
	for i := range 30 {
		p := payload(uint64(i + 1))
		want = append(want, p)
		seq, err := j.Append(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if i%5 == 4 {
			if err := j.Sync(ctx, seq); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := j.Sync(ctx, 30); err != nil {
		t.Fatal(err)
	}
	got := replayed(t, j)
	if len(got) != 30 {
		t.Fatalf("replayed %d records, want 30", len(got))
	}
	for i, g := range got {
		if g.Data != string(want[i]) {
			t.Fatalf("record %d mismatch", i+1)
		}
	}
	if err := closeBounded(t, j); err != nil {
		t.Fatal(err)
	}

	// Reopen directly on base crash.Disk without ScrapDisk to confirm standard layout parity.
	j2, err := journal.Open("", journal.Options{Capacity: 8 * 8192, Segment: 8192, Disk: base, Clock: newClock()})
	if err != nil {
		t.Fatal(err)
	}
	defer closeWithin(t, j2)
	got2 := replayed(t, j2)
	if len(got2) != 30 {
		t.Fatalf("reopen replayed %d records, want 30", len(got2))
	}
}

func TestScrapFileConcurrent(t *testing.T) {
	mem := newScrapMemFile(128 * journal.PageSize)
	sf := journal.NewScrapFile(mem, journal.ScrapOptions{MaxPages: 16})
	defer sf.Close()

	var writers sync.WaitGroup
	var readers sync.WaitGroup
	var stop atomic.Bool
	for g := range 4 {
		writers.Add(1)
		go func(id int) {
			defer writers.Done()
			rng := rand.New(rand.NewPCG(uint64(id+1), 99))
			buf := make([]byte, 512)
			aligned := journal.AllocAligned(journal.PageSize)
			for i := range 200 {
				if i%5 == 0 {
					off := int64(rng.IntN(64)) * journal.PageSize
					if _, err := sf.WriteAt(aligned, off); err != nil {
						t.Error(err)
						return
					}
				} else {
					off := int64(rng.IntN(64*journal.PageSize - len(buf)))
					if _, err := sf.WriteAt(buf, off); err != nil {
						t.Error(err)
						return
					}
				}
				if i%25 == 0 {
					if err := sf.Sync(); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}(g)
	}
	readers.Add(1)
	go func() {
		defer readers.Done()
		buf := make([]byte, journal.PageSize)
		for !stop.Load() {
			_, _ = sf.ReadAt(buf, 0)
		}
	}()
	writers.Wait()
	stop.Store(true)
	readers.Wait()
}

// BenchmarkJournalAppendBaseline_SmallUnaligned measures baseline unbuffered
// small-record journal appends (each small write allocates/copies a page RMW).
func BenchmarkJournalAppendBaseline_SmallUnaligned(b *testing.B) {
	mem := newScrapMemFile(64 << 20)
	base := &baselineCopyFile{f: mem}
	rec := bytes.Repeat([]byte("j"), 256)
	const limit = (64 << 20) - journal.PageSize

	b.SetBytes(int64(len(rec)))
	b.ReportAllocs()
	b.ResetTimer()
	var off int64
	for i := 0; i < b.N; i++ {
		if _, err := base.WriteAt(rec, off); err != nil {
			b.Fatal(err)
		}
		off += int64(len(rec))
		if off >= limit {
			off = 0
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(base.physBytes)/float64(max(1, base.logicalBytes)), "write-amp")
	b.ReportMetric(float64(base.physWrites)/float64(b.N), "phys-writes/op")
	b.ReportMetric(float64(base.pageCopies)/float64(b.N), "copy-B/op")
}

// BenchmarkJournalAppendWSBuffer_SmallUnaligned measures WSBuffer ScrapLog/ScrapFile
// absorbing < 4 KiB unaligned writes into page-aligned scrap pages.
func BenchmarkJournalAppendWSBuffer_SmallUnaligned(b *testing.B) {
	mem := newScrapMemFile(64 << 20)
	sf := journal.NewScrapFile(mem, journal.ScrapOptions{MaxPages: 64, AppendOnly: true})
	rec := bytes.Repeat([]byte("j"), 256)
	const limit = (64 << 20) - journal.PageSize

	b.SetBytes(int64(len(rec)))
	b.ReportAllocs()
	b.ResetTimer()
	var off int64
	for i := 0; i < b.N; i++ {
		if _, err := sf.WriteAt(rec, off); err != nil {
			b.Fatal(err)
		}
		off += int64(len(rec))
		if off >= limit {
			off = 0
		}
	}
	if err := sf.Flush(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	st := sf.Stats()
	b.ReportMetric(st.WriteAmp(), "write-amp")
	b.ReportMetric(float64(st.PhysicalWrites)/float64(b.N), "phys-writes/op")
	b.ReportMetric(float64(st.PageCopies)/float64(b.N), "copy-B/op")
}

// BenchmarkJournal4KRandomWrite_BaselineCopy measures 4 KiB random-write
// throughput, heap allocations, and copy/write amplification for a baseline
// copy-staging write path.
func BenchmarkJournal4KRandomWrite_BaselineCopy(b *testing.B) {
	const fileSize = 64 << 20
	mem := newScrapMemFile(fileSize)
	base := &baselineCopyFile{f: mem}
	page := journal.AllocAligned(journal.PageSize)
	const numPages = fileSize / journal.PageSize

	b.SetBytes(journal.PageSize)
	b.ReportAllocs()
	b.ResetTimer()
	var idx uint64 = 1
	for i := 0; i < b.N; i++ {
		idx = idx*6364136223846793005 + 1442695040888963407
		off := int64((idx>>16)%numPages) * journal.PageSize
		if _, err := base.WriteAt(page, off); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(base.physBytes)/float64(max(1, base.logicalBytes)), "write-amp")
	b.ReportMetric(float64(base.pageCopies)/float64(b.N), "copy-B/op")
}

// BenchmarkJournal4KRandomWrite_WSBufferBypass measures 4 KiB aligned random-write
// throughput, heap allocations, and write amplification using WSBuffer zero-copy bypass.
func BenchmarkJournal4KRandomWrite_WSBufferBypass(b *testing.B) {
	const fileSize = 64 << 20
	mem := newScrapMemFile(fileSize)
	sf := journal.NewScrapFile(mem, journal.ScrapOptions{MaxPages: 64})
	page := journal.AllocAligned(journal.PageSize)
	const numPages = fileSize / journal.PageSize

	b.SetBytes(journal.PageSize)
	b.ReportAllocs()
	b.ResetTimer()
	var idx uint64 = 1
	for i := 0; i < b.N; i++ {
		idx = idx*6364136223846793005 + 1442695040888963407
		off := int64((idx>>16)%numPages) * journal.PageSize
		if _, err := sf.WriteAt(page, off); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	st := sf.Stats()
	b.ReportMetric(st.WriteAmp(), "write-amp")
	b.ReportMetric(float64(st.PageCopies)/float64(b.N), "copy-B/op")
}

// BenchmarkJournal4KRandomWrite_OSFile compares baseline copy vs WSBuffer bypass
// over an actual OS file (pwrite without per-op fsync).
func BenchmarkJournal4KRandomWrite_OSFile(b *testing.B) {
	const fileSize = 16 << 20
	const numPages = fileSize / journal.PageSize
	page := journal.AllocAligned(journal.PageSize)

	for _, useScrap := range []bool{false, true} {
		name := "baseline-copy"
		if useScrap {
			name = "wsbuffer-bypass"
		}
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "bench.seg")
			f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			defer f.Close()
			if err := f.Truncate(fileSize); err != nil {
				b.Fatal(err)
			}
			wf := &osBenchFile{File: f, size: fileSize}
			var sf *journal.ScrapFile
			var base *baselineCopyFile
			if useScrap {
				sf = journal.NewScrapFile(wf, journal.ScrapOptions{MaxPages: 64})
			} else {
				base = &baselineCopyFile{f: wf}
			}

			b.SetBytes(journal.PageSize)
			b.ReportAllocs()
			b.ResetTimer()
			var idx uint64 = 7
			for i := 0; i < b.N; i++ {
				idx = idx*6364136223846793005 + 1442695040888963407
				off := int64((idx>>16)%numPages) * journal.PageSize
				if useScrap {
					if _, err := sf.WriteAt(page, off); err != nil {
						b.Fatal(err)
					}
				} else {
					if _, err := base.WriteAt(page, off); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			if useScrap {
				st := sf.Stats()
				b.ReportMetric(st.WriteAmp(), "write-amp")
				b.ReportMetric(float64(st.PageCopies)/float64(b.N), "copy-B/op")
			} else {
				b.ReportMetric(float64(base.physBytes)/float64(max(1, base.logicalBytes)), "write-amp")
				b.ReportMetric(float64(base.pageCopies)/float64(b.N), "copy-B/op")
			}
		})
	}
}

type osBenchFile struct {
	*os.File
	size int64
}

func (f *osBenchFile) Writeback(int64, int64) error { return nil }
func (f *osBenchFile) Size() int64                  { return f.size }

// Ensure ScrapLog Append+Sync throughput benchmark is also covered cleanly.
func BenchmarkScrapLogAppendSync(b *testing.B) {
	for _, sz := range []int{256, 4096} {
		b.Run(fmt.Sprintf("payload-%dB", sz), func(b *testing.B) {
			mem := newScrapMemFile(128 << 20)
			log, err := journal.OpenScrapLog(mem, []byte("bench"), 0, journal.ScrapOptions{MaxPages: 64})
			if err != nil {
				b.Fatal(err)
			}
			defer log.Close()
			rec := bytes.Repeat([]byte("r"), sz)

			b.SetBytes(int64(sz))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seq, err := log.Append(context.Background(), rec)
				if errors.Is(err, journal.ErrFull) {
					b.StopTimer()
					mem = newScrapMemFile(128 << 20)
					log, _ = journal.OpenScrapLog(mem, []byte("bench"), 0, journal.ScrapOptions{MaxPages: 64})
					b.StartTimer()
					seq, err = log.Append(context.Background(), rec)
				}
				if err != nil {
					b.Fatal(err)
				}
				if i&15 == 15 {
					if err := log.Sync(context.Background(), seq); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			st := log.Stats()
			b.ReportMetric(st.WriteAmp(), "write-amp")
		})
	}
}
