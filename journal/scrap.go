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

package journal

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"sync"
	"time"
	"unsafe"
)

const (
	// PageSize is the 4 KiB page granularity used by page-aligned scrap buffers.
	PageSize = 4096
	pageMask = PageSize - 1

	defaultScrapMaxPages = 64 // 256 KiB of buffered dirty scrap pages
	dirtyWords           = PageSize / 64
)

// ErrInvalidOffset is returned when a negative file offset is supplied.
var ErrInvalidOffset = errors.New("journal: negative offset")

// IsPageAligned reports whether b is non-empty and its backing array starts on
// a 4 KiB page boundary.
func IsPageAligned(b []byte) bool {
	return len(b) > 0 && uintptr(unsafe.Pointer(unsafe.SliceData(b)))&pageMask == 0
}

// AllocAligned allocates a byte slice of length n whose base address is aligned
// to PageSize (4096 bytes).
func AllocAligned(n int) []byte {
	if n <= 0 {
		return nil
	}
	raw := make([]byte, n+PageSize-1)
	addr := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	shift := int((PageSize - (addr & pageMask)) & pageMask)
	return raw[shift : shift+n : shift+n]
}

type scrapPage struct {
	data []byte // len == cap == PageSize, aligned to PageSize
}

// ScrapPool recycles 4 KiB page-aligned scrap pages to avoid heap allocations on
// steady-state small-write absorption.
type ScrapPool struct {
	pool sync.Pool
}

// NewScrapPool constructs a pool of PageSize-aligned 4 KiB scrap buffers.
func NewScrapPool() *ScrapPool {
	p := &ScrapPool{}
	p.pool.New = func() any {
		return &scrapPage{data: AllocAligned(PageSize)}
	}
	return p
}

var defaultScrapPool = NewScrapPool()

func (p *ScrapPool) get() *scrapPage {
	return p.pool.Get().(*scrapPage)
}

func (p *ScrapPool) put(pg *scrapPage) {
	if pg != nil && cap(pg.data) == PageSize {
		pg.data = pg.data[:PageSize]
		p.pool.Put(pg)
	}
}

// ScrapOptions configures a ScrapFile or ScrapLog.
type ScrapOptions struct {
	// MaxPages bounds the number of 4 KiB scrap pages retained in memory before
	// the oldest dirty page is flushed to the backing File. Defaults to 64.
	MaxPages int
	// AppendOnly indicates sequential log appends where untouched tail bytes past
	// the append frontier need not be read-modified-written from disk.
	AppendOnly bool
	// Pool optionally specifies a shared ScrapPool. Nil uses a process-wide pool.
	Pool *ScrapPool
}

func (o ScrapOptions) normalized() ScrapOptions {
	if o.MaxPages <= 0 {
		o.MaxPages = defaultScrapMaxPages
	}
	if o.Pool == nil {
		o.Pool = defaultScrapPool
	}
	return o
}

// ScrapStats records partial-write absorption, zero-copy bypass, and write
// amplification counters for a ScrapFile or ScrapLog.
type ScrapStats struct {
	SmallWrites    uint64 // unaligned or < 4 KiB write fragments absorbed into scrap pages
	ScrapHits      uint64 // fragments coalesced into an existing buffered scrap page
	BypassWrites   uint64 // >= 4 KiB aligned writes written directly without scrap copy
	BypassBytes    uint64 // bytes written via zero-copy aligned bypass
	LogicalBytes   uint64 // total logical bytes accepted by WriteAt
	PhysicalWrites uint64 // backing File.WriteAt calls emitted
	PhysicalBytes  uint64 // physical bytes emitted to backing File.WriteAt
	PageCopies     uint64 // bytes copied into scrap / RMW buffers (0 on aligned bypass)
	RMWReads       uint64 // backing File.ReadAt calls performed for partial-page flush
	FlushedPages   uint64 // scrap pages flushed to backing File
	DiscardedPages uint64 // scrap pages superseded by full-page aligned writes
	DirtyPages     int    // dirty scrap pages currently buffered
}

// WriteAmp returns PhysicalBytes / LogicalBytes, or 0 when no bytes were written.
func (s ScrapStats) WriteAmp() float64 {
	if s.LogicalBytes == 0 {
		return 0
	}
	return float64(s.PhysicalBytes) / float64(s.LogicalBytes)
}

type scrapEntry struct {
	pageIdx    int64
	pg         *scrapPage
	dirty      [dirtyWords]uint64
	dirtyCount int
	dirtyLo    int
	dirtyHi    int
	hydrated   bool
	prev, next *scrapEntry
}

func (e *scrapEntry) reset(pageIdx int64, pg *scrapPage) {
	e.pageIdx = pageIdx
	e.pg = pg
	clear(e.dirty[:])
	e.dirtyCount = 0
	e.dirtyLo = PageSize
	e.dirtyHi = 0
	e.hydrated = false
	e.prev = nil
	e.next = nil
}

func (e *scrapEntry) clearDirty() {
	clear(e.dirty[:])
	e.dirtyCount = 0
	e.dirtyLo = PageSize
	e.dirtyHi = 0
}

func (e *scrapEntry) isDirty(i int) bool {
	return (e.dirty[i>>6]>>(uint(i)&63))&1 != 0
}

func (e *scrapEntry) markDirty(lo, hi int) {
	if lo >= hi {
		return
	}
	if e.dirtyCount == 0 {
		e.dirtyLo = lo
		e.dirtyHi = hi
	} else {
		if lo < e.dirtyLo {
			e.dirtyLo = lo
		}
		if hi > e.dirtyHi {
			e.dirtyHi = hi
		}
	}
	w0, b0 := lo>>6, uint(lo&63)
	w1, b1 := (hi-1)>>6, uint((hi-1)&63)
	if w0 == w1 {
		mask := (^uint64(0) << b0) & (^uint64(0) >> (63 - b1))
		e.dirtyCount += bits.OnesCount64(mask &^ e.dirty[w0])
		e.dirty[w0] |= mask
		return
	}
	m0 := ^uint64(0) << b0
	e.dirtyCount += bits.OnesCount64(m0 &^ e.dirty[w0])
	e.dirty[w0] |= m0
	for w := w0 + 1; w < w1; w++ {
		if e.dirty[w] != ^uint64(0) {
			e.dirtyCount += 64 - bits.OnesCount64(e.dirty[w])
			e.dirty[w] = ^uint64(0)
		}
	}
	m1 := ^uint64(0) >> (63 - b1)
	e.dirtyCount += bits.OnesCount64(m1 &^ e.dirty[w1])
	e.dirty[w1] |= m1
}

// ScrapFile wraps a File with page-aligned 4 KiB scrap buffers that absorb
// small unaligned writes (< 4 KiB) while bypassing extra page copies for
// page-aligned writes >= 4 KiB. ScrapFile is safe for concurrent use.
type ScrapFile struct {
	f          File
	pool       *ScrapPool
	maxPages   int
	appendOnly bool

	mu        sync.Mutex
	pages     map[int64]*scrapEntry
	lruHead   *scrapEntry // oldest
	lruTail   *scrapEntry // newest
	free      []*scrapEntry
	flushBuf  []*scrapEntry
	highWater int64
	dirtyCnt  int
	stats     ScrapStats
	closed    bool
}

// NewScrapFile wraps f with a page-aligned scrap buffer layer.
func NewScrapFile(f File, opt ScrapOptions) *ScrapFile {
	opt = opt.normalized()
	return &ScrapFile{
		f:          f,
		pool:       opt.Pool,
		maxPages:   opt.MaxPages,
		appendOnly: opt.AppendOnly,
		pages:      make(map[int64]*scrapEntry, min(opt.MaxPages, 64)),
		free:       make([]*scrapEntry, 0, min(opt.MaxPages, 64)),
		highWater:  f.Size(),
	}
}

func (sf *ScrapFile) linkTail(e *scrapEntry) {
	e.prev = sf.lruTail
	e.next = nil
	if sf.lruTail != nil {
		sf.lruTail.next = e
	} else {
		sf.lruHead = e
	}
	sf.lruTail = e
}

func (sf *ScrapFile) unlink(e *scrapEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		sf.lruHead = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		sf.lruTail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (sf *ScrapFile) touch(e *scrapEntry) {
	if sf.lruTail != e {
		sf.unlink(e)
		sf.linkTail(e)
	}
}

func (sf *ScrapFile) allocEntry(pageIdx int64) *scrapEntry {
	var e *scrapEntry
	if n := len(sf.free); n > 0 {
		e = sf.free[n-1]
		sf.free = sf.free[:n-1]
	} else {
		e = &scrapEntry{}
	}
	e.reset(pageIdx, sf.pool.get())
	sf.pages[pageIdx] = e
	sf.linkTail(e)
	return e
}

func (sf *ScrapFile) releaseEntry(e *scrapEntry) {
	sf.unlink(e)
	delete(sf.pages, e.pageIdx)
	if e.dirtyCount > 0 {
		sf.dirtyCnt--
	}
	sf.pool.put(e.pg)
	e.pg = nil
	if len(sf.free) < sf.maxPages {
		sf.free = append(sf.free, e)
	}
}

func (sf *ScrapFile) discardCovered(firstPage, numPages int64) {
	if len(sf.pages) == 0 || numPages <= 0 {
		return
	}
	endPage := firstPage + numPages
	if int64(len(sf.pages)) < numPages {
		for pgIdx, e := range sf.pages {
			if pgIdx >= firstPage && pgIdx < endPage {
				sf.stats.DiscardedPages++
				sf.releaseEntry(e)
			}
		}
		return
	}
	for pgIdx := firstPage; pgIdx < endPage; pgIdx++ {
		if e, ok := sf.pages[pgIdx]; ok {
			sf.stats.DiscardedPages++
			sf.releaseEntry(e)
		}
	}
}

// evictOne removes the oldest clean page if any exists, otherwise flushes and
// removes the oldest dirty page. Caller holds sf.mu.
func (sf *ScrapFile) evictOne() error {
	for e := sf.lruHead; e != nil; e = e.next {
		if e.dirtyCount == 0 {
			sf.releaseEntry(e)
			return nil
		}
	}
	oldest := sf.lruHead
	if oldest == nil {
		return nil
	}
	if err := sf.flushEntry(oldest); err != nil {
		return err
	}
	sf.releaseEntry(oldest)
	return nil
}

// flushEntry writes dirty bytes of e to the backing File in a page-aligned 4 KiB
// write whenever possible, preserving untouched bytes on partial pages.
// Caller holds sf.mu.
func (sf *ScrapFile) flushEntry(e *scrapEntry) error {
	if e.dirtyCount == 0 {
		return nil
	}
	pageOff := e.pageIdx * PageSize
	fileSize := sf.f.Size()
	writeLen := PageSize
	if fileSize < pageOff+PageSize {
		writeLen = max(e.dirtyHi, int(max(0, fileSize-pageOff)))
	}

	if e.dirtyCount >= PageSize {
		e.hydrated = true
	} else if !e.hydrated && e.dirtyCount < writeLen {
		if sf.appendOnly && e.dirtyLo == 0 {
			// Append-only tail starting at page boundary: untouched tail bytes are zero.
			if e.dirtyHi < PageSize {
				clear(e.pg.data[e.dirtyHi:])
			}
			e.hydrated = true
		} else if pageOff < fileSize {
			// Hydrate untouched bytes once from the backing file to preserve neighbors.
			base := sf.pool.get()
			readLen := min(writeLen, int(fileSize-pageOff))
			clear(base.data)
			if readLen > 0 {
				sf.stats.RMWReads++
				n, err := sf.f.ReadAt(base.data[:readLen], pageOff)
				if err != nil && !errors.Is(err, io.EOF) {
					sf.pool.put(base)
					return err
				}
				_ = n
			}
			sf.mergeDirtyBytes(base.data, e, writeLen)
			sf.pool.put(e.pg)
			e.pg = base
			e.hydrated = true
		} else {
			clearNonDirty(e, PageSize)
			e.hydrated = true
		}
	}

	n, err := sf.f.WriteAt(e.pg.data[:writeLen], pageOff)
	sf.stats.PhysicalWrites++
	sf.stats.PhysicalBytes += uint64(n)
	if err != nil {
		return err
	}
	if n != writeLen {
		return io.ErrShortWrite
	}
	sf.stats.FlushedPages++
	e.clearDirty()
	sf.dirtyCnt--
	return nil
}

func (sf *ScrapFile) mergeDirtyBytes(dst []byte, e *scrapEntry, limit int) {
	// Fast path: single contiguous dirty range [dirtyLo, dirtyHi) or byte-exact mask.
	for i := e.dirtyLo; i < min(e.dirtyHi, limit); i++ {
		if e.isDirty(i) {
			dst[i] = e.pg.data[i]
		}
	}
	sf.stats.PageCopies += uint64(limit)
}

func clearNonDirty(e *scrapEntry, limit int) {
	for i := 0; i < limit; i++ {
		if !e.isDirty(i) {
			e.pg.data[i] = 0
		}
	}
}

// WriteAt writes len(p) bytes at off. Unaligned or < 4 KiB portions are
// absorbed into page-aligned scrap pages; page-aligned portions >= 4 KiB bypass
// extra page copies directly to the backing File.
func (sf *ScrapFile) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrInvalidOffset
	}
	if len(p) == 0 {
		return 0, nil
	}
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if sf.closed {
		return 0, ErrClosed
	}

	sf.stats.LogicalBytes += uint64(len(p))
	pos := 0
	for pos < len(p) {
		curOff := off + int64(pos)
		rem := len(p) - pos

		// Aligned >= 4 KiB write bypasses scrap-page buffering and copies.
		if curOff&pageMask == 0 && rem >= PageSize {
			alignedLen := rem &^ pageMask
			sf.discardCovered(curOff>>12, int64(alignedLen)>>12)
			n, err := sf.f.WriteAt(p[pos:pos+alignedLen], curOff)
			sf.stats.BypassWrites++
			sf.stats.BypassBytes += uint64(alignedLen)
			sf.stats.PhysicalWrites++
			sf.stats.PhysicalBytes += uint64(n)
			if err != nil {
				return pos + n, err
			}
			if n != alignedLen {
				return pos + n, io.ErrShortWrite
			}
			if end := curOff + int64(alignedLen); end > sf.highWater {
				sf.highWater = end
			}
			pos += alignedLen
			continue
		}

		// Small or unaligned write fragment: absorb into a page-aligned 4 KiB scrap page.
		pageIdx := curOff >> 12
		inLo := int(curOff & pageMask)
		inHi := min(PageSize, inLo+rem)
		chunkLen := inHi - inLo

		e, ok := sf.pages[pageIdx]
		if ok {
			sf.stats.ScrapHits++
			sf.touch(e)
		} else {
			if len(sf.pages) >= sf.maxPages {
				if err := sf.evictOne(); err != nil {
					return pos, err
				}
			}
			e = sf.allocEntry(pageIdx)
		}
		wasClean := e.dirtyCount == 0
		copy(e.pg.data[inLo:inHi], p[pos:pos+chunkLen])
		e.markDirty(inLo, inHi)
		if wasClean && e.dirtyCount > 0 {
			sf.dirtyCnt++
		}
		sf.stats.SmallWrites++
		sf.stats.PageCopies += uint64(chunkLen)
		if end := curOff + int64(chunkLen); end > sf.highWater {
			sf.highWater = end
		}
		pos += chunkLen
	}
	return len(p), nil
}

// ReadAt reads up to len(p) bytes at off, overlaying any buffered scrap-page
// writes over the backing File so buffered writes are immediately visible.
func (sf *ScrapFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrInvalidOffset
	}
	if len(p) == 0 {
		return 0, nil
	}
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if sf.closed {
		return 0, ErrClosed
	}

	limit := max(sf.f.Size(), sf.highWater)
	if off >= limit {
		return 0, io.EOF
	}
	want := len(p)
	eof := false
	if int64(want) > limit-off {
		want = int(limit - off)
		eof = true
	}

	pos := 0
	for pos < want {
		curOff := off + int64(pos)
		pageIdx := curOff >> 12
		inLo := int(curOff & pageMask)
		inHi := min(PageSize, inLo+(want-pos))
		chunkLen := inHi - inLo

		if e, ok := sf.pages[pageIdx]; ok {
			sf.touch(e)
			if e.hydrated || e.dirtyCount == PageSize {
				copy(p[pos:pos+chunkLen], e.pg.data[inLo:inHi])
				pos += chunkLen
				continue
			}
			// Partial dirty page: read underlying bytes if file holds them, then overlay dirty bytes.
			if curOff < sf.f.Size() {
				rn := min(chunkLen, int(sf.f.Size()-curOff))
				if _, err := sf.f.ReadAt(p[pos:pos+rn], curOff); err != nil && !errors.Is(err, io.EOF) {
					return pos, err
				}
				if rn < chunkLen {
					clear(p[pos+rn : pos+chunkLen])
				}
			} else {
				clear(p[pos : pos+chunkLen])
			}
			for i := inLo; i < inHi; i++ {
				if e.isDirty(i) {
					p[pos+i-inLo] = e.pg.data[i]
				}
			}
			pos += chunkLen
			continue
		}

		// Contiguous unbuffered span until next buffered page or want.
		spanEnd := curOff + int64(want-pos)
		for pIdx := range sf.pages {
			pStart := pIdx * PageSize
			if pStart > curOff && pStart < spanEnd {
				spanEnd = pStart
			}
		}
		spanLen := int(spanEnd - curOff)
		n, err := sf.f.ReadAt(p[pos:pos+spanLen], curOff)
		pos += n
		if err != nil && !errors.Is(err, io.EOF) {
			return pos, err
		}
		if n < spanLen {
			clear(p[pos : pos+(spanLen-n)])
			pos += spanLen - n
		}
	}

	if eof {
		return want, io.EOF
	}
	return want, nil
}

// Flush writes all dirty scrap pages to the backing File in ascending page order
// without issuing Sync.
func (sf *ScrapFile) Flush() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if sf.closed {
		return ErrClosed
	}
	return sf.flushLocked()
}

func (sf *ScrapFile) flushLocked() error {
	if sf.dirtyCnt == 0 {
		return nil
	}
	dirty := sf.flushBuf[:0]
	for _, e := range sf.pages {
		if e.dirtyCount > 0 {
			dirty = append(dirty, e)
		}
	}
	sf.flushBuf = dirty
	// Sort ascending by pageIdx without allocating.
	for i := 1; i < len(dirty); i++ {
		cur := dirty[i]
		j := i - 1
		for j >= 0 && dirty[j].pageIdx > cur.pageIdx {
			dirty[j+1] = dirty[j]
			j--
		}
		dirty[j+1] = cur
	}
	for _, e := range dirty {
		if err := sf.flushEntry(e); err != nil {
			return err
		}
	}
	clear(dirty)
	sf.flushBuf = dirty[:0]
	return nil
}

// Writeback forwards writeback hints after flushing any overlapping dirty scrap
// pages so the kernel sees up-to-date bytes.
func (sf *ScrapFile) Writeback(off, n int64) error {
	if err := sf.Flush(); err != nil {
		return err
	}
	return sf.f.Writeback(off, n)
}

// Sync flushes all dirty scrap pages to the backing File and calls Sync on it.
func (sf *ScrapFile) Sync() error {
	sf.mu.Lock()
	if sf.closed {
		sf.mu.Unlock()
		return ErrClosed
	}
	err := sf.flushLocked()
	f := sf.f
	sf.mu.Unlock()
	if err != nil {
		return err
	}
	return f.Sync()
}

// Size returns the backing File's fixed size (or highWater if larger).
func (sf *ScrapFile) Size() int64 {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return max(sf.f.Size(), sf.highWater)
}

// Stats returns a snapshot of the scrap buffer counters.
func (sf *ScrapFile) Stats() ScrapStats {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	st := sf.stats
	st.DirtyPages = sf.dirtyCnt
	return st
}

// Close flushes dirty scrap pages, returns pooled buffers, and closes the
// backing File.
func (sf *ScrapFile) Close() error {
	sf.mu.Lock()
	if sf.closed {
		sf.mu.Unlock()
		return nil
	}
	err := sf.flushLocked()
	sf.closed = true
	for _, e := range sf.pages {
		sf.pool.put(e.pg)
		e.pg = nil
	}
	clear(sf.pages)
	sf.lruHead, sf.lruTail = nil, nil
	sf.free = nil
	sf.flushBuf = nil
	sf.dirtyCnt = 0
	f := sf.f
	sf.mu.Unlock()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// ScrapDisk wraps a Disk so every opened and created segment file uses ScrapFile.
type ScrapDisk struct {
	Disk
	opt ScrapOptions
}

// NewScrapDisk wraps d with page-aligned scrap buffering for segment files.
func NewScrapDisk(d Disk, opt ScrapOptions) *ScrapDisk {
	return &ScrapDisk{Disk: d, opt: opt.normalized()}
}

func (d *ScrapDisk) Create(name string, size int64) (File, error) {
	f, err := d.Disk.Create(name, size)
	if err != nil {
		return nil, err
	}
	return NewScrapFile(f, d.opt), nil
}

func (d *ScrapDisk) Open(name string) (File, error) {
	f, err := d.Disk.Open(name)
	if err != nil {
		return nil, err
	}
	return NewScrapFile(f, d.opt), nil
}

// ScrapLog is a single-segment durable write-ahead log backed by ScrapFile. It
// preserves the frozen journal record layout and enforces ordered durability
// watermarks (Published <= Durable <= Visible).
type ScrapLog struct {
	sf      *ScrapFile
	seed    uint32
	life    uint64
	lineage lineage

	mu        sync.Mutex
	off       int64
	next      uint64 // next sequence to assign (Visible = next - 1)
	synced    uint64 // Durable watermark
	published uint64 // Published watermark
	marks     uint64
	hdrBuf    [recordHeader]byte
	err       error
	closed    bool
}

// OpenScrapLog opens or initializes a single-segment ScrapLog on f using the
// standard journal preamble and record framing while buffering small unaligned
// writes in page-aligned 4 KiB scrap pages.
func OpenScrapLog(f File, identity []byte, published uint64, opt ScrapOptions) (*ScrapLog, error) {
	opt = opt.normalized()
	opt.AppendOnly = true
	sf := NewScrapFile(f, opt)
	lin := lineageOf(identity)

	size := f.Size()
	if size <= dataStart+recordHeader {
		return nil, fmt.Errorf("journal: segment of %d bytes is too small", size)
	}

	var pre [dataStart]byte
	if _, err := sf.ReadAt(pre[:], 0); err != nil {
		return nil, err
	}
	l := &ScrapLog{
		sf:      sf,
		lineage: lin,
	}

	switch kindOf(pre[:]) {
	case headerLive:
		first, life, hdrLin, _ := parseHeader(pre[:])
		if hdrLin != lin {
			return nil, ErrLineage
		}
		l.life = life
		l.seed = seedOf(life, lin)
		through, floor, newest := parseMarks(pre[:], l.seed)
		l.marks = uint64(newest)
		if published < floor {
			return nil, fmt.Errorf("%w: store holds %d, log reclaimed through %d", ErrMismatch, published, floor)
		}
		nextSeq, endOff, err := scan(sf, l.seed, first, func(record, int64) error { return nil })
		if err != nil {
			return nil, err
		}
		lastSeq := first - 1
		if nextSeq > first {
			lastSeq = nextSeq - 1
		}
		if published > lastSeq {
			return nil, fmt.Errorf("%w: store holds %d, log ends at %d", ErrMismatch, published, lastSeq)
		}
		if through > lastSeq && through > published {
			return nil, fmt.Errorf("%w: records through %d durable, log ends at %d", ErrCorrupt, through, lastSeq)
		}
		l.published = published
		l.synced = max(lastSeq, published)
		l.next = l.synced + 1
		l.off = endOff
		return l, nil
	case headerForeign:
		return nil, ErrFormat
	}

	// Fresh segment: write preamble through ScrapFile so it joins page 0 cleanly.
	var lifeBytes [8]byte
	if _, err := rand.Read(lifeBytes[:]); err != nil {
		return nil, err
	}
	l.life = binary.LittleEndian.Uint64(lifeBytes[:])
	l.seed = seedOf(l.life, lin)
	l.published = published
	l.synced = published
	l.next = published + 1

	var preBuf [dataStart]byte
	buf := appendPreamble(preBuf[:0], l.next, l.life, lin, published, published)
	if _, err := sf.WriteAt(buf, 0); err != nil {
		return nil, err
	}
	if err := sf.Sync(); err != nil {
		return nil, err
	}
	l.off = dataStart
	return l, nil
}

// Append appends payload to the log, absorbing small unaligned records into
// page-aligned scrap pages and advancing the Visible watermark. The record is
// not Durable until Sync returns nil.
func (l *ScrapLog) Append(ctx context.Context, payload []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n := int64(recordHeader) + int64(len(payload))
	payloadSum := crc32.Checksum(payload, castagnoli)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, l.err
	}
	if l.closed {
		return 0, ErrClosed
	}
	if l.off+n > l.sf.Size() {
		return 0, ErrFull
	}

	seq := l.next
	now := time.Now().UnixNano()
	hdr := &l.hdrBuf
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint64(hdr[8:], seq)
	binary.LittleEndian.PutUint64(hdr[16:], uint64(now))
	binary.LittleEndian.PutUint32(hdr[24:], payloadSum)
	binary.LittleEndian.PutUint32(hdr[0:], crc32.Update(l.seed, castagnoli, hdr[4:recordHeader]))

	if _, err := l.sf.WriteAt(hdr[:], l.off); err != nil {
		l.err = err
		return 0, err
	}
	if len(payload) > 0 {
		if _, err := l.sf.WriteAt(payload, l.off+recordHeader); err != nil {
			l.err = err
			return 0, err
		}
	}
	l.off += n
	l.next++
	return seq, nil
}

// WriteAlignedAt writes a >= 4 KiB page-aligned buffer directly to the backing
// File via ScrapFile's zero-copy bypass, advancing high-water offset and
// preserving durability watermarks via Sync.
func (l *ScrapLog) WriteAlignedAt(ctx context.Context, p []byte, off int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, l.err
	}
	if l.closed {
		return 0, ErrClosed
	}
	return l.sf.WriteAt(p, off)
}

// Sync flushes buffered scrap pages to the backing File and syncs it, advancing
// the Durable watermark to cover seq.
func (l *ScrapLog) Sync(ctx context.Context, seq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq <= l.synced {
		return nil
	}
	if l.err != nil {
		return l.err
	}
	if l.closed {
		return ErrClosed
	}
	if seq >= l.next {
		return ErrAhead
	}
	target := l.next - 1
	var markBuf [markSize]byte
	mark := appendMark(markBuf[:0], l.seed, target, l.published)
	markOff := int64(headerSize) + int64((l.marks+1)&1)*markSize
	if _, err := l.sf.WriteAt(mark, markOff); err != nil {
		l.err = err
		return err
	}
	if err := l.sf.Sync(); err != nil {
		l.err = err
		return err
	}
	l.marks++
	l.synced = target
	return nil
}

// Publish advances the Published watermark up to seq. It returns ErrAhead and
// leaves Published unchanged if seq exceeds the Durable watermark.
func (l *ScrapLog) Publish(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq > l.synced {
		return ErrAhead
	}
	if seq > l.published {
		l.published = seq
	}
	return nil
}

// Marks returns the current durability watermarks (Published <= Durable <= Visible).
func (l *ScrapLog) Marks() Marks {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Marks{
		Visible:   l.next - 1,
		Durable:   l.synced,
		Published: l.published,
	}
}

// Replay reads all records strictly above Published, including buffered
// Visible records not yet synced.
func (l *ScrapLog) Replay(fn func(Record) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	var pre [dataStart]byte
	if _, err := l.sf.ReadAt(pre[:], 0); err != nil {
		return err
	}
	first, _, _, ok := parseHeader(pre[:])
	if !ok {
		return ErrCorrupt
	}
	expect := l.next - 1
	nextSeq, _, err := scan(l.sf, l.seed, first, func(r record, _ int64) error {
		if r.seq > expect {
			return errStop
		}
		if r.seq <= l.published {
			return nil
		}
		return fn(Record{Seq: r.seq, Time: time.Unix(0, r.ack), Data: r.payload})
	})
	if err != nil && err != errStop {
		return err
	}
	if err == nil && nextSeq <= expect {
		return ErrCorrupt
	}
	return nil
}

// Stats returns the underlying ScrapFile counters.
func (l *ScrapLog) Stats() ScrapStats { return l.sf.Stats() }

// Close flushes and syncs any pending records, updates the final mark, and
// closes the underlying ScrapFile.
func (l *ScrapLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	var err error
	if l.err == nil && l.next-1 > l.synced {
		target := l.next - 1
		var markBuf [markSize]byte
		mark := appendMark(markBuf[:0], l.seed, target, l.published)
		markOff := int64(headerSize) + int64((l.marks+1)&1)*markSize
		if _, werr := l.sf.WriteAt(mark, markOff); werr != nil {
			err = werr
		} else if serr := l.sf.Sync(); serr != nil {
			err = serr
		} else {
			l.marks++
			l.synced = target
		}
	}
	l.closed = true
	if cerr := l.sf.Close(); err == nil {
		err = cerr
	}
	return err
}
