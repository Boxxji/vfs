// Package crash is a journal.Disk in memory that logs every persistence point
// and can rebuild the disk a crash would have left behind after any prefix of
// that log. It is the harness for systematic crash exploration in the manner
// of CrashMonkey and ACE (Mohan et al., OSDI 2018; Bornholt et al., ASPLOS
// 2016): instead of killing a process at random moments, run a workload once,
// then check the recovery of every crash state the log allows.
//
// The model is the weakest disk the contract may assume. A write is volatile
// until a Sync that began after it completes. At a crash any subset of the
// volatile writes may have reached the media, in any order, and each may be
// torn at a sector boundary or inside a sector. Create and Remove are durable
// when they return, as the journal.Disk contract says.
package crash

import (
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"

	"github.com/hanzoai/vfs/journal"
)

// Sector is the unit a write may be torn at.
const Sector = 512

type kind int

const (
	opCreate kind = iota
	opRemove
	opWrite
	opSync
	opMark
)

type op struct {
	kind   kind
	name   string
	off    int64
	data   []byte // write: the bytes
	size   int64  // create: the length
	covers int    // sync: writes logged before this index are made durable
	note   any    // mark
}

// Event describes a file operation about to happen, for Intercept.
type Event struct {
	Kind string // "create", "remove", "write", "writeback" or "sync"
	Name string
	Off  int64
	Len  int
}

// Disk implements journal.Disk. Create one with New.
type Disk struct {
	mu    sync.Mutex
	files map[string]*content
	base  map[string][]byte // durable before the first point of the log
	log   []op
	hook  func(Event) error
}

type content struct{ data []byte }

// New returns an empty disk.
func New() *Disk { return &Disk{files: map[string]*content{}, base: map[string][]byte{}} }

// Intercept installs f, which is called before every file operation without
// the disk's lock held. It may block to hold the operation, and a non-nil
// error fails the operation without effect. Nil removes it.
func (d *Disk) Intercept(f func(Event) error) {
	d.mu.Lock()
	d.hook = f
	d.mu.Unlock()
}

func (d *Disk) intercept(e Event) error {
	d.mu.Lock()
	h := d.hook
	d.mu.Unlock()
	if h == nil {
		return nil
	}
	return h(e)
}

// Len is the number of points in the log.
func (d *Disk) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.log)
}

// Mark logs an application event, such as "Sync returned for 7", at the
// current point. A check reads the events before a crash point with Marks.
func (d *Disk) Mark(note any) {
	d.mu.Lock()
	d.log = append(d.log, op{kind: opMark, note: note})
	d.mu.Unlock()
}

// Marks returns the events logged before point n, in order.
func (d *Disk) Marks(n int) []any {
	d.mu.Lock()
	defer d.mu.Unlock()
	var notes []any
	for _, o := range d.log[:n] {
		if o.kind == opMark {
			notes = append(notes, o.note)
		}
	}
	return notes
}

func (d *Disk) List() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	names := make([]string, 0, len(d.files))
	for name := range d.files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Close does nothing: the disk outlives the journals opened on it, so a test can
// close one and open the next on the same files.
func (d *Disk) Close() error { return nil }

func (d *Disk) Create(name string, size int64) (journal.File, error) {
	if err := d.intercept(Event{"create", name, 0, int(size)}); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.files[name]; ok {
		return nil, fmt.Errorf("crash: %s exists", name)
	}
	c := &content{make([]byte, size)}
	d.files[name] = c
	d.log = append(d.log, op{kind: opCreate, name: name, size: size})
	return &file{d, name, c}, nil
}

func (d *Disk) Open(name string) (journal.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, ok := d.files[name]
	if !ok {
		return nil, fmt.Errorf("crash: %s: no such file", name)
	}
	return &file{d, name, c}, nil
}

func (d *Disk) Remove(name string) error {
	if err := d.intercept(Event{"remove", name, 0, 0}); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.files[name]; !ok {
		return fmt.Errorf("crash: %s: no such file", name)
	}
	delete(d.files, name)
	d.log = append(d.log, op{kind: opRemove, name: name})
	return nil
}

type file struct {
	d    *Disk
	name string
	c    *content
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if off >= int64(len(f.c.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.c.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *file) WriteAt(p []byte, off int64) (int, error) {
	if err := f.d.intercept(Event{"write", f.name, off, len(p)}); err != nil {
		return 0, err
	}
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if off+int64(len(p)) > int64(len(f.c.data)) {
		return 0, fmt.Errorf("crash: write past the end of %s", f.name)
	}
	copy(f.c.data[off:], p)
	f.d.log = append(f.d.log, op{kind: opWrite, name: f.name, off: off, data: append([]byte(nil), p...)})
	return len(p), nil
}

// Writeback is a hint with no effect on what a crash leaves; it is reported to
// Intercept so a test can see it.
func (f *file) Writeback(off, n int64) error {
	return f.d.intercept(Event{"writeback", f.name, off, int(n)})
}

func (f *file) Sync() error {
	f.d.mu.Lock()
	covers := len(f.d.log)
	f.d.mu.Unlock()
	if err := f.d.intercept(Event{"sync", f.name, 0, 0}); err != nil {
		return err
	}
	f.d.mu.Lock()
	f.d.log = append(f.d.log, op{kind: opSync, name: f.name, covers: covers})
	f.d.mu.Unlock()
	return nil
}

func (f *file) Size() int64 {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return int64(len(f.c.data))
}

func (f *file) Close() error { return nil }

// Kind says how the volatile writes at a crash point reached the media.
type Kind int

const (
	// Lost: none did. Only what a completed Sync covered survives.
	Lost Kind = iota
	// All: every one did, as after a process crash with the machine up.
	All
	// Prefix: the first N did, in order.
	Prefix
	// Torn: the first N-1 did and the Nth reached the media only up to Keep bytes.
	Torn
	// Scatter: each write independently did or did not, and each sector of it
	// independently, chosen by Seed.
	Scatter
	// Subset: write i did if bit i of Mask is set, whole or not at all, in any
	// order relative to the others.
	Subset
)

// Variant is one way a crash can leave the volatile writes.
type Variant struct {
	Kind Kind
	N    int
	Keep int
	Seed int64
	Mask uint64
}

func (v Variant) String() string {
	switch v.Kind {
	case Lost:
		return "lost"
	case All:
		return "all"
	case Prefix:
		return fmt.Sprintf("prefix %d", v.N)
	case Torn:
		return fmt.Sprintf("torn write %d after %d bytes", v.N, v.Keep)
	case Subset:
		return fmt.Sprintf("subset %b", v.Mask)
	default:
		return fmt.Sprintf("scatter seed %d", v.Seed)
	}
}

type pending struct {
	index int
	name  string
	off   int64
	data  []byte
}

// durable replays the first n points and returns the files that are durable
// and the writes still volatile.
func (d *Disk) durable(n int) (map[string][]byte, []pending) {
	d.mu.Lock()
	defer d.mu.Unlock()
	base := map[string][]byte{}
	for name, data := range d.base {
		base[name] = append([]byte(nil), data...)
	}
	var volatile []pending
	for i, o := range d.log[:n] {
		switch o.kind {
		case opCreate:
			base[o.name] = make([]byte, o.size)
			volatile = without(volatile, o.name)
		case opRemove:
			delete(base, o.name)
			volatile = without(volatile, o.name)
		case opWrite:
			volatile = append(volatile, pending{i, o.name, o.off, o.data})
		case opSync:
			kept := volatile[:0]
			for _, w := range volatile {
				if w.name == o.name && w.index < o.covers {
					if b, ok := base[w.name]; ok {
						copy(b[w.off:], w.data)
					}
				} else {
					kept = append(kept, w)
				}
			}
			volatile = kept
		}
	}
	return base, volatile
}

func without(ws []pending, name string) []pending {
	kept := ws[:0]
	for _, w := range ws {
		if w.name != name {
			kept = append(kept, w)
		}
	}
	return kept
}

// Variants lists the crash states worth checking at point n: nothing
// persisted, everything persisted, each prefix of the volatile writes, the
// last few writes torn at assorted offsets, and scatter random states.
func (d *Disk) Variants(n, scatter int, seed int64) []Variant {
	_, volatile := d.durable(n)
	vs := []Variant{{Kind: Lost}}
	p := len(volatile)
	if p == 0 {
		return vs
	}
	vs = append(vs, Variant{Kind: All})
	for m := 1; m < p; m++ {
		vs = append(vs, Variant{Kind: Prefix, N: m})
	}
	// Every subset of a few writes, so an ordering that must hold is tried in
	// both directions; for more writes, each one alone and each one missing.
	all := uint64(1)<<min(p, 63) - 1
	if p <= 6 {
		for mask := uint64(1); mask < all; mask++ {
			vs = append(vs, Variant{Kind: Subset, Mask: mask})
		}
	} else {
		for i := 0; i < 8; i++ {
			vs = append(vs, Variant{Kind: Subset, Mask: 1 << i}, Variant{Kind: Subset, Mask: all &^ (1 << i)})
		}
	}
	for i := max(1, p-2); i <= p; i++ {
		size := len(volatile[i-1].data)
		for _, keep := range []int{1, 100, Sector, Sector + 100, size / 2 / Sector * Sector, size - 1} {
			if keep > 0 && keep < size {
				vs = append(vs, Variant{Kind: Torn, N: i, Keep: keep})
			}
		}
	}
	for s := 0; s < scatter; s++ {
		vs = append(vs, Variant{Kind: Scatter, Seed: seed + int64(s)})
	}
	return vs
}

// Image returns the disk a restart finds after a crash at point n with the
// volatile writes left as v says. The image has an empty log of its own.
func (d *Disk) Image(n int, v Variant) *Disk {
	base, volatile := d.durable(n)
	rng := rand.New(rand.NewSource(v.Seed))
	for i, w := range volatile {
		b, ok := base[w.name]
		if !ok {
			continue
		}
		switch v.Kind {
		case Lost:
		case All:
			copy(b[w.off:], w.data)
		case Prefix:
			if i < v.N {
				copy(b[w.off:], w.data)
			}
		case Torn:
			if i < v.N-1 {
				copy(b[w.off:], w.data)
			} else if i == v.N-1 {
				copy(b[w.off:], w.data[:v.Keep])
			}
		case Subset:
			if i < 64 && v.Mask>>i&1 == 1 {
				copy(b[w.off:], w.data)
			}
		case Scatter:
			if rng.Intn(2) == 0 {
				continue
			}
			for s := 0; s < len(w.data); {
				end := min(len(w.data), (int(w.off)+s)/Sector*Sector+Sector-int(w.off))
				if rng.Intn(4) != 0 {
					copy(b[int(w.off)+s:], w.data[s:end])
				}
				s = end
			}
		}
	}
	img := New()
	for name, data := range base {
		img.files[name] = &content{append([]byte(nil), data...)}
		img.base[name] = data
	}
	return img
}

// Explore calls check on the image of every crash state: each point of the log
// with each of its variants. It stops at the first error and returns it with
// the point and the variant.
func (d *Disk) Explore(scatter int, seed int64, check func(img *Disk, point int, v Variant) error) error {
	total := d.Len()
	for n := 0; n <= total; n++ {
		for _, v := range d.Variants(n, scatter, seed+int64(n)*1000) {
			if err := check(d.Image(n, v), n, v); err != nil {
				return fmt.Errorf("crash at point %d of %d, %s: %w", n, total, v, err)
			}
		}
	}
	return nil
}

// Log describes the first n points of the log, one per line, for failure messages.
func (d *Disk) Log(n int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var s string
	for i, o := range d.log[:n] {
		switch o.kind {
		case opCreate:
			s += fmt.Sprintf("%3d create %s %d\n", i, o.name, o.size)
		case opRemove:
			s += fmt.Sprintf("%3d remove %s\n", i, o.name)
		case opWrite:
			s += fmt.Sprintf("%3d write  %s @%d len %d\n", i, o.name, o.off, len(o.data))
		case opSync:
			s += fmt.Sprintf("%3d sync   %s covers <%d\n", i, o.name, o.covers)
		case opMark:
			s += fmt.Sprintf("%3d mark   %v\n", i, o.note)
		}
	}
	return s
}
