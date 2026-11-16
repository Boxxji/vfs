package journal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Disk is the file layer the journal writes through. The journal never
// touches the operating system directly, so a test double can record every
// persistence point and replay a crash after each one.
//
// Names are flat (no directories). A Disk may be used by one Journal at a time,
// and the Journal owns it from Open to Close.
type Disk interface {
	// List returns the names of the files in the journal's directory.
	List() ([]string, error)
	// Create makes a file of size bytes with every byte written (never sparse,
	// never unwritten extents: overwriting allocated blocks is what makes a
	// later Sync cheap) and returns it open. The file and its directory entry
	// are durable when Create returns.
	Create(name string, size int64) (File, error)
	// Open opens an existing file for reading and writing.
	Open(name string) (File, error)
	// Remove deletes a file. The deletion is durable when Remove returns.
	Remove(name string) error
	// Close releases what the Disk holds, such as the lock on its directory. The
	// journal calls it from Close, and after an Open that failed.
	Close() error
}

// File is one segment file. WriteAt and ReadAt may run concurrently with each
// other and with Sync.
type File interface {
	ReadAt(p []byte, off int64) (int, error)
	// WriteAt writes all of p, or returns an error.
	WriteAt(p []byte, off int64) (int, error)
	// Writeback asks the system to start writing the range to the media now,
	// without waiting and without any guarantee. The journal asks as it writes,
	// so a later Sync finds most of the data already on its way and does not
	// make the callers behind it wait for a large flush.
	Writeback(off, n int64) error
	// Sync makes every write that completed before Sync was called durable,
	// without the cost of flushing file metadata that did not change (fdatasync).
	// A write that overlaps the call may or may not be covered. On macOS the
	// operating system call behind Go's File.Sync is F_FULLFSYNC, which asks the
	// drive to flush its own cache and is much slower than fdatasync on Linux.
	Sync() error
	// Size is the file's length in bytes. It never changes.
	Size() int64
	Close() error
}

const (
	slotSuffix = ".seg"
	tempSuffix = ".tmp"
)

func slotName(id uint64) string { return fmt.Sprintf("%06d%s", id, slotSuffix) }

func parseSlot(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, slotSuffix)
	if !ok || digits == "" {
		return 0, false
	}
	var id uint64
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
		id = id*10 + uint64(c-'0')
	}
	return id, true
}

// directory is the Disk over a directory of the operating system's files. It
// holds an exclusive lock on the directory until it is closed, or until the
// process dies, when the kernel releases it.
type directory struct {
	path     string
	lock     *os.File
	warnings []string
}

// Files returns the Disk over the operating system's files in dir, creating the
// directory (readable by its owner alone: the journal holds every write of the
// volume in the clear; the Volume seals what it appends) and the entry of each
// directory it created in its parent durably. A directory that already exists
// is used as it is, with the mode it has; the files in it are 0600 in any case. It takes an exclusive lock on the directory
// and returns ErrLocked if another journal has it, and ErrVolatile for a
// directory on memory, where a sync makes nothing durable. It is what Open uses
// when Options.Disk is nil; a caller wraps it to observe or count what the
// journal does to the files.
func Files(dir string) (Disk, error) {
	dir = filepath.Clean(dir)
	// The directories MkdirAll is about to make, innermost first: each one's
	// entry is in its parent, and a parent's entry is in the next one up.
	var made []string
	for d := dir; ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil || !errors.Is(err, fs.ErrNotExist) || d == filepath.Dir(d) {
			break
		}
		made = append(made, d)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if len(made) == 0 {
		made = []string{dir} // it was there; its entry may still be unsynced from a crash
	}
	for _, d := range made {
		if err := syncDirectory(filepath.Dir(d)); err != nil {
			return nil, err
		}
	}
	warnings, err := inspect(dir)
	if err != nil {
		return nil, err
	}
	lock, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}
	return &directory{path: dir, lock: lock, warnings: warnings}, nil
}

// Warnings lists the risks of the directory's file system.
func (d *directory) Warnings() []string { return d.warnings }

func (d *directory) Close() error {
	if d.lock == nil {
		return nil
	}
	err := d.lock.Close()
	d.lock = nil
	return err
}

// ownTemp reports whether name is the temporary file of a Create: a segment's
// name and the suffix. Any other file that ends in the suffix is not the journal's.
func ownTemp(name string) bool {
	base, ok := strings.CutSuffix(name, tempSuffix)
	if !ok {
		return false
	}
	_, ok = parseSlot(base)
	return ok
}

// List also removes the temporary files an interrupted Create leaves behind.
func (d *directory) List() ([]string, error) {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		switch {
		case !e.Type().IsRegular():
		case ownTemp(e.Name()):
			if err := os.Remove(filepath.Join(d.path, e.Name())); err != nil {
				return nil, err
			}
		default:
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// Create writes the whole file under a temporary name, syncs it, then renames
// it into place and syncs the directory, so a crash leaves either no file or a
// complete one.
func (d *directory) Create(name string, size int64) (File, error) {
	final := filepath.Join(d.path, name)
	temp := final + tempSuffix
	f, err := os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	zeros := make([]byte, 1<<20)
	for off := int64(0); off < size; {
		n := min(int64(len(zeros)), size-off)
		if _, err := f.WriteAt(zeros[:n], off); err != nil {
			f.Close()
			os.Remove(temp)
			return nil, err
		}
		off += n
	}
	if err := datasync(f); err != nil {
		f.Close()
		os.Remove(temp)
		return nil, err
	}
	if err := os.Rename(temp, final); err != nil {
		f.Close()
		os.Remove(temp)
		return nil, err
	}
	if err := syncDirectory(d.path); err != nil {
		f.Close()
		return nil, err
	}
	return &osFile{f, size}, nil
}

func (d *directory) Open(name string) (File, error) {
	f, err := os.OpenFile(filepath.Join(d.path, name), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &osFile{f, info.Size()}, nil
}

func (d *directory) Remove(name string) error {
	if err := os.Remove(filepath.Join(d.path, name)); err != nil {
		return err
	}
	return syncDirectory(d.path)
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

type osFile struct {
	*os.File
	size int64
}

func (f *osFile) Sync() error { return datasync(f.File) }
func (f *osFile) Size() int64 { return f.size }

func (f *osFile) Writeback(off, n int64) error { return writeback(f.File, off, n) }
