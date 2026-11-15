package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Disk is the file layer the journal writes through. The journal never
// touches the operating system directly, so a test double can record every
// persistence point and replay a crash after each one.
//
// Names are flat (no directories). A Disk may be used by one Journal at a time.
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
}

// File is one segment file. WriteAt and ReadAt may run concurrently with each
// other and with Sync.
type File interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	// Writeback asks the system to start writing the range to the media now,
	// without waiting and without any guarantee. The journal asks as it writes,
	// so a later Sync finds most of the data already on its way and does not
	// make the callers behind it wait for a large flush.
	Writeback(off, n int64) error
	// Sync makes every write that completed before Sync was called durable,
	// without the cost of flushing file metadata that did not change (fdatasync).
	// A write that overlaps the call may or may not be covered.
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

// directory is the Disk over a directory of the operating system's files.
type directory struct{ path string }

// Files returns the Disk over the operating system's files in dir, creating
// the directory if needed. It is what Open uses when Options.Disk is nil; a
// caller wraps it to observe or count what the journal does to the files.
func Files(dir string) (Disk, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return directory{dir}, nil
}

// List also removes the temporary files an interrupted Create leaves behind.
func (d directory) List() ([]string, error) {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		switch {
		case !e.Type().IsRegular():
		case strings.HasSuffix(e.Name(), tempSuffix):
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
func (d directory) Create(name string, size int64) (File, error) {
	final := filepath.Join(d.path, name)
	temp := final + tempSuffix
	f, err := os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
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
	if err := d.sync(); err != nil {
		f.Close()
		return nil, err
	}
	return &osFile{f, size}, nil
}

func (d directory) Open(name string) (File, error) {
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

func (d directory) Remove(name string) error {
	if err := os.Remove(filepath.Join(d.path, name)); err != nil {
		return err
	}
	return d.sync()
}

func (d directory) sync() error {
	f, err := os.Open(d.path)
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

func (f *osFile) Writeback(off, n int64) error { return writeback(f.File, off, n) }
func (f *osFile) Size() int64                  { return f.size }
