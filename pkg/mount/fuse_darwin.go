//go:build fuse && darwin

// macOS FUSE driver. bazil.org/fuse dropped macOS support upstream (commit
// 65cc252+); we use github.com/jacobsa/fuse instead, which still supports both
// macFUSE and fuse-t.
//
// Build with: `go build -tags fuse ./...`
// Requires either macFUSE (https://macfuse.io) or fuse-t (https://www.fuse-t.org).
// fuse-t is preferred: no kernel extension, runs as a userspace NFSv4 server,
// works under SIP.
//
// A mounted Volume is durable at FlushFile and SyncFile, which return only after
// the Volume's Head has advanced in the store, and once more when the mount ends.
package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/hanzoai/vfs"
)

// Mount mounts vol at mountpoint and serves until ctx is cancelled or the kernel
// unmounts it, then flushes the Volume. The caller owns vol and closes it.
func Mount(ctx context.Context, vol *vfs.Volume, mountpoint string) error {
	if vol == nil {
		return errors.New("mount: Volume required")
	}
	if mountpoint == "" {
		return errors.New("mount: mountpoint required")
	}
	mfs, err := fuse.Mount(mountpoint, fuseutil.NewFileSystemServer(newServer(vol)), &fuse.MountConfig{
		FSName:                    "vfs",
		Subtype:                   "hanzovfs",
		ReadOnly:                  false,
		EnableNoOpenSupport:       true,
		EnableNoOpendirSupport:    true,
		DisableDefaultPermissions: false,
	})
	if err != nil {
		return fmt.Errorf("fuse.Mount %q: %w", mountpoint, err)
	}
	go func() {
		<-ctx.Done()
		_ = fuse.Unmount(mountpoint)
	}()
	return finish(vol, mfs.Join(context.Background()))
}

// server maps FUSE inode numbers straight onto Volume inodes; both start the
// root at 1.
type server struct {
	fuseutil.NotImplementedFileSystem
	vol   *vfs.Volume
	group *committer

	mu      sync.Mutex
	next    fuseops.HandleID
	handles map[fuseops.HandleID]*vfs.File
}

func newServer(vol *vfs.Volume) *server {
	return &server{vol: vol, group: newCommitter(vol), next: 1, handles: map[fuseops.HandleID]*vfs.File{}}
}

func attrs(in vfs.Inode) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Size: in.Size, Nlink: 1, Mode: os.FileMode(in.Mode),
		Atime: in.Atime, Mtime: in.Mtime, Ctime: in.Ctime, Crtime: in.Ctime,
		Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid()),
	}
}

func entry(in vfs.Inode) fuseops.ChildInodeEntry {
	exp := time.Now().Add(time.Second)
	return fuseops.ChildInodeEntry{
		Child: fuseops.InodeID(in.ID), Attributes: attrs(in),
		AttributesExpiration: exp, EntryExpiration: exp,
	}
}

func (s *server) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Lookup(ctx, vfs.InodeID(op.Parent), op.Name)
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	return nil
}

func (s *server) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Stat(ctx, vfs.InodeID(op.Inode))
	if err != nil {
		return errno(err)
	}
	op.Attributes = attrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

func (s *server) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	// Only the size can be set; the rest would succeed without changing anything.
	if op.Mode != nil || op.Atime != nil || op.Mtime != nil {
		return unsupported
	}
	if op.Size != nil {
		if err := s.vol.Truncate(ctx, vfs.InodeID(op.Inode), *op.Size); err != nil {
			return errno(err)
		}
	}
	in, err := s.vol.Stat(ctx, vfs.InodeID(op.Inode))
	if err != nil {
		return errno(err)
	}
	op.Attributes = attrs(in)
	op.AttributesExpiration = time.Now().Add(time.Second)
	return nil
}

func (s *server) MkDir(ctx context.Context, op *fuseops.MkDirOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Mkdir(ctx, vfs.InodeID(op.Parent), op.Name, op.Mode.Perm())
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	return nil
}

func (s *server) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	in, err := s.vol.Create(ctx, vfs.InodeID(op.Parent), op.Name, op.Mode.Perm())
	if err != nil {
		return errno(err)
	}
	f, err := s.vol.Open(ctx, in.ID)
	if err != nil {
		return errno(err)
	}
	op.Entry = entry(in)
	op.Handle = s.add(f)
	return nil
}

func (s *server) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	if err := kind(ctx, s.vol, vfs.InodeID(op.Parent), op.Name, false); err != nil {
		return errno(err)
	}
	return errno(s.vol.Remove(ctx, vfs.InodeID(op.Parent), op.Name))
}

func (s *server) RmDir(ctx context.Context, op *fuseops.RmDirOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	if err := kind(ctx, s.vol, vfs.InodeID(op.Parent), op.Name, true); err != nil {
		return errno(err)
	}
	return errno(s.vol.Remove(ctx, vfs.InodeID(op.Parent), op.Name))
}

// kind checks that name in dir is the kind of thing the caller is removing:
// unlink removes a file and refuses a directory, rmdir the reverse. (The Linux
// kernel makes this check itself before it asks the file system; an NFS-backed
// mount does not.)
func kind(ctx context.Context, vol *vfs.Volume, dir vfs.InodeID, name string, wantDir bool) error {
	in, err := vol.Lookup(ctx, dir, name)
	if err != nil {
		return err
	}
	switch {
	case wantDir && !in.IsDir():
		return vfs.ErrNotDir
	case !wantDir && in.IsDir():
		return vfs.ErrIsDir
	}
	return nil
}

// ─── directories ────────────────────────────────────────────────────────────

func (s *server) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	op.Handle = s.add(nil) // a directory needs a handle id, not a file
	return nil
}

func (s *server) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	ents, err := s.vol.ReadDir(ctx, vfs.InodeID(op.Inode))
	if err != nil {
		return errno(err)
	}
	for i, e := range ents {
		if i < int(op.Offset) {
			continue
		}
		typ := fuseutil.DT_File
		if e.Mode.IsDir() {
			typ = fuseutil.DT_Directory
		}
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], fuseutil.Dirent{
			Offset: fuseops.DirOffset(i + 1), Inode: fuseops.InodeID(e.ID), Name: e.Name, Type: typ,
		})
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	return nil
}

func (s *server) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error {
	s.drop(op.Handle)
	return nil
}

// ─── files ──────────────────────────────────────────────────────────────────

func (s *server) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	f, err := s.vol.Open(ctx, vfs.InodeID(op.Inode))
	if err != nil {
		return errno(err)
	}
	op.Handle = s.add(f)
	op.KeepPageCache = false
	return nil
}

func (s *server) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	f := s.file(op.Handle)
	if f == nil {
		return syscall.EBADF
	}
	n, err := f.ReadAt(ctx, op.Dst, op.Offset)
	op.BytesRead = n
	if err != nil && !errors.Is(err, io.EOF) {
		return errno(err)
	}
	return nil
}

func (s *server) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	f := s.file(op.Handle)
	if f == nil {
		return syscall.EBADF
	}
	_, err := f.WriteAt(ctx, op.Data, op.Offset)
	return errno(err)
}

// A commit is not abandoned halfway because the kernel interrupted the call: it is
// repeatable and safe to finish, so it runs to its end.
func (s *server) SyncFile(ctx context.Context, _ *fuseops.SyncFileOp) error {
	ctx = context.WithoutCancel(ctx) // no request is answered EINTR: see errno.go
	return errno(s.group.commit())
}

func (s *server) FlushFile(ctx context.Context, _ *fuseops.FlushFileOp) error {
	return errno(s.group.commit()) // close(2) is not restarted: wait for the commit
}

func (s *server) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	if f := s.file(op.Handle); f != nil {
		_ = f.Close()
	}
	s.drop(op.Handle)
	return nil
}

func (s *server) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	op.BlockSize = blockSize
	// The store has no capacity to run out of: report about a pebibyte free.
	op.Blocks = 1 << 38
	op.BlocksFree = 1 << 38
	op.BlocksAvailable = 1 << 38
	op.IoSize = blockSize
	return nil
}

// ─── handle table ───────────────────────────────────────────────────────────

func (s *server) add(f *vfs.File) fuseops.HandleID {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.next
	s.next++
	s.handles[id] = f
	return id
}

func (s *server) file(id fuseops.HandleID) *vfs.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handles[id]
}

func (s *server) drop(id fuseops.HandleID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, id)
}
