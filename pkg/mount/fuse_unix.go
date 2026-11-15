//go:build fuse && linux

// NOTE: bazil.org/fuse dropped macOS support (commit 65cc252+); macOS uses
// jacobsa/fuse in fuse_darwin.go.

// Package mount serves a *vfs.Volume to the kernel over FUSE, so any POSIX
// program (SQLite, Postgres, ripgrep) reads and writes the sealed Volume as a
// mounted directory.
//
// A mounted Volume is durable at Flush, which the kernel asks for on every close
// of a file descriptor, and at Fsync; both return only after the Volume's Head
// has advanced in the store, so each costs a commit. There is no node-local
// journal yet to make Fsync cheaper. When the mount ends the Volume is flushed
// once more.
//
// Build-tag gated: `go build -tags fuse ./...`.
package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"bazil.org/fuse"
	bzfs "bazil.org/fuse/fs"

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
	conn, err := fuse.Mount(mountpoint,
		fuse.FSName("vfs"),
		fuse.Subtype("hanzovfs"),
		fuse.AllowNonEmptyMount(),
	)
	if err != nil {
		return fmt.Errorf("fuse.Mount %q: %w", mountpoint, err)
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		_ = fuse.Unmount(mountpoint)
	}()

	var serveErr error
	if err := bzfs.New(conn, nil).Serve(&root{vol: vol, base: ctx}); err != nil {
		serveErr = fmt.Errorf("fuse.Serve: %w", err)
	}
	return finish(vol, serveErr)
}

// root is the file system. base is the Mount context: the work of a read runs on
// it, not on the request that asked, so an interrupted request abandons only its wait.
type root struct {
	vol  *vfs.Volume
	base context.Context
}

var _ bzfs.FS = (*root)(nil)

func (r *root) Root() (bzfs.Node, error) {
	return &node{vol: r.vol, base: r.base, ino: vfs.RootInode, parent: vfs.RootInode}, nil
}

// node is a file or directory, named by its inode.
type node struct {
	vol    *vfs.Volume
	base   context.Context
	ino    vfs.InodeID
	parent vfs.InodeID // for ".."; every inode has one name, so one parent
}

var (
	_ bzfs.Node               = (*node)(nil)
	_ bzfs.NodeStringLookuper = (*node)(nil)
	_ bzfs.HandleReadDirAller = (*node)(nil)
	_ bzfs.NodeMkdirer        = (*node)(nil)
	_ bzfs.NodeCreater        = (*node)(nil)
	_ bzfs.NodeRemover        = (*node)(nil)
	_ bzfs.NodeSetattrer      = (*node)(nil)
	_ bzfs.NodeFsyncer        = (*node)(nil)
	_ bzfs.NodeOpener         = (*node)(nil)
	_ bzfs.NodeRenamer        = (*node)(nil)
	_ bzfs.NodeLinker         = (*node)(nil)
	_ bzfs.NodeSymlinker      = (*node)(nil)
)

func attr(a *fuse.Attr, in vfs.Inode) {
	a.Inode = uint64(in.ID)
	a.Mode = os.FileMode(in.Mode)
	a.Size = in.Size
	a.Blocks = (in.Size + 511) / 512
	a.Mtime, a.Atime, a.Ctime = in.Mtime, in.Atime, in.Ctime
	a.Nlink = 1
	a.Uid, a.Gid = uint32(os.Getuid()), uint32(os.Getgid())
	a.BlockSize = blockSize
}

func (n *node) Attr(ctx context.Context, a *fuse.Attr) error {
	in, err := n.vol.Stat(ctx, n.ino)
	if err != nil {
		return errno(err)
	}
	attr(a, in)
	return nil
}

func (n *node) Lookup(ctx context.Context, name string) (bzfs.Node, error) {
	in, err := n.vol.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, errno(err)
	}
	return &node{vol: n.vol, ino: in.ID, parent: n.ino}, nil
}

func (n *node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	ents, err := n.vol.ReadDir(ctx, n.ino)
	if err != nil {
		return nil, errno(err)
	}
	// A listing starts with "." and "..": POSIX requires them, and the kernel does
	// not make them up for a file system.
	out := make([]fuse.Dirent, 0, len(ents)+2)
	out = append(out,
		fuse.Dirent{Inode: uint64(n.ino), Name: ".", Type: fuse.DT_Dir},
		fuse.Dirent{Inode: uint64(n.parent), Name: "..", Type: fuse.DT_Dir})
	for _, e := range ents {
		typ := fuse.DT_File
		if e.Mode.IsDir() {
			typ = fuse.DT_Dir
		}
		out = append(out, fuse.Dirent{Inode: uint64(e.ID), Name: e.Name, Type: typ})
	}
	return out, nil
}

func (n *node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (bzfs.Node, error) {
	ctx = context.WithoutCancel(ctx) // not answered EINTR: see errno.go
	in, err := n.vol.Mkdir(ctx, n.ino, req.Name, req.Mode.Perm())
	if err != nil {
		return nil, errno(err)
	}
	return &node{vol: n.vol, ino: in.ID, parent: n.ino}, nil
}

func (n *node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (bzfs.Node, bzfs.Handle, error) {
	ctx = context.WithoutCancel(ctx) // not answered EINTR: see errno.go
	in, err := n.vol.Create(ctx, n.ino, req.Name, req.Mode.Perm())
	if err != nil {
		return nil, nil, errno(err)
	}
	f, err := n.vol.Open(ctx, in.ID)
	if err != nil {
		return nil, nil, errno(err)
	}
	attr(&resp.Attr, in)
	return &node{vol: n.vol, base: n.base, ino: in.ID, parent: n.ino}, &handle{vol: n.vol, base: n.base, file: f}, nil
}

func (n *node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	ctx = context.WithoutCancel(ctx) // not answered EINTR: see errno.go
	return errno(n.vol.Remove(ctx, n.ino, req.Name))
}

// Rename, Link and Symlink are not built: they answer "not supported".
func (n *node) Rename(context.Context, *fuse.RenameRequest, bzfs.Node) error { return unsupported }

func (n *node) Link(context.Context, *fuse.LinkRequest, bzfs.Node) (bzfs.Node, error) {
	return nil, unsupported
}

func (n *node) Symlink(context.Context, *fuse.SymlinkRequest) (bzfs.Node, error) {
	return nil, unsupported
}

func (n *node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ctx = context.WithoutCancel(ctx) // not answered EINTR: see errno.go
	// Only the size can be set. chmod, chown and utimens would succeed here without
	// changing anything, which tar, make and rsync would believe: they are refused.
	if req.Valid.Mode() || req.Valid.Uid() || req.Valid.Gid() || req.Valid.Atime() || req.Valid.Mtime() {
		return unsupported
	}
	if req.Valid.Size() {
		if err := n.vol.Truncate(ctx, n.ino, req.Size); err != nil {
			return errno(err)
		}
	}
	in, err := n.vol.Stat(ctx, n.ino)
	if err != nil {
		return errno(err)
	}
	attr(&resp.Attr, in)
	return nil
}

func (n *node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (bzfs.Handle, error) {
	ctx = context.WithoutCancel(ctx) // not answered EINTR: see errno.go
	in, err := n.vol.Stat(ctx, n.ino)
	if err != nil {
		return nil, errno(err)
	}
	if in.IsDir() {
		return n, nil // served by ReadDirAll
	}
	if req.Flags&fuse.OpenFlags(os.O_TRUNC) != 0 {
		if err := n.vol.Truncate(ctx, n.ino, 0); err != nil {
			return nil, errno(err)
		}
	}
	f, err := n.vol.Open(ctx, n.ino)
	if err != nil {
		return nil, errno(err)
	}
	return &handle{vol: n.vol, base: n.base, file: f}, nil
}

func (n *node) Fsync(ctx context.Context, _ *fuse.FsyncRequest) error {
	return errno(commit(ctx, n.vol))
}

// handle is an open file.
type handle struct {
	vol  *vfs.Volume
	base context.Context
	file *vfs.File
}

var (
	_ bzfs.Handle         = (*handle)(nil)
	_ bzfs.HandleReader   = (*handle)(nil)
	_ bzfs.HandleWriter   = (*handle)(nil)
	_ bzfs.HandleReleaser = (*handle)(nil)
	_ bzfs.HandleFlusher  = (*handle)(nil)
)

func (h *handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	size, off := req.Size, req.Offset
	data, err := own(context.WithoutCancel(ctx), h.base, func(c context.Context) ([]byte, error) {
		buf := make([]byte, size)
		n, err := h.file.ReadAt(c, buf, off)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		return buf[:n], nil
	})
	if err != nil {
		return errno(err)
	}
	resp.Data = data
	return nil
}

func (h *handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	ctx = context.WithoutCancel(ctx) // not answered EINTR: see errno.go
	n, err := h.file.WriteAt(ctx, req.Data, req.Offset)
	if err != nil {
		return errno(err)
	}
	resp.Size = n
	return nil
}

// Flush is a commit, and it is the one request that is not answered EINTR when the
// kernel interrupts it: a signal that lands while the caller is in close(2) reaches
// here as an interrupt, but nothing restarts close (Linux has already dropped the
// descriptor, and Go's os.File.Close returns the error as it is). So the caller waits
// for the commit to end, whatever signals arrive, and sees its result. The commit
// runs on a context of its own either way. Fsync, which callers do restart, abandons
// only its wait.
func (h *handle) Flush(ctx context.Context, _ *fuse.FlushRequest) error {
	return errno(commit(context.WithoutCancel(ctx), h.vol))
}

func (h *handle) Release(context.Context, *fuse.ReleaseRequest) error {
	return errno(h.file.Close())
}
