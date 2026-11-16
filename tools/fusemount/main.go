//go:build linux

// Command fusemount stands in for fusermount3 where there is no setuid helper to
// mount with. A sandboxed CI runner ignores the setuid bit and gives its user no
// capability, but lets that user make a user namespace in which it holds every
// capability, and mount a file system there. Run inside one (`unshare -Urm`), with
// this program on PATH under the name fusermount3, the FUSE mount tests find the
// helper they ask for.
//
// It speaks the protocol of the real one, which is all the FUSE libraries use:
//
//	fusermount3 -o OPTIONS -- DIR     open /dev/fuse, mount it on DIR, and send the
//	                                  descriptor back over the socket in _FUSE_COMMFD
//	fusermount3 -u [-z] DIR           unmount DIR (-z: lazily)
//
// It is a tool for the tests' environment and is not used by the Volume or the mount.
// Linux only: it uses mount(2).
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "fusermount3: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	var options, dir string
	var unmount, lazy bool
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "-u":
			unmount = true
		case a == "-z":
			lazy = true
		case a == "-q" || a == "-V":
		case a == "-o" && i+1 < len(args):
			i++
			options = args[i]
		case a == "--":
			if i+1 < len(args) {
				dir = args[i+1]
			}
			i = len(args)
		case !strings.HasPrefix(a, "-"):
			dir = a
		}
	}
	if dir == "" {
		return fmt.Errorf("no mountpoint")
	}
	if unmount {
		flags := 0
		if lazy {
			flags = syscall.MNT_DETACH
		}
		// A busy mount is an error, as it is from the real helper: only -z detaches.
		return syscall.Unmount(dir, flags)
	}
	return mount(dir, options)
}

func mount(dir, options string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("failed to access mountpoint %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("mountpoint %s is not a directory", dir)
	}
	commfd, err := strconv.Atoi(os.Getenv("_FUSE_COMMFD"))
	if err != nil {
		return fmt.Errorf("_FUSE_COMMFD is not set: this is not called by a FUSE library")
	}
	f, err := os.OpenFile("/dev/fuse", os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	source, fstype := "fuse", "fuse"
	data := fmt.Sprintf("fd=%d,rootmode=%o,user_id=%d,group_id=%d", f.Fd(), syscall.S_IFDIR, os.Getuid(), os.Getgid())
	for _, o := range strings.Split(options, ",") {
		k, _, _ := strings.Cut(o, "=")
		switch k {
		case "fsname":
			source = strings.TrimPrefix(o, "fsname=")
		case "subtype":
			fstype = "fuse." + strings.TrimPrefix(o, "subtype=")
		case "allow_other", "default_permissions", "max_read", "blksize":
			data += "," + o
		}
	}
	if err := syscall.Mount(source, dir, fstype, syscall.MS_NOSUID|syscall.MS_NODEV, data); err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	// Hand the descriptor to the caller, as the real helper does.
	rights := syscall.UnixRights(int(f.Fd()))
	if err := syscall.Sendmsg(commfd, []byte{0}, rights, nil, 0); err != nil {
		_ = syscall.Unmount(dir, syscall.MNT_DETACH)
		return fmt.Errorf("send the descriptor: %w", err)
	}
	return nil
}
