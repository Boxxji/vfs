package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// counts is what `strace -c -f` reports for one command and every process it
// started: the calls that decide how git behaves on a disk with a price per
// call (a volume that publishes to object storage), not a full profile.
type counts struct {
	fsync, fdatasync, syncrange int
	rename, openat, unlink      int
	stat, getdents, write       int
	total                       int
}

// family maps a system call name to the counter it adds to. Variants of one
// call (open and openat, stat and statx) are one counter: they cost the same
// to a filesystem.
func (c *counts) family(name string) *int {
	switch name {
	case "fsync":
		return &c.fsync
	case "fdatasync":
		return &c.fdatasync
	case "sync_file_range":
		return &c.syncrange
	case "rename", "renameat", "renameat2":
		return &c.rename
	case "open", "openat", "openat2":
		return &c.openat
	case "unlink", "unlinkat":
		return &c.unlink
	case "stat", "lstat", "fstat", "newfstatat", "statx", "stat64", "lstat64", "fstat64", "fstatat64":
		return &c.stat
	case "getdents", "getdents64":
		return &c.getdents
	case "write", "pwrite64", "writev", "pwritev", "pwritev2":
		return &c.write
	}
	return nil
}

// tally reads the table strace -c writes. A row is
//
//	% time  seconds  usecs/call  calls  [errors]  syscall
//
// with the errors column blank when there were none, so the call count is the
// fourth field and the name the last.
func tally(table string) (counts, error) {
	var c counts
	seen := false
	sc := bufio.NewScanner(strings.NewReader(table))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 5 {
			continue
		}
		if _, err := strconv.ParseFloat(f[0], 64); err != nil {
			continue
		}
		n, err := strconv.Atoi(f[3])
		if err != nil {
			continue
		}
		name := f[len(f)-1]
		if name == "total" {
			c.total = n
			seen = true
			continue
		}
		if p := c.family(name); p != nil {
			*p += n
		}
	}
	if !seen {
		return c, fmt.Errorf("no total row in strace summary")
	}
	return c, nil
}

// load is the one-minute load average, recorded beside every timing because
// the machine is shared.
func load() float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(f[0], 64)
	return v
}

// fstype names the filesystem a directory lives on, from the longest mount
// point that prefixes it, so a row says what it measured (ext4, tmpfs,
// fuse.hanzovfs) without the operator typing it.
func fstype(dir string) string {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "unknown"
	}
	defer f.Close()
	best, kind := -1, "unknown"
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		left, right, ok := strings.Cut(sc.Text(), " - ")
		if !ok {
			continue
		}
		lf, rf := strings.Fields(left), strings.Fields(right)
		if len(lf) < 5 || len(rf) < 1 {
			continue
		}
		mount := unescape(lf[4])
		if (dir == mount || strings.HasPrefix(dir, strings.TrimSuffix(mount, "/")+"/")) && len(mount) > best {
			best, kind = len(mount), rf[0]
		}
	}
	return kind
}

// unescape reverses the octal escapes mountinfo writes for spaces and tabs.
func unescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
