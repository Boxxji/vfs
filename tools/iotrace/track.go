package main

import (
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
)

// desc is an open file description. Descriptors made by dup or inherited
// across fork share one, and with it the file position.
type desc struct {
	path   string
	pos    int64 // -1 when unknown
	append bool
}

type table struct{ fds map[int]*desc }

func (t *table) clone() *table {
	c := &table{fds: make(map[int]*desc, len(t.fds))}
	for k, v := range t.fds {
		c.fds[k] = v
	}
	return c
}

type proc struct {
	files *table
	cwd   string
}

type link struct {
	parent int
	share  bool // CLONE_FILES: parent and child use one descriptor table
}

// tracker turns decoded calls into events, keeping the descriptor
// tables strace does not print (file positions) in step with the
// process tree.
type tracker struct {
	cwd    string
	links  map[int]link
	procs  map[int]*proc
	sizes  map[string]int64
	events []Event
	t0     int64
}

// normalise reads the strace output twice: once for the process tree
// (a child can log before its parent's clone returns) and once for the
// accesses. cwd is the working directory of the traced command.
func normalise(open func() (io.Reader, error), cwd string) ([]Event, map[string]int64, error) {
	t := &tracker{cwd: cwd, links: map[int]link{}, procs: map[int]*proc{}, sizes: map[string]int64{}}
	r, err := open()
	if err != nil {
		return nil, nil, err
	}
	if err := scan(r, t.tree, nil); err != nil {
		return nil, nil, err
	}
	if c, ok := r.(io.Closer); ok {
		c.Close()
	}
	r, err = open()
	if err != nil {
		return nil, nil, err
	}
	err = scan(r, t.call, func(tid int, line string) {
		if strings.HasPrefix(line, "+++ ") {
			delete(t.procs, tid)
		}
	})
	if c, ok := r.(io.Closer); ok {
		c.Close()
	}
	if err != nil {
		return nil, nil, err
	}
	if len(t.events) == 0 {
		return nil, nil, errors.New("no file accesses in the trace")
	}
	return t.events, t.sizes, nil
}

func (t *tracker) tree(c call) {
	switch c.name {
	case "clone", "clone3", "fork", "vfork":
		if c.ret > 0 && !c.fail {
			t.links[int(c.ret)] = link{c.tid, strings.Contains(strings.Join(c.args, ","), "CLONE_FILES")}
		}
	}
}

func (t *tracker) proc(tid int) *proc {
	if p := t.procs[tid]; p != nil {
		return p
	}
	p := &proc{cwd: t.cwd}
	if l, ok := t.links[tid]; ok && l.parent != tid {
		parent := t.proc(l.parent)
		p.cwd = parent.cwd
		if l.share {
			p.files = parent.files
		} else {
			p.files = parent.files.clone()
		}
	} else {
		p.files = &table{fds: map[int]*desc{}}
	}
	t.procs[tid] = p
	return p
}

func (t *tracker) emit(c call, op, path string, off, n int64, aux string) {
	if !keep(path) {
		return
	}
	if t.t0 == 0 {
		t.t0 = c.time
	}
	t.events = append(t.events, Event{Time: c.time - t.t0, Dur: c.dur, Tid: c.tid, Op: op, Path: path, Off: off, Len: n, Aux: aux})
}

// keep drops descriptors that are not files: pipes, sockets, and the
// kernel's own trees.
func keep(p string) bool {
	if !strings.HasPrefix(p, "/") {
		return false
	}
	for _, skip := range [...]string{"/proc/", "/sys/", "/dev/"} {
		if strings.HasPrefix(p, skip) {
			return false
		}
	}
	return true
}

// where resolves a path argument against its directory descriptor
// argument. An empty path means the descriptor itself.
func (p *proc) where(dirarg, patharg string) string {
	fd, dir := fdpath(dirarg)
	if fd == -100 {
		if dir == "" {
			dir = p.cwd
		} else {
			p.cwd = dir
		}
	}
	s, ok := quoted(patharg)
	if !ok {
		return ""
	}
	switch {
	case s == "":
		return dir
	case strings.HasPrefix(s, "/"):
		return path.Clean(s)
	case dir == "":
		return ""
	}
	return path.Join(dir, s)
}

func (p *proc) cwdwhere(patharg string) string { return p.where("AT_FDCWD", patharg) }

// lookup finds the description of an fd argument, creating one from the
// annotation when the descriptor was opened before tracing began.
func (p *proc) lookup(arg string) *desc {
	fd, ap := fdpath(arg)
	if d := p.files.fds[fd]; d != nil {
		return d
	}
	if ap == "" {
		return nil
	}
	d := &desc{path: ap, pos: -1}
	p.files.fds[fd] = d
	return d
}

func num(s string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 0, 64)
	if err != nil {
		return -1
	}
	return v
}

// pointer reads an offset pointer argument: NULL, or [123].
func pointer(s string) (int64, bool) {
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return num(s[1 : len(s)-1]), true
	}
	return -1, false
}

func arg(c call, i int) string {
	if i < len(c.args) {
		return c.args[i]
	}
	return ""
}

func (t *tracker) call(c call) {
	p := t.proc(c.tid)
	switch c.name {
	case "openat", "open", "openat2", "creat":
		dirarg, patharg, flags := "AT_FDCWD", arg(c, 0), arg(c, 1)
		if c.name == "openat" || c.name == "openat2" {
			dirarg, patharg, flags = arg(c, 0), arg(c, 1), arg(c, 2)
		}
		if c.name == "creat" {
			flags = "O_CREAT|O_WRONLY|O_TRUNC"
		}
		abs := p.where(dirarg, patharg)
		if c.fail {
			t.emit(c, opMiss, abs, -1, -1, "")
			return
		}
		_, ap := fdpath(c.raw)
		if ap == "" {
			ap = abs
		}
		p.files.fds[int(c.ret)] = &desc{path: ap, pos: 0, append: strings.Contains(flags, "O_APPEND")}
		t.emit(c, opOpen, ap, 0, 0, "")
		if strings.Contains(flags, "O_TRUNC") {
			t.emit(c, opTrunc, ap, 0, 0, "")
		}
	case "read", "readv":
		t.transfer(p, c, opRead, arg(c, 0), -1, false)
	case "pread64", "preadv", "preadv2":
		off := num(arg(c, 3))
		t.transfer(p, c, opRead, arg(c, 0), off, off >= 0)
	case "write", "writev":
		t.transfer(p, c, opWrite, arg(c, 0), -1, false)
	case "pwrite64", "pwritev", "pwritev2":
		off := num(arg(c, 3))
		t.transfer(p, c, opWrite, arg(c, 0), off, off >= 0)
	case "lseek":
		if !c.fail {
			if d := p.lookup(arg(c, 0)); d != nil {
				d.pos = c.ret
			}
		}
	case "dup", "dup2", "dup3":
		if !c.fail {
			p.dup(arg(c, 0), int(c.ret), c.raw)
		}
	case "fcntl":
		if !c.fail && strings.HasPrefix(arg(c, 1), "F_DUPFD") {
			p.dup(arg(c, 0), int(c.ret), c.raw)
		}
	case "close":
		fd, _ := fdpath(arg(c, 0))
		delete(p.files.fds, fd)
	case "mmap":
		if c.fail || !strings.Contains(arg(c, 2), "PROT_READ") {
			return
		}
		if _, ap := fdpath(arg(c, 4)); ap != "" {
			op := opMap
			if strings.Contains(arg(c, 2), "PROT_WRITE") && strings.Contains(arg(c, 3), "MAP_SHARED") {
				op = opWrite // the program stores through it: the bytes are its own
			}
			t.emit(c, op, ap, num(arg(c, 5)), num(arg(c, 1)), "")
		}
	case "execve", "execveat":
		dirarg, patharg := "AT_FDCWD", arg(c, 0)
		if c.name == "execveat" {
			dirarg, patharg = arg(c, 0), arg(c, 1)
		}
		abs := p.where(dirarg, patharg)
		if c.fail {
			t.emit(c, opMiss, abs, -1, -1, "")
			return
		}
		t.emit(c, opExec, abs, 0, -1, "")
	case "getdents", "getdents64":
		if d := p.lookup(arg(c, 0)); d != nil && c.ret > 0 {
			t.emit(c, opList, d.path, -1, c.ret, "")
		}
	case "newfstatat", "fstatat64", "statx", "faccessat", "faccessat2", "readlinkat":
		t.lookupcall(p, c, p.where(arg(c, 0), arg(c, 1)))
	case "stat", "lstat", "stat64", "lstat64", "access", "readlink":
		t.lookupcall(p, c, p.cwdwhere(arg(c, 0)))
	case "fstat", "fstat64":
		if d := p.lookup(arg(c, 0)); d != nil && !c.fail {
			t.size(d.path, c)
		}
	case "rename":
		t.namespace(c, opRename, p.cwdwhere(arg(c, 0)), p.cwdwhere(arg(c, 1)))
	case "renameat", "renameat2":
		t.namespace(c, opRename, p.where(arg(c, 0), arg(c, 1)), p.where(arg(c, 2), arg(c, 3)))
	case "link":
		t.namespace(c, opLink, p.cwdwhere(arg(c, 0)), p.cwdwhere(arg(c, 1)))
	case "linkat":
		t.namespace(c, opLink, p.where(arg(c, 0), arg(c, 1)), p.where(arg(c, 2), arg(c, 3)))
	case "unlink", "rmdir":
		t.namespace(c, opUnlink, p.cwdwhere(arg(c, 0)), "")
	case "unlinkat":
		t.namespace(c, opUnlink, p.where(arg(c, 0), arg(c, 1)), "")
	case "mkdir":
		t.namespace(c, opMkdir, p.cwdwhere(arg(c, 0)), "")
	case "mkdirat":
		t.namespace(c, opMkdir, p.where(arg(c, 0), arg(c, 1)), "")
	case "symlink":
		target, _ := quoted(arg(c, 0))
		t.namespace(c, opSymlink, p.cwdwhere(arg(c, 1)), target)
	case "symlinkat":
		target, _ := quoted(arg(c, 0))
		t.namespace(c, opSymlink, p.where(arg(c, 1), arg(c, 2)), target)
	case "truncate":
		if !c.fail {
			t.emit(c, opTrunc, p.cwdwhere(arg(c, 0)), num(arg(c, 1)), 0, "")
		}
	case "ftruncate":
		if d := p.lookup(arg(c, 0)); d != nil && !c.fail {
			t.emit(c, opTrunc, d.path, num(arg(c, 1)), 0, "")
		}
	case "copy_file_range":
		t.copy(p, c, arg(c, 0), arg(c, 1), arg(c, 2), arg(c, 3))
	case "sendfile":
		t.copy(p, c, arg(c, 1), arg(c, 2), arg(c, 0), "NULL")
	}
}

func (p *proc) dup(oldarg string, newfd int, ret string) {
	oldfd, ap := fdpath(oldarg)
	d := p.files.fds[oldfd]
	if d == nil {
		if ap == "" {
			_, ap = fdpath(ret)
		}
		if ap == "" {
			return
		}
		d = &desc{path: ap, pos: -1}
	}
	p.files.fds[newfd] = d
}

// transfer records a data call. Calls with an explicit offset leave the
// file position alone; the others read and advance it.
func (t *tracker) transfer(p *proc, c call, op, fdarg string, off int64, explicit bool) {
	if c.fail || c.ret <= 0 {
		return
	}
	d := p.lookup(fdarg)
	if d == nil {
		return
	}
	if explicit {
		t.emit(c, op, d.path, off, c.ret, "")
		return
	}
	at := d.pos
	switch {
	case d.append && op == opWrite:
		at, d.pos = -1, -1
	case d.pos >= 0:
		d.pos += c.ret
	}
	t.emit(c, op, d.path, at, c.ret, "")
}

// copy handles copy_file_range and sendfile: a read of the source and a
// write of the destination, at the given offsets or at the positions.
func (t *tracker) copy(p *proc, c call, inarg, inoff, outarg, outoff string) {
	if c.fail || c.ret <= 0 {
		return
	}
	in, out := p.lookup(inarg), p.lookup(outarg)
	if in != nil {
		off, ok := pointer(inoff)
		if !ok {
			off = in.pos
			if in.pos >= 0 {
				in.pos += c.ret
			}
		}
		t.emit(c, opRead, in.path, off, c.ret, "")
	}
	if out != nil {
		off, ok := pointer(outoff)
		if !ok {
			off = out.pos
			if out.pos >= 0 {
				out.pos += c.ret
			}
		}
		t.emit(c, opWrite, out.path, off, c.ret, "")
	}
}

func (t *tracker) lookupcall(p *proc, c call, abs string) {
	if abs == "" {
		return
	}
	if c.fail {
		t.emit(c, opMiss, abs, -1, -1, "")
		return
	}
	t.size(abs, c)
	switch c.name {
	case "faccessat", "faccessat2", "access", "readlink", "readlinkat":
		t.emit(c, opStat, abs, -1, -1, "")
	default:
		if !strings.Contains(strings.Join(c.args, ","), "AT_EMPTY_PATH") {
			t.emit(c, opStat, abs, -1, -1, "")
		}
	}
}

func (t *tracker) namespace(c call, op, path, aux string) {
	if c.fail || path == "" {
		return
	}
	t.emit(c, op, path, -1, -1, aux)
}

// size records st_size or stx_size of a regular file from a stat result.
func (t *tracker) size(path string, c call) {
	all := strings.Join(c.args, ",")
	if !strings.Contains(all, "S_IFREG") {
		return
	}
	for _, key := range [...]string{"st_size=", "stx_size="} {
		if i := strings.Index(all, key); i >= 0 {
			rest := all[i+len(key):]
			end := 0
			for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
				end++
			}
			if v, err := strconv.ParseInt(rest[:end], 10, 64); err == nil && keep(path) {
				t.sizes[path] = v
			}
			return
		}
	}
}
