package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Operations in a normalised trace. Data-bearing operations carry an
// offset and a length; the rest name a path.
const (
	opOpen    = "open"    // successful open of a path
	opMiss    = "miss"    // lookup, open or exec that failed
	opRead    = "read"    // bytes returned by read, pread, readv, copy or sendfile
	opWrite   = "write"   // bytes accepted by write, pwrite, writev, copy or sendfile
	opMap     = "map"     // file range mapped into memory
	opExec    = "exec"    // file executed; length is unknown (-1), the whole file
	opStat    = "stat"    // stat, access or readlink by path
	opList    = "list"    // directory entries returned
	opLink    = "link"    // hard link: path is the existing name, aux the new one
	opRename  = "rename"  // path renamed to aux
	opUnlink  = "unlink"  // path removed
	opMkdir   = "mkdir"   // directory created
	opTrunc   = "trunc"   // file truncated to offset
	opSymlink = "symlink" // symbolic link created at path
)

// Event is one normalised file access.
type Event struct {
	Time int64  // nanoseconds since the first traced call
	Dur  int64  // nanoseconds spent in the call
	Tid  int    // thread that made it
	Op   string // one of the op constants
	Path string // absolute path
	Off  int64  // byte offset, -1 when unknown
	Len  int64  // byte count, -1 when the whole file
	Aux  string // second path for link and rename
}

// escape keeps a field on one tab-separated line.
func escape(s string) string {
	if !strings.ContainsAny(s, "%\t\n\r") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '%', '\t', '\n', '\r':
			fmt.Fprintf(&b, "%%%02x", c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func unescape(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// writeEvents emits the tab-separated form:
// workflow run t_ns dur_ns tid op path offset length aux
func writeEvents(w io.Writer, workflow, run string, events []Event) error {
	bw := bufio.NewWriter(w)
	for _, e := range events {
		_, err := fmt.Fprintf(bw, "%s\t%s\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%s\n",
			escape(workflow), escape(run), e.Time, e.Dur, e.Tid, e.Op, escape(e.Path), e.Off, e.Len, escape(e.Aux))
		if err != nil {
			return err
		}
	}
	return bw.Flush()
}

// readEvents parses what writeEvents wrote.
func readEvents(r io.Reader) (workflow, run string, events []Event, err error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for line := 1; sc.Scan(); line++ {
		f := strings.Split(sc.Text(), "\t")
		if len(f) != 10 {
			return "", "", nil, fmt.Errorf("line %d: %d fields, want 10", line, len(f))
		}
		var e Event
		var perr error
		num := func(s string) int64 {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil && perr == nil {
				perr = fmt.Errorf("line %d: %w", line, err)
			}
			return v
		}
		workflow, run = unescape(f[0]), unescape(f[1])
		e.Time, e.Dur, e.Tid = num(f[2]), num(f[3]), int(num(f[4]))
		e.Op, e.Path = f[5], unescape(f[6])
		e.Off, e.Len, e.Aux = num(f[7]), num(f[8]), unescape(f[9])
		if perr != nil {
			return "", "", nil, perr
		}
		events = append(events, e)
	}
	return workflow, run, events, sc.Err()
}
