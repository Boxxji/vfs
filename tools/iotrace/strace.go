package main

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// call is one completed system call decoded from strace output
// (`strace -f -ttt -T -y`).
type call struct {
	tid  int
	time int64 // entry time, ns since the epoch
	name string
	args []string // top-level arguments, fd annotations kept
	ret  int64    // return value, or -1 on failure
	fail bool
	raw  string // result text after ") = ", used for fd annotations
	dur  int64  // ns
}

const unfinished = " <unfinished ...>"

// scan reads strace lines and calls fn for every completed call. Calls
// that strace split into an unfinished and a resumed line are joined.
// Lines that are not calls (signals, exits) go to other with the tid.
func scan(r io.Reader, fn func(call), other func(tid int, line string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	part := map[int]struct {
		time int64
		text string
	}{}
	for sc.Scan() {
		line := sc.Text()
		tid, rest, ok := cut(line, ' ')
		if !ok {
			continue
		}
		id, err := strconv.Atoi(tid)
		if err != nil {
			continue
		}
		ts, body, ok := cut(rest, ' ')
		if !ok {
			continue
		}
		t, ok := parseTime(ts)
		if !ok {
			continue
		}
		switch {
		case strings.HasPrefix(body, "+++ ") || strings.HasPrefix(body, "--- "):
			if other != nil {
				other(id, body)
			}
			continue
		case strings.HasPrefix(body, "<... "):
			p, ok := part[id]
			if !ok {
				continue
			}
			delete(part, id)
			i := strings.Index(body, " resumed>")
			if i < 0 {
				continue
			}
			body = p.text + body[i+len(" resumed>"):]
			t = p.time
		case strings.HasSuffix(body, unfinished):
			part[id] = struct {
				time int64
				text string
			}{t, strings.TrimSuffix(body, unfinished)}
			continue
		}
		if c, ok := decode(id, t, body); ok {
			fn(c)
		}
	}
	return sc.Err()
}

func cut(s string, sep byte) (head, tail string, ok bool) {
	i := strings.IndexByte(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+1:], true
}

// parseTime reads seconds.microseconds into nanoseconds.
func parseTime(s string) (int64, bool) {
	sec, frac, ok := cut(s, '.')
	if !ok {
		return 0, false
	}
	a, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return 0, false
	}
	for len(frac) < 9 {
		frac += "0"
	}
	b, err := strconv.ParseInt(frac[:9], 10, 64)
	if err != nil {
		return 0, false
	}
	return a*1e9 + b, true
}

// decode splits `name(args) = ret <dur>`.
func decode(tid int, t int64, body string) (call, bool) {
	open := strings.IndexByte(body, '(')
	if open <= 0 {
		return call{}, false
	}
	c := call{tid: tid, time: t, name: body[:open]}
	end := strings.LastIndex(body, ") = ")
	if end < open {
		return call{}, false // never returned (exit_group) or malformed
	}
	c.args = splitArgs(body[open+1 : end])
	res := body[end+len(") = "):]
	if i := strings.LastIndex(res, " <"); i >= 0 && strings.HasSuffix(res, ">") {
		if d, ok := parseDur(res[i+2 : len(res)-1]); ok {
			c.dur = d
		}
		res = res[:i]
	}
	c.raw = res
	c.ret, c.fail = parseRet(res)
	return c, true
}

func parseDur(s string) (int64, bool) {
	sec, frac, ok := cut(s, '.')
	if !ok {
		return 0, false
	}
	a, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return 0, false
	}
	for len(frac) < 9 {
		frac += "0"
	}
	b, err := strconv.ParseInt(frac[:9], 10, 64)
	if err != nil {
		return 0, false
	}
	return a*1e9 + b, true
}

// parseRet reads the leading integer of a result such as
// `3</x/y>`, `-1 ENOENT (...)`, `0x7f00` or `4096`.
func parseRet(res string) (int64, bool) {
	if strings.HasPrefix(res, "0x") {
		end := 2
		for end < len(res) && strings.IndexByte("0123456789abcdef", res[end]) >= 0 {
			end++
		}
		v, err := strconv.ParseUint(res[2:end], 16, 64)
		if err != nil {
			return -1, true
		}
		return int64(v), false
	}
	end := 0
	if end < len(res) && res[end] == '-' {
		end++
	}
	for end < len(res) && res[end] >= '0' && res[end] <= '9' {
		end++
	}
	v, err := strconv.ParseInt(res[:end], 10, 64)
	if err != nil {
		return -1, true
	}
	return v, v < 0
}

// splitArgs splits on top-level commas, respecting quoted strings,
// brackets, braces, parentheses and fd annotations (`3</a,b>`).
func splitArgs(s string) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '"':
			for i++; i < len(s) && s[i] != '"'; i++ {
				if s[i] == '\\' {
					i++
				}
			}
		case '<':
			if i > 0 && (s[i-1] >= '0' && s[i-1] <= '9' || s[i-1] == 'D') {
				// fd annotation: runs to the closing '>' that ends the path.
				j := i + 1
				for j < len(s) && !(s[j] == '>' && (j+1 == len(s) || s[j+1] == ',' || s[j+1] == ')' || s[j+1] == ']' || s[j+1] == '}')) {
					j++
				}
				i = j
			}
		case '{', '[', '(':
			depth++
		case '}', ']', ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if tail := strings.TrimSpace(s[start:]); tail != "" || len(out) > 0 {
		out = append(out, tail)
	}
	return out
}

// quoted decodes an strace string literal ("a\nb\303\251"), without
// the trailing `...` that marks truncation.
func quoted(s string) (string, bool) {
	s = strings.TrimSuffix(s, "...")
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", false
	}
	return unquote(s[1 : len(s)-1]), true
}

func unquote(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch c := s[i]; {
		case c >= '0' && c <= '7':
			v, n := 0, 0
			for n < 3 && i < len(s) && s[i] >= '0' && s[i] <= '7' {
				v = v*8 + int(s[i]-'0')
				i++
				n++
			}
			i--
			b.WriteByte(byte(v))
		case c == 'n':
			b.WriteByte('\n')
		case c == 't':
			b.WriteByte('\t')
		case c == 'r':
			b.WriteByte('\r')
		case c == 'a':
			b.WriteByte('\a')
		case c == 'b':
			b.WriteByte('\b')
		case c == 'f':
			b.WriteByte('\f')
		case c == 'v':
			b.WriteByte('\v')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// fdpath splits `3</a/b>` into the descriptor and the annotation. The
// path is empty when the descriptor carries none. AT_FDCWD gives -100.
func fdpath(s string) (fd int, path string) {
	i := strings.IndexByte(s, '<')
	num := s
	if i >= 0 {
		num = s[:i]
		path = s[i+1:]
		path = strings.TrimSuffix(path, ">")
		path = strings.TrimSuffix(path, " (deleted)")
		path = unquote(path)
	}
	if num == "AT_FDCWD" {
		return -100, path
	}
	v, err := strconv.Atoi(num)
	if err != nil {
		return -1, path
	}
	return v, path
}
