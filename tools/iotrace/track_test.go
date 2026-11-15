package main

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

func fixture(t *testing.T, cwd string, lines ...string) ([]Event, map[string]int64) {
	t.Helper()
	text := strings.Join(lines, "\n") + "\n"
	ev, sizes, err := normalise(func() (io.Reader, error) { return strings.NewReader(text), nil }, cwd)
	if err != nil {
		t.Fatal(err)
	}
	return ev, sizes
}

// reads picks the data events of one op as "path@off+len".
func summary(ev []Event, op string) []string {
	var out []string
	for _, e := range ev {
		if e.Op == op {
			out = append(out, e.Path+"@"+itoa(e.Off)+"+"+itoa(e.Len))
		}
	}
	return out
}

func itoa(v int64) string {
	if v < 0 {
		return "-" + itoa(-v)
	}
	if v == 0 {
		return "0"
	}
	var b []byte
	for ; v > 0; v /= 10 {
		b = append([]byte{byte('0' + v%10)}, b...)
	}
	return string(b)
}

func expect(t *testing.T, name string, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s\n got %q\nwant %q", name, got, want)
	}
}

func TestPositionFollowsReadsAndSeeks(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "f", O_RDONLY) = 3</w/f> <0.000010>`,
		`10 100.000002 read(3</w/f>, ""..., 4096) = 4096 <0.000010>`,
		`10 100.000003 read(3</w/f>, ""..., 4096) = 100 <0.000010>`,
		`10 100.000004 lseek(3</w/f>, 1000000, SEEK_SET) = 1000000 <0.000010>`,
		`10 100.000005 read(3</w/f>, ""..., 50) = 50 <0.000010>`,
		`10 100.000006 pread64(3</w/f>, ""..., 10, 2000000) = 10 <0.000010>`,
		`10 100.000007 read(3</w/f>, ""..., 8) = 8 <0.000010>`,
		`10 100.000008 read(3</w/f>, ""..., 8) = 0 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{
		"/w/f@0+4096", "/w/f@4096+100", "/w/f@1000000+50", "/w/f@2000000+10", "/w/f@1000050+8"})
}

func TestVectoredAndPositional(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "f", O_RDONLY) = 3</w/f> <0.000010>`,
		`10 100.000002 readv(3</w/f>, [{iov_base=""..., iov_len=10}, {iov_base=""..., iov_len=20}], 2) = 30 <0.000010>`,
		`10 100.000003 preadv(3</w/f>, [{iov_base=""..., iov_len=10}], 1, 500) = 10 <0.000010>`,
		`10 100.000004 preadv2(3</w/f>, [{iov_base=""..., iov_len=10}], 1, -1, 0) = 7 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{"/w/f@0+30", "/w/f@500+10", "/w/f@30+7"})
}

func TestDupSharesPosition(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "f", O_RDONLY) = 3</w/f> <0.000010>`,
		`10 100.000002 fcntl(3</w/f>, F_DUPFD_CLOEXEC, 0) = 4</w/f> <0.000010>`,
		`10 100.000003 read(3</w/f>, ""..., 100) = 100 <0.000010>`,
		`10 100.000004 read(4</w/f>, ""..., 100) = 100 <0.000010>`,
		`10 100.000005 close(3</w/f>) = 0 <0.000010>`,
		`10 100.000006 read(4</w/f>, ""..., 100) = 100 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{"/w/f@0+100", "/w/f@100+100", "/w/f@200+100"})
}

func TestThreadsShareTableAndForksShareDescriptions(t *testing.T) {
	// Thread 11 logs before thread 10's clone returns; both are
	// CLONE_FILES, so 11 reads at the position 10 left.
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "f", O_RDONLY) = 3</w/f> <0.000010>`,
		`10 100.000002 read(3</w/f>, ""..., 100) = 100 <0.000010>`,
		`11 100.000004 read(3</w/f>, ""..., 100) = 100 <0.000010>`,
		`10 100.000003 clone3({flags=CLONE_VM|CLONE_FS|CLONE_FILES|CLONE_THREAD, exit_signal=0}, 88) = 11 <0.000010>`,
		`10 100.000005 clone(child_stack=NULL, flags=CLONE_CHILD_CLEARTID|SIGCHLD, child_tidptr=0x1) = 12 <0.000010>`,
		`12 100.000006 read(3</w/f>, ""..., 100) = 100 <0.000010>`,
		`10 100.000007 read(3</w/f>, ""..., 100) = 100 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{"/w/f@0+100", "/w/f@100+100", "/w/f@200+100", "/w/f@300+100"})
}

func TestForkedTableIsACopy(t *testing.T) {
	// The child closes its descriptor and opens another at the same
	// number; the parent's table is unchanged.
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "a", O_RDONLY) = 3</w/a> <0.000010>`,
		`10 100.000002 clone(child_stack=NULL, flags=SIGCHLD, child_tidptr=0x1) = 12 <0.000010>`,
		`12 100.000003 close(3</w/a>) = 0 <0.000010>`,
		`12 100.000004 openat(AT_FDCWD</w>, "b", O_RDONLY) = 3</w/b> <0.000010>`,
		`12 100.000005 read(3</w/b>, ""..., 5) = 5 <0.000010>`,
		`10 100.000006 read(3</w/a>, ""..., 7) = 7 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{"/w/b@0+5", "/w/a@0+7"})
}

func TestUnfinishedAndResumedJoin(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "f", O_RDONLY) = 3</w/f> <0.000010>`,
		`10 100.000002 read(3</w/f>,  <unfinished ...>`,
		`11 100.000003 openat(AT_FDCWD</w>, "g", O_RDONLY) = 4</w/g> <0.000010>`,
		`10 100.000009 <... read resumed>""..., 4096) = 4096 <0.007000>`,
	)
	if len(ev) != 3 {
		t.Fatalf("events %v", ev)
	}
	r := ev[2]
	if r.Op != opRead || r.Len != 4096 || r.Dur != 7000000 {
		t.Errorf("read %+v", r)
	}
	if r.Time != 1000 { // entry time of the unfinished half, 1 us after the first call
		t.Errorf("time %d, want entry time 1000", r.Time)
	}
}

func TestRelativePathsAndDirectoryDescriptors(t *testing.T) {
	ev, sizes := fixture(t, "/start",
		`10 100.000001 newfstatat(AT_FDCWD</w>, "a/b", {st_mode=S_IFREG|0644, st_size=123, ...}, 0) = 0 <0.000010>`,
		`10 100.000002 openat(5</w/d>, "x", O_RDONLY) = 6</w/d/x> <0.000010>`,
		`10 100.000003 statx(AT_FDCWD</w>, "../etc/y", AT_STATX_SYNC_AS_STAT, STATX_BASIC_STATS, {stx_mask=0, stx_mode=S_IFREG|0644, stx_size=77, ...}) = 0 <0.000010>`,
		`10 100.000004 newfstatat(AT_FDCWD</w>, "nope", 0x1, 0) = -1 ENOENT (No such file or directory) <0.000010>`,
		`10 100.000005 newfstatat(6</w/d/x>, "", {st_mode=S_IFREG|0644, st_size=9, ...}, AT_EMPTY_PATH) = 0 <0.000010>`,
		`10 100.000006 execve("tool", [...], 0x1 /* 3 vars */) = 0 <0.000010>`,
	)
	expect(t, "stat", summary(ev, opStat), []string{"/w/a/b@-1+-1", "/etc/y@-1+-1"})
	expect(t, "miss", summary(ev, opMiss), []string{"/w/nope@-1+-1"})
	expect(t, "exec", summary(ev, opExec), []string{"/w/tool@0+-1"})
	want := map[string]int64{"/w/a/b": 123, "/etc/y": 77, "/w/d/x": 9}
	if !reflect.DeepEqual(sizes, want) {
		t.Errorf("sizes %v, want %v", sizes, want)
	}
}

func TestMapListAndWrite(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "lib.so", O_RDONLY|O_CLOEXEC) = 3</w/lib.so> <0.000010>`,
		`10 100.000002 mmap(NULL, 8192, PROT_READ|PROT_EXEC, MAP_PRIVATE|MAP_DENYWRITE, 3</w/lib.so>, 4096) = 0xffff0000 <0.000010>`,
		`10 100.000003 mmap(NULL, 8192, PROT_NONE, MAP_PRIVATE, 3</w/lib.so>, 0) = 0xffff1000 <0.000010>`,
		`10 100.000004 mmap(NULL, 8192, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0) = 0xffff2000 <0.000010>`,
		`10 100.000005 openat(AT_FDCWD</w>, "out", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 4</w/out> <0.000010>`,
		`10 100.000006 write(4</w/out>, ""..., 10) = 10 <0.000010>`,
		`10 100.000007 write(4</w/out>, ""..., 5) = 5 <0.000010>`,
		`10 100.000008 getdents64(5</w/d>, 0x1 /* 3 entries */, 32768) = 96 <0.000010>`,
		`10 100.000009 getdents64(5</w/d>, 0x1 /* 0 entries */, 32768) = 0 <0.000010>`,
		`10 100.000010 openat(AT_FDCWD</w>, "log", O_WRONLY|O_APPEND) = 6</w/log> <0.000010>`,
		`10 100.000011 write(6</w/log>, ""..., 5) = 5 <0.000010>`,
	)
	expect(t, "map", summary(ev, opMap), []string{"/w/lib.so@4096+8192"})
	expect(t, "write", summary(ev, opWrite), []string{"/w/out@0+10", "/w/out@10+5", "/w/log@-1+5"})
	expect(t, "list", summary(ev, opList), []string{"/w/d@-1+96"})
	expect(t, "trunc", summary(ev, opTrunc), []string{"/w/out@0+0"})
}

func TestSharedWritableMappingIsAWrite(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "counters", O_RDWR|O_CREAT, 0666) = 3</w/counters> <0.000010>`,
		`10 100.000002 mmap(NULL, 16384, PROT_READ|PROT_WRITE, MAP_SHARED, 3</w/counters>, 0) = 0xffff0000 <0.000010>`,
		`10 100.000003 mmap(NULL, 4096, PROT_READ|PROT_WRITE, MAP_PRIVATE, 3</w/counters>, 0) = 0xffff1000 <0.000010>`,
		`10 100.000004 mmap(NULL, 4096, PROT_READ, MAP_SHARED, 3</w/counters>, 0) = 0xffff2000 <0.000010>`,
	)
	expect(t, "write", summary(ev, opWrite), []string{"/w/counters@0+16384"})
	expect(t, "map", summary(ev, opMap), []string{"/w/counters@0+4096", "/w/counters@0+4096"})
}

func TestCopyFileRangeAndSendfile(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "src", O_RDONLY) = 3</w/src> <0.000010>`,
		`10 100.000002 openat(AT_FDCWD</w>, "dst", O_WRONLY|O_CREAT) = 4</w/dst> <0.000010>`,
		`10 100.000003 copy_file_range(3</w/src>, NULL, 4</w/dst>, NULL, 1000, 0) = 600 <0.000010>`,
		`10 100.000004 copy_file_range(3</w/src>, NULL, 4</w/dst>, NULL, 1000, 0) = 400 <0.000010>`,
		`10 100.000005 sendfile(4</w/dst>, 3</w/src>, [5000], 100) = 100 <0.000010>`,
	)
	expect(t, "read", summary(ev, opRead), []string{"/w/src@0+600", "/w/src@600+400", "/w/src@5000+100"})
	expect(t, "write", summary(ev, opWrite), []string{"/w/dst@0+600", "/w/dst@600+400", "/w/dst@1000+100"})
}

func TestNamespaceOperations(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 renameat2(AT_FDCWD</w>, "a.lock", AT_FDCWD</w>, "a", RENAME_NOREPLACE) = 0 <0.000010>`,
		`10 100.000002 linkat(AT_FDCWD</s>, "x", AT_FDCWD</w>, "y", 0) = 0 <0.000010>`,
		`10 100.000003 unlinkat(AT_FDCWD</w>, "gone", 0) = 0 <0.000010>`,
		`10 100.000004 unlinkat(AT_FDCWD</w>, "kept", 0) = -1 ENOENT (No such file or directory) <0.000010>`,
		`10 100.000005 mkdirat(AT_FDCWD</w>, "d", 0777) = 0 <0.000010>`,
		`10 100.000006 symlinkat("target", AT_FDCWD</w>, "ln") = 0 <0.000010>`,
	)
	if got := ev[0]; got.Op != opRename || got.Path != "/w/a.lock" || got.Aux != "/w/a" {
		t.Errorf("rename %+v", got)
	}
	if got := ev[1]; got.Op != opLink || got.Path != "/s/x" || got.Aux != "/w/y" {
		t.Errorf("link %+v", got)
	}
	var ops []string
	for _, e := range ev[2:] {
		ops = append(ops, e.Op+" "+e.Path)
	}
	expect(t, "rest", ops, []string{"unlink /w/gone", "mkdir /w/d", "symlink /w/ln"})
}

func TestNonFilesAreDropped(t *testing.T) {
	_, _, err := normalise(func() (io.Reader, error) {
		return strings.NewReader(strings.Join([]string{
			`10 100.000001 read(3<pipe:[123]>, ""..., 100) = 100 <0.000010>`,
			`10 100.000002 read(4<socket:[9]>, ""..., 100) = 100 <0.000010>`,
			`10 100.000003 openat(AT_FDCWD</w>, "/proc/self/maps", O_RDONLY) = 5</proc/12/maps> <0.000010>`,
			`10 100.000004 read(5</proc/12/maps>, ""..., 100) = 100 <0.000010>`,
		}, "\n")), nil
	}, "/w")
	if err == nil {
		t.Fatal("pipes, sockets and /proc must not produce events")
	}
}

func TestExitedThreadIDIsReused(t *testing.T) {
	// A new process with the same id inherits nothing: its first read of a
	// descriptor it did not open has no known position.
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "a", O_RDONLY) = 3</w/a> <0.000010>`,
		`10 100.000002 read(3</w/a>, ""..., 100) = 100 <0.000010>`,
		`10 100.000003 +++ exited with 0 +++`,
		`10 100.000004 read(3</w/a>, ""..., 50) = 50 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{"/w/a@0+100", "/w/a@-1+50"})
}

func TestClosedDescriptorLosesItsPosition(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 openat(AT_FDCWD</w>, "a", O_RDONLY) = 3</w/a> <0.000010>`,
		`10 100.000002 read(3</w/a>, ""..., 100) = 100 <0.000010>`,
		`10 100.000003 close(3</w/a>) = 0 <0.000010>`,
		`10 100.000004 read(3</w/a>, ""..., 50) = 50 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{"/w/a@0+100", "/w/a@-1+50"})
}

func TestAbsolutePathsIgnoreTheDirectory(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 newfstatat(AT_FDCWD</w>, "/abs/../abs/x", {st_mode=S_IFREG|0644, st_size=5, ...}, 0) = 0 <0.000010>`,
	)
	expect(t, "stat", summary(ev, opStat), []string{"/abs/x@-1+-1"})
}

func TestOnlyRegularFilesHaveSizes(t *testing.T) {
	_, sizes := fixture(t, "/w",
		`10 100.000001 newfstatat(AT_FDCWD</w>, "d", {st_mode=S_IFDIR|0755, st_size=4096, ...}, 0) = 0 <0.000010>`,
		`10 100.000002 newfstatat(AT_FDCWD</w>, "f", {st_mode=S_IFREG|0644, st_size=7, ...}, 0) = 0 <0.000010>`,
	)
	if !reflect.DeepEqual(sizes, map[string]int64{"/w/f": 7}) {
		t.Errorf("%v", sizes)
	}
}

func TestChildInheritsTheWorkingDirectory(t *testing.T) {
	ev, _ := fixture(t, "/start",
		`10 100.000001 newfstatat(AT_FDCWD</w2>, "x", {st_mode=S_IFREG|0644, st_size=1, ...}, 0) = 0 <0.000010>`,
		`10 100.000002 clone(child_stack=NULL, flags=SIGCHLD, child_tidptr=0x1) = 12 <0.000010>`,
		`12 100.000003 execve("tool", [...], 0x1 /* 3 vars */) = 0 <0.000010>`,
	)
	expect(t, "exec", summary(ev, opExec), []string{"/w2/tool@0+-1"})
}

func TestResultIsSplitFromTheRight(t *testing.T) {
	ev, sizes := fixture(t, "/w",
		`10 100.000001 newfstatat(AT_FDCWD</w>, "a) = b", {st_mode=S_IFREG|0644, st_size=5, ...}, 0) = 0 <0.000010>`,
	)
	expect(t, "stat", summary(ev, opStat), []string{"/w/a) = b@-1+-1"})
	if sizes["/w/a) = b"] != 5 {
		t.Errorf("%v", sizes)
	}
}

func TestTimesAndDurations(t *testing.T) {
	if v, ok := parseTime("1794770336.962357"); !ok || v != 1794770336962357000 {
		t.Errorf("time %d %v", v, ok)
	}
	if v, ok := parseDur("0.000142"); !ok || v != 142000 {
		t.Errorf("duration %d %v", v, ok)
	}
}

func TestDeletedSuffixIsDropped(t *testing.T) {
	if fd, p := fdpath("3</w/f (deleted)>"); fd != 3 || p != "/w/f" {
		t.Errorf("%d %q", fd, p)
	}
	if fd, p := fdpath("AT_FDCWD</w>"); fd != -100 || p != "/w" {
		t.Errorf("%d %q", fd, p)
	}
}

func TestInheritedDescriptorGetsUnknownOffset(t *testing.T) {
	ev, _ := fixture(t, "/w",
		`10 100.000001 read(3</w/in>, ""..., 100) = 100 <0.000010>`,
		`10 100.000002 pread64(3</w/in>, ""..., 100, 4096) = 100 <0.000010>`,
	)
	expect(t, "reads", summary(ev, opRead), []string{"/w/in@-1+100", "/w/in@4096+100"})
}

func TestSplitArgsKeepsAnnotationsAndStrings(t *testing.T) {
	got := splitArgs(`AT_FDCWD</a,b/c>, "x,y\"z", {a=1, b=[2, 3]}, O_RDONLY|O_CLOEXEC`)
	want := []string{`AT_FDCWD</a,b/c>`, `"x,y\"z"`, `{a=1, b=[2, 3]}`, `O_RDONLY|O_CLOEXEC`}
	expect(t, "args", got, want)
}

func TestUnquote(t *testing.T) {
	cases := map[string]string{
		`a\nb`:        "a\nb",
		`caf\303\251`: "café",
		`q\"q\\`:      `q"q\`,
		`plain`:       "plain",
		`\0`:          "\x00",
	}
	for in, want := range cases {
		if got := unquote(in); got != want {
			t.Errorf("unquote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseRet(t *testing.T) {
	cases := []struct {
		in   string
		ret  int64
		fail bool
	}{
		{"4096", 4096, false},
		{"0", 0, false},
		{"3</a/b>", 3, false},
		{"-1 ENOENT (No such file or directory)", -1, true},
		{"0xffff0000", 0xffff0000, false},
		{"? ERESTARTSYS (To be restarted)", -1, true},
	}
	for _, c := range cases {
		if ret, fail := parseRet(c.in); ret != c.ret || fail != c.fail {
			t.Errorf("parseRet(%q) = %d,%v want %d,%v", c.in, ret, fail, c.ret, c.fail)
		}
	}
}

func TestEventsRoundTrip(t *testing.T) {
	in := []Event{
		{Time: 1, Dur: 2, Tid: 3, Op: opRead, Path: "/a\tb%c\nd/x%41y", Off: 4, Len: 5},
		{Time: 6, Dur: 7, Tid: 8, Op: opRename, Path: "/x", Off: -1, Len: -1, Aux: "/y z"},
	}
	var b strings.Builder
	if err := writeEvents(&b, "wf\t1", "r%1", in); err != nil {
		t.Fatal(err)
	}
	wf, run, out, err := readEvents(strings.NewReader(b.String()))
	if err != nil {
		t.Fatal(err)
	}
	if wf != "wf\t1" || run != "r%1" || !reflect.DeepEqual(in, out) {
		t.Errorf("round trip: %q %q %+v", wf, run, out)
	}
}
