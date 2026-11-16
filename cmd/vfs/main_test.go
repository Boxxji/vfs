package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func vfsRun(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := run(context.Background(), args, strings.NewReader(stdin), &out)
	return out.String(), err
}

func TestCLIRoundTrip(t *testing.T) {
	dir := t.TempDir()
	be := "--backend=file://" + filepath.Join(dir, "store")

	key, err := vfsRun(t, "", "keygen")
	if err != nil || !strings.HasPrefix(key, "AGE-SECRET-KEY-PQ-1") {
		t.Fatalf("keygen = %q, %v", key, err)
	}
	if _, err := vfsRun(t, key, "init", be); err != nil {
		t.Fatal(err)
	}
	if _, err := vfsRun(t, key, "init", be); !strings.Contains(errString(err), "already holds a volume") {
		t.Fatalf("second init = %v", err)
	}

	src := filepath.Join(dir, "in.bin")
	data := bytes.Repeat([]byte("hanzo vfs "), 120_000) // 1.2 MB: chunked
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := vfsRun(t, key, "put", be, "--round=1", src, "/deep/er/file.bin"); err != nil {
		t.Fatal(err)
	}
	if _, err := vfsRun(t, key, "put", be, "--round=1", src, "/other"); !strings.Contains(errString(err), "fenced") {
		t.Fatalf("put at a spent round = %v, want a fence error", err)
	}
	got, err := vfsRun(t, key, "get", be, "/deep/er/file.bin")
	if err != nil || got != string(data) {
		t.Fatalf("get returned %d bytes, %v; want %d", len(got), err, len(data))
	}
	ls, err := vfsRun(t, key, "ls", be, "/deep/er")
	if err != nil || !strings.Contains(ls, "file.bin") || !strings.Contains(ls, "1200000") {
		t.Fatalf("ls = %q, %v", ls, err)
	}
	st, err := vfsRun(t, key, "stats", be)
	if err != nil || !strings.Contains(st, "commits:      1") {
		t.Fatalf("stats = %q, %v", st, err)
	}

	// A key that is not the Volume's reads nothing.
	other, _ := vfsRun(t, "", "keygen")
	if _, err := vfsRun(t, other, "get", be, "/deep/er/file.bin"); err == nil {
		t.Fatal("get with the wrong key succeeded")
	}
	// No key on standard input is an error, not a prompt or a lookup elsewhere.
	if _, err := vfsRun(t, "", "get", be, "/deep/er/file.bin"); err == nil || !strings.Contains(err.Error(), "no key") {
		t.Fatalf("get with no key = %v", err)
	}
	// The key comes from standard input only: the command reads no environment
	// variable at all.
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range []string{"os.Getenv", "os.LookupEnv", "os.Environ", "os.ExpandEnv"} {
			if strings.Contains(string(src), call) {
				t.Errorf("%s calls %s: the command must read no environment variable", name, call)
			}
		}
	}
	if _, err := vfsRun(t, key, "get", be, "/nope"); !strings.Contains(errString(err), fs.ErrNotExist.Error()) {
		t.Fatalf("get of absent = %v", err)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// A Volume copied to another directory opens there after `vfs reseal`, whether or not the
// old place is remembered, and a repeat at a higher round is not an error.
func TestCLIReseal(t *testing.T) {
	dir := t.TempDir()
	old := "file://" + filepath.Join(dir, "old")
	key, _ := vfsRun(t, "", "keygen")
	if _, err := vfsRun(t, key, "init", "--backend="+old); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "in")
	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := vfsRun(t, key, "put", "--backend="+old, "--round=1", src, "/f"); err != nil {
		t.Fatal(err)
	}
	for i, from := range []string{old, ""} {
		to := "file://" + filepath.Join(dir, "new"+string(rune('a'+i)))
		if err := os.CopyFS(filepath.Join(dir, "new"+string(rune('a'+i))), os.DirFS(filepath.Join(dir, "old"))); err != nil {
			t.Fatal(err)
		}
		if _, err := vfsRun(t, key, "get", "--backend="+to, "/f"); err == nil || !strings.Contains(err.Error(), "vfs reseal") {
			t.Fatalf("get at the copy before reseal = %v, want the hint to reseal", err)
		}
		args := []string{"reseal", "--backend=" + to, "--round=5"}
		if from != "" {
			args = append(args, "--from="+from)
		}
		if _, err := vfsRun(t, key, args...); err != nil {
			t.Fatalf("reseal from %q: %v", from, err)
		}
		if got, err := vfsRun(t, key, "get", "--backend="+to, "/f"); err != nil || got != "hello" {
			t.Fatalf("get after reseal = %q, %v", got, err)
		}
		args[2] = "--round=6"
		if _, err := vfsRun(t, key, args...); err != nil {
			t.Fatalf("repeat of reseal from %q: %v", from, err)
		}
	}
	if _, err := vfsRun(t, key, "reseal", "--backend="+old); err == nil {
		t.Fatal("reseal without a round succeeded")
	}
	// a place that is not there is not made by naming it as the place to move from
	typo := filepath.Join(dir, "typo", "deeper")
	if err := os.CopyFS(filepath.Join(dir, "newc"), os.DirFS(filepath.Join(dir, "old"))); err != nil {
		t.Fatal(err)
	}
	if _, err := vfsRun(t, key, "reseal", "--backend=file://"+filepath.Join(dir, "newc"), "--round=9", "--from=file://"+typo); err == nil {
		t.Fatal("reseal from a place that holds no Volume succeeded")
	}
	if _, err := os.Stat(filepath.Dir(typo)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("naming %s made it: %v", typo, err)
	}
	// a copy without its objects is refused, and --verify is accepted
	bare := filepath.Join(dir, "bare")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(filepath.Join(dir, "partial"), os.DirFS(filepath.Join(dir, "old"))); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, "partial", "meta")); err != nil {
		t.Fatal(err)
	}
	if _, err := vfsRun(t, key, "reseal", "--backend=file://"+filepath.Join(dir, "partial"), "--round=5", "--verify"); err == nil {
		t.Fatal("reseal of a copy without its shards succeeded")
	}
	if _, err := vfsRun(t, key, "reseal", "--backend=file://"+filepath.Join(dir, "newa"), "--round=7", "--verify"); err != nil {
		t.Fatalf("reseal --verify of a whole copy: %v", err)
	}
}
