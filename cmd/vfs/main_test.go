package main

import (
	"bytes"
	"context"
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
