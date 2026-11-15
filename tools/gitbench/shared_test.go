package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// cloned takes the clone step of the sequence under a named config and
// returns the working directory.
func cloned(t *testing.T, fx *fixture, name string) string {
	t.Helper()
	k, err := pick(name)
	if err != nil {
		t.Fatal(err)
	}
	w := &world{set: &settings{fx: fx, until: "clone"}, knob: k, dir: filepath.Join(t.TempDir(), "w")}
	w.session()
	if w.got[0].exit != 0 {
		t.Fatalf("%s clone: %s", name, w.got[0].note)
	}
	return w.dir
}

// stored counts the objects a clone holds itself, loose and packed, and the
// bytes they take.
func stored(t *testing.T, fx *fixture, dir string) (objects, kb int) {
	t.Helper()
	out, err := fx.git(dir, nil, "count-objects", "-v")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(string(out), "\n") {
		k, v, _ := strings.Cut(l, ": ")
		n, _ := strconv.Atoi(v)
		switch k {
		case "count", "in-pack":
			objects += n
		case "size", "size-pack":
			kb += n
		}
	}
	return objects, kb
}

func TestReferenceCloneBorrowsTheStore(t *testing.T) {
	fx := built(t, 60, 12)
	plain := cloned(t, fx, "default")
	borrow := cloned(t, fx, "ref")
	store, err := fx.cache()
	if err != nil {
		t.Fatal(err)
	}
	alt, err := os.ReadFile(filepath.Join(borrow, ".git", "objects", "info", "alternates"))
	if err != nil || strings.TrimSpace(string(alt)) != filepath.Join(store, "objects") {
		t.Fatalf("alternates = %q (%v), want the store's object directory", alt, err)
	}
	if _, err := os.Stat(filepath.Join(plain, ".git", "objects", "info", "alternates")); err == nil {
		t.Fatal("a plain clone must not borrow")
	}
	own, _ := stored(t, fx, plain)
	borrowed, _ := stored(t, fx, borrow)
	if own == 0 || borrowed != 0 {
		t.Fatalf("objects written: plain %d, by reference %d; the reference clone must write none", own, borrowed)
	}
	for _, d := range []string{plain, borrow} {
		if _, err := fx.git(d, nil, "fsck", "--no-dangling"); err != nil {
			t.Fatalf("%s: %v", d, err)
		}
		if out, err := fx.git(d, nil, "status", "--porcelain"); err != nil || len(out) != 0 {
			t.Fatalf("%s: a fresh clone must be clean: %q %v", d, out, err)
		}
	}
}

// git gc repacks only what the clone owns: the borrowed objects stay in the
// store, the alternates file stays, and the repository stays whole.
func TestGarbageCollectionLeavesBorrowedObjectsAlone(t *testing.T) {
	fx := built(t, 60, 12)
	dir := cloned(t, fx, "ref")
	if err := os.WriteFile(filepath.Join(dir, "alpha", "added.txt"), []byte("work\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"add", "-A"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "work"},
		{"gc", "--quiet", "--prune=now"},
		{"fsck", "--no-dangling"},
	} {
		if out, err := fx.git(dir, nil, args...); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	owned, _ := stored(t, fx, dir)
	if owned >= 10 {
		t.Fatalf("gc copied the store into the clone: %d objects (the work is 3)", owned)
	}
	if _, err := os.Stat(filepath.Join(dir, ".git", "objects", "info", "alternates")); err != nil {
		t.Fatalf("gc removed the alternates file: %v", err)
	}
}

// The hazard of sharing: when the store drops objects that a clone borrowed,
// the clone is broken. A store must therefore never prune while a clone that
// borrows from it lives, or the clone must stop borrowing first.
func TestPrunedStoreBreaksABorrowingClone(t *testing.T) {
	fx := built(t, 60, 12)
	borrow := cloned(t, fx, "ref")
	store, _ := fx.cache()
	prune(t, fx, store)
	if _, err := fx.git(borrow, nil, "fsck", "--no-dangling"); err == nil {
		t.Fatal("a clone must not survive its store losing the objects it borrowed")
	}
}

// Repacking without -l copies what was borrowed, so the clone stands alone
// once the alternates file is removed; a clone made with --dissociate is born
// that way.
func TestDissociatedCloneSurvivesAPrunedStore(t *testing.T) {
	fx := built(t, 60, 12)
	late := cloned(t, fx, "ref")
	early := cloned(t, fx, "refcopy")
	if _, err := fx.git(late, nil, "repack", "-a", "-d", "--quiet"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(late, ".git", "objects", "info", "alternates")); err != nil {
		t.Fatal(err)
	}
	store, _ := fx.cache()
	prune(t, fx, store)
	for name, dir := range map[string]string{"repacked": late, "dissociated": early} {
		if out, err := fx.git(dir, nil, "fsck", "--no-dangling"); err != nil {
			t.Errorf("%s clone broke when the store was pruned: %v %s", name, err, out)
		}
	}
	if _, err := os.Stat(filepath.Join(early, ".git", "objects", "info", "alternates")); err == nil {
		t.Error("--dissociate must remove the alternates file")
	}
}

// Work done in a borrowing clone goes back to the origin like any other.
func TestPushFromABorrowingClone(t *testing.T) {
	fx := built(t, 60, 12)
	dir := cloned(t, fx, "ref")
	if err := os.WriteFile(filepath.Join(dir, "beta", "new.txt"), []byte("from the sandbox\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"switch", "--quiet", "-c", "agent/run"},
		{"add", "-A"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "work"},
		{"push", "--quiet", "file://" + fx.origin, "HEAD:refs/heads/agent/run"},
	} {
		if out, err := fx.git(dir, nil, args...); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	tip, err := fx.git(dir, nil, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	got, err := fx.git(fx.origin, nil, "rev-parse", "refs/heads/agent/run")
	if err != nil || string(got) != string(tip) {
		t.Fatalf("origin has %q, the clone pushed %q (%v)", got, tip, err)
	}
	if out, err := fx.git(fx.origin, nil, "fsck", "--no-dangling"); err != nil {
		t.Fatalf("origin after the push: %v %s", err, out)
	}
}

// prune empties the store the way a cache rotation would: every ref gone, no
// reflog, unreachable objects removed now.
func prune(t *testing.T, fx *fixture, store string) {
	t.Helper()
	refs, err := fx.git(store, nil, "for-each-ref", "--format=%(refname)")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range strings.Fields(string(refs)) {
		if _, err := fx.git(store, nil, "update-ref", "-d", r); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"reflog", "expire", "--expire=now", "--all"},
		{"gc", "--quiet", "--prune=now"},
	} {
		if out, err := fx.git(store, nil, args...); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
}

// The sandbox mounts the store read-only: nothing git does in a borrowing clone
// writes to it.
func TestBorrowedStoreMayBeReadOnly(t *testing.T) {
	fx := built(t, 60, 12)
	dir := cloned(t, fx, "ref")
	store, _ := fx.cache()
	lock(t, store, 0o555, 0o444)
	t.Cleanup(func() { lock(t, store, 0o755, 0o644) })
	if err := os.WriteFile(filepath.Join(dir, "gamma", "added.txt"), []byte("work\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"switch", "--quiet", "-c", "agent/run"},
		{"add", "-A"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "work"},
		{"gc", "--quiet"},
		{"fsck", "--no-dangling"},
		{"push", "--quiet", "file://" + fx.origin, "HEAD:refs/heads/agent/run"},
	} {
		if out, err := fx.git(dir, nil, args...); err != nil {
			t.Fatalf("git %v with a read-only store: %v %s", args, err, out)
		}
	}
}

// lock sets the mode of every directory and file under root.
func lock(t *testing.T, root string, dir, file os.FileMode) {
	t.Helper()
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		os.Chmod(p, file)
		return nil
	})
	var dirs []string
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, p)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		os.Chmod(dirs[i], dir)
	}
}
