package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// duplicate is the snapshot command: it measures starting a run from a copy of
// a finished checkout instead of a clone. A volume snapshot cloned in constant
// time hands the sandbox exactly this, a tree whose files have the contents and
// modification times of the original but new inode numbers and change times.
// The command copies a checkout with cp -a, which has those properties, and
// times the first status, where git decides for every file whether it changed.
func duplicate(args []string, out io.Writer) error {
	var f flags
	fl := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	f.register(fl)
	if err := fl.Parse(args); err != nil {
		return err
	}
	if f.list == "default" {
		f.list = "default,checkstat"
	}
	knobs, err := names(f.list)
	if err != nil {
		return err
	}
	f.bulk = 10
	b, done, err := f.open(out)
	if err != nil {
		return err
	}
	defer done()
	base := filepath.Join(b.root, "base")
	if err := os.MkdirAll(base, 0o755); err != nil {
		return err
	}
	fx := b.fx
	if _, err := fx.git(base, nil, "clone", "--quiet", "--depth", "1", "file://"+fx.origin, "."); err != nil {
		return err
	}
	if _, err := fx.git(base, nil, "status", "--porcelain"); err != nil {
		return err
	}
	return b.loop(knobs, f.reps, f.counts, func(w *world) {
		if err := os.MkdirAll(w.dir, 0o755); err != nil {
			w.fail("copy", err)
			return
		}
		w.raw("copy", "cp", "-a", base+"/.", w.dir)
		w.do("status_copy", "", "status", "--porcelain")
		w.do("status_copy_warm", "", "status", "--porcelain")
		w.check()
	})
}

// check fails the sequence if the copy is not the checkout it was made from:
// a clean tree must stay clean.
func (w *world) check() {
	if w.dead {
		return
	}
	out, err := w.set.fx.git(w.dir, nil, "status", "--porcelain")
	if err != nil {
		w.fail("check", err)
		return
	}
	if len(out) != 0 {
		w.fail("check", fmt.Errorf("the copy is not clean: %.80s", out))
	}
}
