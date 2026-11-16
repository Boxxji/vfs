// Command mutate checks that the tests catch what they claim to. For each entry
// of mutations.json it breaks the source in one deliberate way, in a copy of the
// repository, runs the tests that should notice, and reports whether any failed. A
// mutation that no test notices has survived, and the command exits non-zero.
//
//	go run ./tools/mutate                       # every mutation
//	go run ./tools/mutate -only fence           # those whose name contains "fence"
//
// Each entry names a file, the exact text to replace (it must occur once), its
// replacement, the package to test and a -run pattern. The tests run with -short
// unless the entry says "full", for a measurement that -short skips.
// The real-server tests run if an `s3` binary is on PATH.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type mutation struct {
	Name string `json:"name"`
	File string `json:"file"`
	Old  string `json:"old"`
	New  string `json:"new"`
	Pkg  string `json:"pkg"`
	Run  string `json:"run"`
	Tags string `json:"tags,omitempty"`
	Full bool   `json:"full,omitempty"` // run without -short: the test is a measurement that -short skips
}

func main() {
	list := flag.String("list", "tools/mutate/mutations.json", "the mutations")
	only := flag.String("only", "", "run only mutations whose name contains this")
	timeout := flag.Duration("timeout", 10*time.Minute, "per mutation")
	jobs := flag.Int("j", 4, "mutations tried at once, each in its own copy")
	flag.Parse()
	if err := run(*list, *only, *timeout, *jobs); err != nil {
		fmt.Fprintln(os.Stderr, "mutate:", err)
		os.Exit(1)
	}
}

func run(list, only string, timeout time.Duration, jobs int) error {
	b, err := os.ReadFile(list)
	if err != nil {
		return err
	}
	var all []mutation
	if err := json.Unmarshal(b, &all); err != nil {
		return err
	}
	var ms []mutation
	for _, m := range all {
		if strings.Contains(m.Name, only) {
			ms = append(ms, m)
		}
	}

	verdicts := make([]string, len(ms))
	next := make(chan int)
	var wg sync.WaitGroup
	for range max(1, min(jobs, len(ms))) {
		work, err := os.MkdirTemp("", "vfs-mutate-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(work)
		if err := copyTree(".", work); err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				verdicts[i] = try(work, ms[i], timeout)
				fmt.Printf("%-9s %s\n", verdicts[i], ms[i].Name)
			}
		}()
	}
	for i := range ms {
		next <- i
	}
	close(next)
	wg.Wait()

	var survived, broken int
	for _, v := range verdicts {
		switch v {
		case "SURVIVED":
			survived++
		case "BADPATCH", "BUILD":
			broken++
		}
	}
	fmt.Printf("%d mutations, %d killed, %d survived, %d did not apply or build\n", len(ms), len(ms)-survived-broken, survived, broken)
	if survived+broken > 0 {
		return fmt.Errorf("%d survived, %d unusable", survived, broken)
	}
	return nil
}

// try applies one mutation to the copy, runs its tests, and restores the file.
func try(work string, m mutation, timeout time.Duration) string {
	path := filepath.Join(work, m.File)
	orig, err := os.ReadFile(path)
	if err != nil {
		return "BADPATCH"
	}
	if strings.Count(string(orig), m.Old) != 1 {
		return "BADPATCH"
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(orig), m.Old, m.New, 1)), 0o644); err != nil {
		return "BADPATCH"
	}
	defer os.WriteFile(path, orig, 0o644)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	args := []string{"test", "-count=1", "-timeout", timeout.String(), "-run", m.Run}
	if !m.Full {
		args = append(args, "-short")
	}
	if m.Tags != "" {
		args = append(args, "-tags", m.Tags)
	}
	cmd := exec.CommandContext(ctx, "go", append(args, m.Pkg)...)
	cmd.Dir = work
	out, err := cmd.CombinedOutput()
	switch {
	case err == nil:
		return "SURVIVED"
	case strings.Contains(string(out), "[build failed]") && !strings.Contains(string(out), "--- FAIL"):
		return "BUILD"
	}
	return "killed"
}

// copyTree copies the repository, without its history, to dst.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "bin") {
			return filepath.SkipDir
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
}
