package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// synthetic writes a dataset: one workflow, two projects with two runs
// each. Every run reads the same toolchain binary and twenty files of
// its own workspace, a second apart, so a prefetcher that knows run 1
// has all the time it needs and a cold cache has none.
func synthetic(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, label := range []string{"a", "b"} {
		for n := 1; n <= 2; n++ {
			run := fmt.Sprintf("%s%d", label, n)
			out := filepath.Join(dir, "wf", run)
			if err := os.MkdirAll(out, 0o755); err != nil {
				t.Fatal(err)
			}
			ws := "/ws/" + label
			sizes := map[string]int64{"/tc/bin": 3 << 20}
			events := []Event{{Time: 1e8, Dur: 1000, Tid: 1, Op: opExec, Path: "/tc/bin", Off: 0, Len: -1}}
			for i := 0; i < 20; i++ {
				p := fmt.Sprintf("%s/f%02d", ws, i)
				sizes[p] = 100 << 10
				events = append(events,
					Event{Time: int64(i+1) * 1e9, Dur: 5000, Tid: 1, Op: opOpen, Path: p},
					Event{Time: int64(i+1)*1e9 + 1000, Dur: 20000, Tid: 1, Op: opRead, Path: p, Off: 0, Len: 100 << 10})
			}
			write := func(name string, fn func(f *os.File) error) {
				f, err := os.Create(filepath.Join(out, name))
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if err := fn(f); err != nil {
					t.Fatal(err)
				}
			}
			write("events.tsv", func(f *os.File) error { return writeEvents(f, "wf", run, events) })
			write("sizes.tsv", func(f *os.File) error { return writeSizes(f, "wf", run, sizes) })
			b, _ := json.Marshal(Manifest{Workflow: "wf", Project: label, Run: run, Wall: 21e9, Plain: 20e9, Warm: 5e9, Events: len(events),
				Roots: map[string]string{"workspace": ws, "toolchain": "/tc"}})
			if err := os.WriteFile(filepath.Join(out, "manifest.json"), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dir
}

func TestReportOnASyntheticDataset(t *testing.T) {
	var buf bytes.Buffer
	if err := report(&buf, synthetic(t)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"### Summary", "### Predictability", "### Order", "### Reuse across projects", "### Read calls",
		"### Cache policies", "### Sensitivity", "### Working set", "### Time waiting for file reads"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q", want)
		}
	}
	row := func(prefix string) []string {
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, prefix) {
				var cells []string
				for _, c := range strings.Split(strings.Trim(line, "| "), " | ") {
					cells = append(cells, strings.TrimSpace(c))
				}
				return cells
			}
		}
		t.Fatalf("no row starting %q", prefix)
		return nil
	}
	// predictability: run 2 reads exactly what run 1 read
	if c := row("| wf | a | reads+maps |"); c[4] != "100.0" || c[6] != "100.0" || c[8] != "100.0" {
		t.Errorf("predictability %v", c)
	}
	// reuse: the toolchain binary is shared, the workspace is not
	if c := row("| wf | b from a | toolchain |"); c[5] != "100.0" {
		t.Errorf("toolchain reuse %v", c)
	}
	if c := row("| wf | b from a | workspace |"); c[5] != "0.0" {
		t.Errorf("workspace reuse %v", c)
	}
	// policies: a cold cache misses everything, a prefetcher that knows run 1 hits it all
	if c := row("| wf | LRU 4 GiB |"); c[3] != "0.0" {
		t.Errorf("cold LRU hit %v", c)
	}
	if c := row("| wf | LRU 4 GiB + trace prefetch |"); c[3] != "100.0" || c[9] != "0.00 / 0.00" {
		t.Errorf("trace prefetch %v", c)
	}
	if c := row("| wf | same node, LRU 64 MiB |"); c[3] != "100.0" {
		t.Errorf("same node %v", c)
	}
	// the other project shares only the toolchain: preloading it hits one extent in four
	if c := row("| wf | LRU 4 GiB + base warm set |"); c[3] == "0.0" {
		t.Errorf("base warm set did not hit %v", c)
	}
	// time: the disk costs 15 of the 20 cold seconds
	if c := row("| wf | a | 20 |"); c[7] != "75.0" {
		t.Errorf("disk share %v", c)
	}
}
