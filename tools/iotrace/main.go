// iotrace records the file accesses of a command and measures whether a
// prefetcher could have predicted them.
//
//	iotrace record -out DIR -workflow W -run R -- command args...
//	iotrace run -workflow W -project DIR -label L -state DIR -data DIR
//	iotrace evict PATH...
//	iotrace calibrate -dir DIR -out FILE
//	iotrace report -data DIR
//
// record traces one command with strace and writes DIR/events.tsv (one
// line per access: workflow, run, time, duration, thread, op, path,
// offset, length, second path), DIR/sizes.tsv and DIR/manifest.json.
// run builds a fresh state, drops the page cache and records a catalogued
// workflow several times. report reads a directory of recorded runs and
// prints the tables of docs/prefetch.md.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

type roots map[string]string

func (r roots) String() string { return fmt.Sprint(map[string]string(r)) }
func (r roots) Set(v string) error {
	k, p, ok := strings.Cut(v, "=")
	if !ok || k == "" || p == "" {
		return fmt.Errorf("want class=path, got %q", v)
	}
	r[k] = p
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fail(fmt.Errorf("usage: iotrace record|run|evict|calibrate|report"))
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "record":
		fs := flag.NewFlagSet("record", flag.ExitOnError)
		out := fs.String("out", "", "directory for the recorded run")
		wf := fs.String("workflow", "", "workflow name")
		run := fs.String("run", "1", "run name")
		dir := fs.String("dir", ".", "working directory of the command")
		keep := fs.Bool("keep", false, "keep the raw strace output")
		cls := roots{}
		fs.Var(cls, "root", "class=path, repeatable; classes the report groups paths by")
		fs.Parse(args)
		if *out == "" || *wf == "" || fs.NArg() == 0 {
			fail(fmt.Errorf("record needs -out, -workflow and a command"))
		}
		m, err := record(Manifest{Workflow: *wf, Project: *run, Run: *run, Argv: fs.Args(), Dir: *dir, Roots: cls}, os.Environ(), *out, *keep)
		if err != nil {
			fail(err)
		}
		fmt.Printf("%d events, exit %d, %.2f s\n", m.Events, m.Exit, float64(m.Wall)/1e9)
	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		wf := fs.String("workflow", "", "workflow name")
		label := fs.String("label", "a", "project label in the dataset")
		data := fs.String("data", "", "dataset directory")
		runs := fs.Int("runs", 2, "runs per workflow")
		plain := fs.Int("plain", 0, "untraced cold and warm timing pairs to take after the traced runs")
		keep := fs.Bool("keep", false, "keep the raw strace output")
		var p Params
		fs.StringVar(&p.Project, "project", "", "source tree")
		fs.StringVar(&p.State, "state", "", "scratch directory for the fresh state")
		fs.StringVar(&p.Cache, "cache", "", "populated package cache")
		fs.StringVar(&p.From, "from", "", "git: commit to start from")
		fs.StringVar(&p.To, "to", "", "git: commit to check out")
		fs.Parse(args)
		if *wf == "" || *data == "" || p.Project == "" {
			fail(fmt.Errorf("run needs -workflow, -data and -project"))
		}
		if err := run(*wf, *label, p, *data, *runs, *plain, *keep); err != nil {
			fail(err)
		}
	case "evict":
		files, bytes, err := evict(args)
		if err != nil {
			fail(err)
		}
		fmt.Printf("%d files, %d bytes\n", files, bytes)
	case "calibrate":
		fs := flag.NewFlagSet("calibrate", flag.ExitOnError)
		dir := fs.String("dir", os.TempDir(), "scratch directory")
		out := fs.String("out", "", "calibration file to write")
		fs.Parse(args)
		c, err := calibrate(*dir)
		if err != nil {
			fail(err)
		}
		b, _ := json.MarshalIndent(c, "", "  ")
		if *out == "" {
			fmt.Println(string(b))
		} else if err := os.WriteFile(*out, b, 0o644); err != nil {
			fail(err)
		}
	case "probe":
		if len(args) != 1 {
			fail(fmt.Errorf("probe takes one file"))
		}
		if err := probe(args[0]); err != nil {
			fail(err)
		}
	case "report":
		fs := flag.NewFlagSet("report", flag.ExitOnError)
		data := fs.String("data", "", "dataset directory")
		fs.Parse(args)
		if *data == "" {
			fail(fmt.Errorf("report needs -data"))
		}
		if err := report(os.Stdout, *data); err != nil {
			fail(err)
		}
	default:
		fail(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "iotrace:", err)
	os.Exit(1)
}
