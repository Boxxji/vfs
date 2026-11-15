package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// request is one HTTP exchange of a clone: when git sent it and how long the
// server took to start answering. For a git server the second number is the
// server's own compute (authorisation, spawning upload-pack, walking objects)
// plus one network round trip.
type request struct {
	kind string // "get" for ref discovery, "post" for ls-refs and fetch
	wait time.Duration
}

// clock reads the time of day git's trace writes at the start of a line.
func clock(line string) (time.Time, bool) {
	if len(line) < 15 {
		return time.Time{}, false
	}
	t, err := time.Parse("15:04:05.000000", line[:15])
	return t, err == nil
}

// requests pairs each request header in a GIT_TRACE_CURL log with the first
// response header after it.
func requests(curl string) []request {
	var out []request
	var kind string
	var sent time.Time
	sc := bufio.NewScanner(strings.NewReader(curl))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		at, ok := clock(line)
		if !ok {
			continue
		}
		switch {
		case strings.Contains(line, "=> Send header: GET "):
			kind, sent = "get", at
		case strings.Contains(line, "=> Send header: POST "):
			kind, sent = "post", at
		case strings.Contains(line, "<= Recv header: HTTP/") && kind != "":
			out = append(out, request{kind, at.Sub(sent)})
			kind = ""
		}
	}
	return out
}

// protocol reads the wire protocol version the server answered with from a
// GIT_TRACE_PACKET log.
func protocol(packet string) string {
	if strings.Contains(packet, "git< version 2") {
		return "2"
	}
	if strings.Contains(packet, "git< version 1") {
		return "1"
	}
	return "0"
}

// remote is the net command: clone a repository served over HTTP the way a
// coding run does and report where the time goes, request by request.
func remote(args []string, out io.Writer) error {
	fl := flag.NewFlagSet("net", flag.ContinueOnError)
	url := fl.String("url", "", "repository to clone; it is only read")
	dir := fl.String("dir", os.TempDir(), "directory for the clones")
	reps := fl.Int("reps", 5, "repetitions")
	filter := fl.String("filter", "blob:none", "partial clone filter; empty for a whole-tree clone")
	depth := fl.Int("depth", 1, "history depth; 0 for all")
	conf := fl.String("c", "", "comma list of key=value given to git with -c")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if *url == "" {
		return fmt.Errorf("-url is required")
	}
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	root := filepath.Join(abs, "gitbench-net-"+strconv.Itoa(os.Getpid()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(root)
	home, gitconf, err := isolate(root)
	if err != nil {
		return err
	}
	fx := &fixture{root: root, home: home, conf: gitconf}
	sh, err := newsheet(out, abs, strings.TrimSuffix(*url, ".git"), 0)
	if err != nil {
		return err
	}
	name := "filter=" + *filter
	if *filter == "" {
		name = "whole"
	}
	for rep := 1; rep <= *reps; rep++ {
		work := filepath.Join(root, "w"+itoa(rep))
		if err := os.MkdirAll(work, 0o755); err != nil {
			return err
		}
		packet, curl := filepath.Join(root, "packet"+itoa(rep)), filepath.Join(root, "curl"+itoa(rep))
		argv := []string{"git"}
		for c := range strings.SplitSeq(*conf, ",") {
			if c != "" {
				argv = append(argv, "-c", c)
			}
		}
		argv = append(argv, "clone", "--quiet", "--single-branch")
		if *depth > 0 {
			argv = append(argv, "--depth", itoa(*depth))
		}
		if *filter != "" {
			argv = append(argv, "--filter="+*filter)
		}
		argv = append(argv, *url, ".")
		cmd := exec.Command(argv[0], argv[1:]...)
		cmd.Dir = work
		cmd.Env = fx.sandbox("GIT_TRACE_PACKET="+packet, "GIT_TRACE_CURL="+curl, "GIT_TRACE_CURL_NO_DATA=1")
		var errs tail
		cmd.Stderr = &errs
		ld := load()
		start := time.Now()
		err := cmd.Run()
		wall := time.Since(start)
		t := sample{op: "net_clone", wall: wall, load: ld}
		if cmd.ProcessState != nil {
			t.cpu = cmd.ProcessState.UserTime() + cmd.ProcessState.SystemTime()
			t.exit = cmd.ProcessState.ExitCode()
		}
		if err != nil {
			t.exit, t.note = max(t.exit, 1), strings.Join(strings.Fields(string(errs.b)), " ")
		}
		rawp, _ := os.ReadFile(packet)
		rawc, _ := os.ReadFile(curl)
		reqs := requests(string(rawc))
		kb, inner := size(filepath.Join(work, ".git"))
		_, all := size(work)
		m := cells(knob{name: name}, 0, 0, rep, t, nil)
		m["req"], m["proto"] = itoa(len(reqs)), protocol(string(rawp))
		m["git_kb"], m["tree_files"] = strconv.FormatInt(kb, 10), itoa(all-inner)
		if err := sh.put(m); err != nil {
			return err
		}
		for i, r := range reqs {
			m := cells(knob{name: name}, 0, 0, rep, sample{op: fmt.Sprintf("net_%d_%s", i+1, r.kind), wall: r.wait, load: ld}, nil)
			m["ttfb_ms"] = ms(r.wait)
			if err := sh.put(m); err != nil {
				return err
			}
		}
		os.RemoveAll(work)
	}
	return nil
}
