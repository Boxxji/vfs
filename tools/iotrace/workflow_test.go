package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryWorkflowIsComplete(t *testing.T) {
	p := Params{Project: "/p", State: "/s", Cache: "/c", From: "a", To: "b"}
	for name, mk := range workflows {
		pl, err := mk(p)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(pl.run) == 0 || pl.dir == "" || pl.roots["workspace"] == "" {
			t.Errorf("%s: incomplete plan %+v", name, pl)
		}
		for _, c := range pl.prep {
			if len(c) == 0 {
				t.Errorf("%s: empty prep command", name)
			}
		}
		// the workspace of a run is under the state directory, so two
		// projects with different states never share a path
		if name != "ripgrep" && !strings.HasPrefix(pl.dir, "/s/") {
			t.Errorf("%s: runs in %s, outside the state", name, pl.dir)
		}
	}
}

func TestWorkflowsNeedTheirInputs(t *testing.T) {
	for name, p := range map[string]Params{
		"checkout": {Project: "/p", State: "/s", From: "a"},
		"npm":      {Project: "/p", State: "/s"},
		"uvsync":   {Project: "/p", State: "/s"},
	} {
		if _, err := workflows[name](p); err == nil {
			t.Errorf("%s accepted missing inputs", name)
		}
	}
}

func TestEnvironmentIsMinimal(t *testing.T) {
	t.Setenv("LD_LIBRARY_PATH", "/should/not/leak")
	t.Setenv("GOFLAGS", "-x")
	env := strings.Join(environment([]string{"A=b"}, []string{"sh"}), "\n")
	for _, bad := range []string{"LD_LIBRARY_PATH", "GOFLAGS", "/should/not/leak"} {
		if strings.Contains(env, bad) {
			t.Errorf("environment carries %s:\n%s", bad, env)
		}
	}
	if !strings.Contains(env, "PATH=") || !strings.Contains(env, "/usr/bin") || !strings.Contains(env, "A=b") {
		t.Errorf("environment:\n%s", env)
	}
}

func TestToleratedExitStatus(t *testing.T) {
	p := Params{Project: "/p", State: "/s"}
	if pl, _ := workflows["ripgrep"](p); pl.exit != 1 {
		t.Error("ripgrep exits 1 when nothing matches")
	}
	if pl, _ := workflows["gotest"](p); pl.exit != 1 {
		t.Error("a failing test still exercised the disk")
	}
	if pl, _ := workflows["gobuild"](p); pl.exit != 0 {
		t.Error("a failed build is not a run")
	}
}

func TestToolDirectoriesComeBeforeSystemOnes(t *testing.T) {
	dir := t.TempDir()
	tool := filepath.Join(dir, "faketool")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", "/usr/bin:/bin:"+dir)
	env := environment(nil, []string{"sh", "faketool"})
	if !strings.HasPrefix(env[0], "PATH="+dir+":") || !strings.HasSuffix(env[0], ":/usr/local/bin:/usr/bin:/bin") {
		t.Errorf("%s", env[0])
	}
}
