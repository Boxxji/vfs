package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Params names what a workflow runs on.
type Params struct {
	Project string // the source tree
	State   string // scratch directory for the fresh state of one run
	Cache   string // pre-populated package cache (npm, uv)
	From    string // git: commit checked out before the run
	To      string // git: commit checked out by the run
}

// plan is one workflow made concrete.
type plan struct {
	prep  [][]string        // untraced commands that build the fresh state
	run   []string          // the traced command
	dir   string            // its working directory
	env   []string          // added to the minimal environment
	evict []string          // trees whose page cache is dropped before the run
	roots map[string]string // path classes for the report
	exit  int               // highest exit status that still counts as a finished run
}

// workflows is the catalogue. Each entry builds the same fresh state for
// every run: the workspace is recreated at the same path, caches the
// workflow is meant to start without are empty, caches it starts with
// are copied from a populated one.
var workflows = map[string]func(Params) (plan, error){
	"checkout": func(p Params) (plan, error) {
		w := filepath.Join(p.State, "w")
		if p.From == "" || p.To == "" {
			return plan{}, fmt.Errorf("checkout needs -from and -to")
		}
		return plan{
			prep: [][]string{
				{"rm", "-rf", w},
				{"git", "clone", "--quiet", "--shared", "--no-checkout", p.Project, w},
				{"git", "-C", w, "checkout", "--quiet", p.From},
				// A checkout leaves the index entries written in its last
				// second "racily clean": the next command reads those files
				// to compare them, and how many there are depends on how
				// fast the checkout ran. Settling the workspace first makes
				// every run start from the state a long-lived one is in.
				{"sleep", "2"},
				{"git", "-C", w, "update-index", "-q", "--refresh"},
			},
			// Git maps a pack in windows of 1 GiB, so a strace sees the whole pack
			// mapped where a few pages are touched. A 1 MiB window leaves the
			// pages touched alone and narrows what a mapping can be taken to mean.
			run:   []string{"git", "-c", "core.packedGitWindowSize=1m", "checkout", "--quiet", p.To},
			dir:   w,
			evict: []string{w},
			roots: map[string]string{"workspace": w, "objects": filepath.Join(p.Project, ".git")},
		}, nil
	},
	"npm": func(p Params) (plan, error) {
		w, c := filepath.Join(p.State, "w"), filepath.Join(p.State, "cache")
		if p.Cache == "" {
			return plan{}, fmt.Errorf("npm needs -cache")
		}
		return plan{
			prep: [][]string{
				{"rm", "-rf", w, c},
				{"mkdir", "-p", w},
				{"sh", "-c", `tar -C "$1" --exclude=node_modules --exclude=.git -cf - . | tar -C "$2" -xf -`, "sh", p.Project, w},
				{"cp", "-a", p.Cache, c},
			},
			run:   []string{"npm", "ci", "--offline", "--ignore-scripts", "--no-audit", "--no-fund", "--cache", c},
			dir:   w,
			env:   []string{"npm_config_update_notifier=false"},
			evict: []string{w, c, nodeRoot()},
			roots: map[string]string{"workspace": w, "cache": c, "toolchain": nodeRoot()},
		}, nil
	},
	"gobuild": func(p Params) (plan, error) { return gocmd(p, []string{"go", "build", "./..."}, 0) },
	"gotest":  func(p Params) (plan, error) { return gocmd(p, []string{"go", "test", "-count=1", "./..."}, 1) },
	"cargocheck": func(p Params) (plan, error) {
		w, t := filepath.Join(p.State, "w"), filepath.Join(p.State, "target")
		return plan{
			prep: [][]string{
				{"rm", "-rf", w, t},
				{"mkdir", "-p", w},
				{"sh", "-c", `tar -C "$1" --exclude=target --exclude=.git -cf - . | tar -C "$2" -xf -`, "sh", p.Project, w},
			},
			run: []string{"cargo", "check", "--offline", "--locked", "--all-targets"},
			dir: w,
			// An empty RUSTC_WRAPPER switches off a compile cache named in the
			// user's cargo configuration: a cache would answer the build from
			// elsewhere, and its daemon outlives the command being traced.
			env:   []string{"CARGO_TARGET_DIR=" + t, "RUSTC_WRAPPER="},
			evict: []string{w, rustRoot()},
			roots: map[string]string{"workspace": w, "cache": cargoRoot(), "toolchain": rustRoot(), "build": t},
		}, nil
	},
	"uvsync": func(p Params) (plan, error) {
		w, c := filepath.Join(p.State, "w"), filepath.Join(p.State, "cache")
		if p.Cache == "" {
			return plan{}, fmt.Errorf("uvsync needs -cache")
		}
		return plan{
			prep: [][]string{
				{"rm", "-rf", w, c},
				{"mkdir", "-p", w},
				{"sh", "-c", `tar -C "$1" --exclude=.venv --exclude=.git -cf - . | tar -C "$2" -xf -`, "sh", p.Project, w},
				{"cp", "-a", p.Cache, c},
			},
			run:   []string{"uv", "sync", "--offline", "--frozen", "--no-progress"},
			dir:   w,
			env:   []string{"UV_CACHE_DIR=" + c, "UV_PYTHON_DOWNLOADS=never", "UV_PYTHON_PREFERENCE=only-managed"},
			evict: []string{w, c},
			roots: map[string]string{"workspace": w, "cache": c, "toolchain": filepath.Join(os.Getenv("HOME"), ".local", "share", "uv", "python")},
		}, nil
	},
	"ripgrep": func(p Params) (plan, error) {
		return plan{
			run:   []string{"rg", "--no-ignore", "--hidden", "--no-messages", "-c", "-e", "TODO|FIXME", "."},
			dir:   p.Project,
			evict: []string{p.Project},
			roots: map[string]string{"workspace": p.Project},
			exit:  1, // no match is not a failure
		}, nil
	},
	"agent": func(p Params) (plan, error) {
		w, g := filepath.Join(p.State, "w"), filepath.Join(p.State, "gocache")
		script := `set -e
find . -name '*.go' -not -path './.git/*' | sort > files.list
head -40 files.list | while read -r f; do cat "$f" > /dev/null; done
git status --short > /dev/null
printf '\n// edit\n' >> "$(head -1 files.list)"
go build ./...
git diff --stat > /dev/null
rm files.list`
		return plan{
			prep: [][]string{
				{"rm", "-rf", w, g, filepath.Join(p.State, "tmp")},
				{"mkdir", "-p", filepath.Join(p.State, "tmp")},
				{"git", "clone", "--quiet", "--shared", p.Project, w},
			},
			run:   []string{"sh", "-c", script},
			dir:   w,
			env:   []string{"GOCACHE=" + g, "GOFLAGS=-mod=readonly", "GOPROXY=off", "GOTMPDIR=" + filepath.Join(p.State, "tmp")},
			evict: []string{w, goRoot()},
			roots: map[string]string{"workspace": w, "objects": filepath.Join(p.Project, ".git"), "toolchain": goRoot(), "cache": modRoot(), "build": g},
		}, nil
	},
}

// gocmd is a Go command on a copy of the project with an empty build
// cache and the module cache as it stands. exit is the highest status
// that counts as a finished run.
func gocmd(p Params, run []string, exit int) (plan, error) {
	w, g := filepath.Join(p.State, "w"), filepath.Join(p.State, "gocache")
	tmp := filepath.Join(p.State, "tmp")
	return plan{
		prep: [][]string{
			{"rm", "-rf", w, g, tmp},
			{"mkdir", "-p", w, tmp},
			{"sh", "-c", `tar -C "$1" --exclude=.git -cf - . | tar -C "$2" -xf -`, "sh", p.Project, w},
		},
		exit:  exit,
		run:   run,
		dir:   w,
		env:   []string{"GOCACHE=" + g, "GOFLAGS=-mod=readonly", "GOPROXY=off", "GOTMPDIR=" + tmp},
		evict: []string{w, goRoot()},
		roots: map[string]string{"workspace": w, "toolchain": goRoot(), "cache": modRoot(), "build": g},
	}, nil
}

// toolRoot is the directory two levels above a tool on PATH
// (…/bin/tool -> …), where its installation lives.
func toolRoot(tool string) string {
	p, err := exec.LookPath(tool)
	if err != nil {
		return ""
	}
	if r, err := filepath.EvalSymlinks(p); err == nil {
		p = r
	}
	return filepath.Dir(filepath.Dir(p))
}

func goRoot() string {
	out, err := exec.Command("go", "env", "GOROOT").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func modRoot() string {
	out, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// nodeRoot is the installation of node unless it is part of the system
// tree, which is shared and stays cached.
func nodeRoot() string {
	switch r := toolRoot("node"); r {
	case "/", "/usr", "/usr/local":
		return ""
	default:
		return r
	}
}

func rustRoot() string {
	out, err := exec.Command("rustc", "--print", "sysroot").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func cargoRoot() string { return filepath.Join(os.Getenv("HOME"), ".cargo", "registry") }

// environment is the minimal environment every workflow runs in: no
// inherited search paths or library paths, so two machines trace alike.
// The directories of the tools come first, ahead of the system ones, so
// the tool that resolved is the tool that runs.
func environment(extra []string, tools []string) []string {
	system := []string{"/usr/local/bin", "/usr/bin", "/bin"}
	seen := map[string]bool{}
	for _, d := range system {
		seen[d] = true
	}
	var dirs []string
	for _, t := range tools {
		if p, err := exec.LookPath(t); err == nil {
			if d := filepath.Dir(p); !seen[d] {
				seen[d] = true
				dirs = append(dirs, d)
			}
		}
	}
	env := []string{
		"PATH=" + strings.Join(append(dirs, system...), ":"),
		"HOME=" + os.Getenv("HOME"),
		"USER=" + os.Getenv("USER"),
		"LANG=C",
		"TMPDIR=" + os.TempDir(),
	}
	return append(env, extra...)
}

// execute runs an untraced command in the minimal environment and
// returns its exit status.
func execute(dir string, env, argv []string) (int, error) {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir, cmd.Env = dir, env
	out, err := cmd.CombinedOutput()
	var x *exec.ExitError
	if errors.As(err, &x) {
		return x.ExitCode(), nil
	}
	if err != nil {
		tail := string(out)
		if len(tail) > 2000 {
			tail = tail[len(tail)-2000:]
		}
		return -1, fmt.Errorf("%s: %w\n%s", strings.Join(argv, " "), err, tail)
	}
	return 0, nil
}
