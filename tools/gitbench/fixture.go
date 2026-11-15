package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// fixture is the forge in miniature, built in the directory under test from a
// repository that is only read: a bare origin that serves partial clones and
// takes pushes, and a branch ("bulk") that differs from the default branch in
// a known number of files. Nothing here is ever the real remote.
type fixture struct {
	root   string   // scratch directory holding everything below
	origin string   // bare repository: clone source and push target
	store  string   // bare repository: the shared object cache; "" until ensured
	base   string   // default branch of origin
	paths  []string // regular files at base, in tree order
	cone   string   // top-level directory a sparse checkout keeps
	home   string   // HOME for every git command: no user configuration leaks in
	conf   string   // the one global configuration file
}

// sandbox returns the environment a coding run's git sees: no system or user
// configuration, only the trust a leased workdir needs, and no prompts.
func (f *fixture) sandbox(extra ...string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + f.home,
		"GIT_CONFIG_GLOBAL=" + f.conf,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"LC_ALL=C",
	}
	return append(env, extra...)
}

// git runs one plumbing command in the fixture's environment.
func (f *fixture) git(dir string, stdin []byte, argv ...string) ([]byte, error) {
	cmd := exec.Command("git", argv...)
	cmd.Dir = dir
	cmd.Env = f.sandbox()
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(argv, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.Bytes(), nil
}

// isolate makes the HOME and the one global configuration file a coding run's
// git sees: trust for the leased workdir and nothing else.
func isolate(root string) (home, conf string, err error) {
	home = filepath.Join(root, "home")
	if err = os.MkdirAll(home, 0o755); err != nil {
		return "", "", err
	}
	conf = filepath.Join(home, "gitconfig")
	return home, conf, os.WriteFile(conf, []byte("[safe]\n\tdirectory = *\n"), 0o644)
}

// build copies src into a bare origin under root and adds the bulk branch.
// bulk is how many files that branch changes: four fifths rewritten, a tenth
// deleted, a tenth added.
func build(src, root string, bulk int) (*fixture, error) {
	f := &fixture{root: root}
	var err error
	if f.home, f.conf, err = isolate(root); err != nil {
		return nil, err
	}
	f.origin = filepath.Join(root, "origin.git")
	if _, err := f.git(root, nil, "clone", "--quiet", "--bare", "--no-hardlinks", src, f.origin); err != nil {
		return nil, err
	}
	for _, kv := range [][2]string{{"uploadpack.allowFilter", "true"}, {"uploadpack.allowAnySHA1InWant", "true"}} {
		if _, err := f.git(f.origin, nil, "config", kv[0], kv[1]); err != nil {
			return nil, err
		}
	}
	head, err := f.git(f.origin, nil, "symbolic-ref", "--short", "HEAD")
	if err != nil {
		return nil, err
	}
	f.base = strings.TrimSpace(string(head))
	if err := f.list(); err != nil {
		return nil, err
	}
	if err := f.branch(bulk); err != nil {
		return nil, err
	}
	return f, nil
}

// list reads the tree at the default branch: the regular files and the
// top-level directory whose size is nearest three percent of the tree.
func (f *fixture) list() error {
	out, err := f.git(f.origin, nil, "ls-tree", "-r", "-z", f.base)
	if err != nil {
		return err
	}
	top := map[string]int{}
	for _, rec := range bytes.Split(out, []byte{0}) {
		meta, path, ok := bytes.Cut(rec, []byte{'\t'})
		if !ok {
			continue
		}
		if bytes.HasPrefix(meta, []byte("100644")) && plain(string(path)) {
			f.paths = append(f.paths, string(path))
		}
		if dir, _, ok := strings.Cut(string(path), "/"); ok && !strings.HasPrefix(dir, ".") {
			top[dir]++
		}
	}
	want, best := len(f.paths)*3/100, -1
	dirs := make([]string, 0, len(top))
	for d := range top {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	for _, d := range dirs {
		if gap := abs(top[d] - want); best < 0 || gap < best {
			best, f.cone = gap, d
		}
	}
	return nil
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// plain reports a path fast-import can be given unquoted.
func plain(p string) bool {
	if p == "" || strings.HasPrefix(p, "/") {
		return false
	}
	for _, r := range p {
		if r <= ' ' || r > '~' || r == '"' || r == '\\' {
			return false
		}
	}
	return true
}

// branch writes the bulk branch with git fast-import, straight into the bare
// origin: no checkout of the whole tree is needed to make it, which matters
// when the tree has a hundred thousand files.
func (f *fixture) branch(bulk int) error {
	if bulk > len(f.paths) {
		return fmt.Errorf("bulk %d exceeds the %d regular files in the tree", bulk, len(f.paths))
	}
	del, add := bulk/10, bulk/10
	mod := bulk - del - add
	var s bytes.Buffer
	msg := "bulk change\n"
	fmt.Fprintf(&s, "commit refs/heads/bulk\ncommitter gitbench <gitbench@localhost> 1700000000 +0000\ndata %d\n%sfrom refs/heads/%s^0\n", len(msg), msg, f.base)
	step := len(f.paths) / bulk
	if step < 1 {
		step = 1
	}
	pick := func(i int) string { return f.paths[(i*step)%len(f.paths)] }
	seen := map[string]bool{}
	next := func(i int) string {
		for j := 0; ; j++ {
			p := pick(i + j*bulk)
			if !seen[p] {
				seen[p] = true
				return p
			}
		}
	}
	body := strings.Repeat("rewritten by gitbench\n", 100)
	for i := range mod {
		fmt.Fprintf(&s, "M 100644 inline %s\ndata %d\n%s\n", next(i), len(body), body)
	}
	for i := range del {
		fmt.Fprintf(&s, "D %s\n", next(mod+i))
	}
	for i := range add {
		fmt.Fprintf(&s, "M 100644 inline gitbench-bulk/%05d.txt\ndata %d\n%s\n", i, len(body), body)
	}
	_, err := f.git(f.origin, s.Bytes(), "fast-import", "--quiet")
	return err
}

// cache makes the per-node object store on first use: a bare copy of origin.
func (f *fixture) cache() (string, error) {
	if f.store != "" {
		return f.store, nil
	}
	dir := filepath.Join(f.root, "store.git")
	if _, err := f.git(f.root, nil, "clone", "--quiet", "--bare", "--no-hardlinks", f.origin, dir); err != nil {
		return "", err
	}
	f.store = dir
	return dir, nil
}

// reset puts origin back as the fixture built it, between runs.
func (f *fixture) reset(refs ...string) error {
	for _, r := range refs {
		if _, err := f.git(f.origin, nil, "update-ref", "-d", r); err != nil {
			return err
		}
	}
	return nil
}

// size is the bytes under dir, which is what a volume would have to store.
func size(dir string) (kb int64, files int) {
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				kb += info.Size()
				files++
			}
		}
		return nil
	})
	return (kb + 1023) / 1024, files
}

func itoa(n int) string { return strconv.Itoa(n) }
