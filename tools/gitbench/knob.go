package main

import (
	"fmt"
	"strings"
)

// knob is one named change to how git behaves. Its configuration is given to
// every git command with -c, so the same knob applies to a clone, a switch and
// a push alike and leaves nothing behind in the repository.
type knob struct {
	name   string
	conf   []string // key=value, passed as -c to every command
	env    []string // KEY=value for every command
	clone  []string // extra arguments to clone; {store} is the shared object store
	sparse bool     // clone with --sparse, then narrow to the fixture's cone
	whole  bool     // no partial-clone filter: fetch the tip's blobs with its trees
}

// catalog is every setting the program measures, one change each, so a result
// is attributable. Combinations are written a+b.
var catalog = []knob{
	{name: "default"},
	{name: "fsyncnone", conf: []string{"core.fsync=none"}},
	{name: "fsyncall", conf: []string{"core.fsync=all"}},
	{name: "fsyncloose", conf: []string{"core.fsync=committed"}},
	{name: "objectfiles", conf: []string{"core.fsyncObjectFiles=true"}},
	{name: "batch", conf: []string{"core.fsync=committed", "core.fsyncMethod=batch"}},
	{name: "batchall", conf: []string{"core.fsync=all", "core.fsyncMethod=batch"}},
	{name: "manyfiles", conf: []string{"feature.manyFiles=true"}},
	{name: "index4", conf: []string{"index.version=4"}},
	{name: "split", conf: []string{"core.splitIndex=true"}},
	{name: "untracked", conf: []string{"core.untrackedCache=true"}},
	{name: "monitor", conf: []string{"core.fsmonitor=true"}},
	{name: "preloadoff", conf: []string{"core.preloadIndex=false"}},
	{name: "preloadon", conf: []string{"core.preloadIndex=true"}},
	{name: "workers", conf: []string{"checkout.workers=0"}},
	{name: "workers4", conf: []string{"checkout.workers=4"}},
	{name: "threads1", conf: []string{"pack.threads=1", "index.threads=1"}},
	{name: "threads8", conf: []string{"pack.threads=8", "index.threads=8"}},
	{name: "gcoff", conf: []string{"gc.auto=0", "maintenance.auto=false"}},
	{name: "graph", conf: []string{"fetch.writeCommitGraph=true"}},
	{name: "keeppack", conf: []string{"transfer.unpackLimit=1"}},
	{name: "nozlib", conf: []string{"core.looseCompression=0", "pack.compression=0"}},
	{name: "locks", env: []string{"GIT_OPTIONAL_LOCKS=0"}},
	{name: "checkstat", conf: []string{"core.checkStat=minimal", "core.trustctime=false"}},
	{name: "sparse", sparse: true},
	{name: "whole", whole: true},
	{name: "ref", clone: []string{"--reference-if-able", "{store}"}},
	{name: "refcopy", clone: []string{"--reference-if-able", "{store}", "--dissociate"}},
}

// pick resolves a name such as "fsyncnone+workers+gcoff" to one knob that
// carries all of their settings.
func pick(name string) (knob, error) {
	if name == "" {
		return knob{}, fmt.Errorf("empty config name")
	}
	out := knob{name: name}
	for part := range strings.SplitSeq(name, "+") {
		found := false
		for _, k := range catalog {
			if k.name != part {
				continue
			}
			out.conf = append(out.conf, k.conf...)
			out.env = append(out.env, k.env...)
			out.clone = append(out.clone, k.clone...)
			out.sparse = out.sparse || k.sparse
			out.whole = out.whole || k.whole
			found = true
			break
		}
		if !found {
			return knob{}, fmt.Errorf("unknown config %q", part)
		}
	}
	return out, nil
}

// names expands a comma list; "all" is the whole catalog, one knob each.
func names(list string) ([]knob, error) {
	var out []knob
	for _, n := range strings.Split(list, ",") {
		n = strings.TrimSpace(n)
		if n == "all" {
			out = append(out, catalog...)
			continue
		}
		k, err := pick(n)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, nil
}
