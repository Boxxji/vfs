# Prefetch study: does a coding agent's file access repeat?

Refs #17, #18, #19. The tool is `tools/iotrace`; this file is the study, its
tables, the verdict, and the interface a prefetcher needs from the engine.

A coding run is repeatable work: the same checkout, install, build or search
over the same files, again and again, in a fresh sandbox. If the second run
reads what the first read, a node can fetch it before it is asked for, and
the cold-fetch latency that an object store adds to every first read stops
costing wall time. The study measures whether that is so: eight workflows, two
projects of each, two traced runs of each (thirty-two runs), on a host with
local NVMe. Every number below is a count from those traces or a simulation
over them, and says which.

## Verdict

**Prefetch pays, for every workflow measured, and it needs a trace per
workflow, not a shared one.** In short:

1. **The set repeats.** Of the pages run 2 read from the disk it started with,
   92.5% to 100% had been read by run 1 (97.2% or more in fifteen of the
   sixteen project pairs); 98.0% to 100% of the files. A whole-trace replay
   wastes at most 6% of what it fetches.
2. **The order does not, for parallel work.** The share of run 2's consecutive
   extent pairs that run 1 also made is 0.2 to 0.6% for a tree search, 5.0 to
   8.8% for Go builds, tests and the agent mix, but 57 to 91% for package
   installs, Python sync and the Rust check. A prefetcher that follows the
   previous order loses the parallel workflows (an earlier cursor-following
   design hit 22% on a Go build); one that walks the list under a byte budget
   and skips what has been read hits 98.6%.
3. **Cold hit rates.** On a node with an empty cache, trace prefetch serves
   83% to 99% of extent lookups from the cache (tree search 83%, install 88%,
   Python sync 90%, checkout 96%, agent mix 97%, Rust check 98.5%, Go build
   and test 98.6%); an LRU alone serves 0.1% to 14%.
4. **Stall.** With a cold extent costing 5 ms, the time reads wait for data,
   summed over threads, falls from 1.9 to 733 s to 0.02 to 119 s: 6 times for
   the tree search up to 180 times for the Rust check, and 30 to 80 times for
   the Go and agent workflows (297 s to 3.9 s for a Go build). The busiest
   thread, a floor for the added wall time, falls from 1.0 to 43 s to 0.01 to
   17 s.
5. **The local disk is not what compile work waits for.** Cold against warm
   page cache on NVMe, the tree search spends 93 to 94% of its cold wall time
   on the disk, a checkout 63 to 70%, Python sync 42 to 45%. For Go, Rust and
   the agent mix a single cold/warm pair scatters from -73% to +28% on the
   loaded host, so no disk share can be read from it, and the Node install
   (-14% and +59%) is the same. The case for prefetch in those is not that
   NVMe is slow but that an object store's first read is 1 to 20 ms, and a
   build makes 60 000 of them.
6. **Cache size.** The working set (4 KiB pages touched, mapped ranges
   included as an upper bound) is 60 to 420 MiB for checkout, installs, Python
   sync, builds and the agent mix, 0.8 to 0.95 GiB for the Rust check (80 to
   130 MiB of it read by calls), and 0.9 to 1.4 GiB for tree searches over
   50 000 to 67 000 files. A node cache of 4 GiB serves a repeat on the same
   node at 98% to 100% of lookups in all eight; 512 MiB gives 99.5% to 100% for
   checkout, install and Python sync, about 51% for Go and the agent mix, and
   2.5 to 3% for Rust and tree searches, whose working sets are larger than
   the cache and which a least-recently-used cache loses entirely. Trace
   prefetch needs the cache only for the unread window: 512 MiB with it is
   within 0 to 14 points of 4 GiB (Go build 85% against 99%, tree search 73%
   against 83%).
7. **Warm sets for the base image.** Files that two projects of one toolchain
   both read, outside the workspace, total 532 MiB over the eight workflows:
   343 MiB for Rust, 127 MiB for Node, 69 to 79 MiB for Go, 20 MiB for Python,
   10 MiB for Git, 7 MiB for the search tool. They are 17 to 28% of the bytes a
   Go, agent or Git run reads, 38 to 46% for Rust, 53 to 78% for installs and
   91% for Python sync, and under 1% for a tree search. Preloading only that
   set lifts the cold hit rate by 8 to 20 points (48 for the install, 0 for the
   tree search) and fetches less than the same cache without it.
8. **A trace from a different project does harm.** It hits 0.7 to 21% (what
   the two projects share; the install, 70%, shares the Node binary) and
   wastes 12 to 513 MiB, up to the whole unread budget. Traces belong to the
   volume and the workflow key; a base image shares content, not traces.
9. **Read-ahead alone is marginal.** It moves the cold hit rate by 0.3 points or
   less in seven workflows (14 for checkout), because sequential runs are
   short: the median run is 0 to 4 KiB, the 99th percentile 3 to 271 KiB. It
   mostly converts misses into waits for a fetch in flight. Many lookups meet
   an extent that is already being fetched (20% for a tree search, 10% for an
   install), so singleflight matters more than read-ahead.
10. **Whole-extent fetches amplify partial reads.** Go builds touch a part of
   many large files: fetching 1 MiB extents moves 1.8 to 2.2 times the bytes
   needed for the Go build, test and agent mix, 256 KiB extents 1.6 to 1.8
   times; installs, Python, search and Rust are within 10% of 1.0. A sub-chunk
   range read (#17) is worth having for Go.

What does not pay: the first run, which has no trace (the warm set and the
cache help it by 8 to 20 points, no more); and a multi-gigabyte tree search,
where even with prefetch the wall time stays seconds to minutes above local
because the link, not the latency, is the limit: a 1 GiB unread budget lifts
its hit rate from 83% to 96% and cuts its stall from 119 s to 28 s, and a
cache that already holds the tree from 119 s to 15 s.

### How far to trust the figures

- *The simulation is a bound, not a prediction.* Reads happen at the instants
  the local run made them; a stalled program would run longer. The stall is
  given as the sum over threads (what a single-threaded program adds) and the
  busiest thread's own (what the wall time must grow by at least). The truth
  is between them.
- *Mapped files and executables are an upper bound* (strace sees the mapping,
  not the page faults). The `reads` rows in the sensitivity table drop them:
  hit rates stay at 83% to 99.9%, and amplification rises because the
  denominator shrinks.
- *Timings come from a loaded shared host.* Cold-against-warm differences of a
  few percent, or negative ones, mean "not measurable", not "negative".
- *Extents are per file.* The merged-request row assumes entries adjacent in
  the trace are adjacent in a pack; the study cannot check that.
- *One run pair per project.* The order and set figures would tighten with more
  runs; a Git checkout, run unsettled, read up to six times as many working-tree
  files in one run as in the other (the index entries of the last second of the
  checkout are "racily clean" and Git rereads them, how many depends on how fast
  the previous command ran). The checkout workflow therefore settles the
  workspace first.

### What was measured, in one table

| workflow | working set MiB per project, reads / with maps | run 2 pages already read by run 1 % | cold node LRU hit % | trace prefetch hit % | same node 512 MiB hit % | stall s, no cache | stall s, trace prefetch | disk share of cold wall % |
|---|---|---|---|---|---|---|---|---|
| agent | 164.0 / 347.1 | 99.7 | 12.4 | 96.7 | 52.7 | 297.50 / 10.13 | 9.88 / 2.03 | -43.3 |
| cargocheck | 104.8 / 864.7 | 100.0 | 2.3 | 98.5 | 2.5 | 49.09 / 4.92 | 0.27 / 0.10 | -0.3 |
| checkout | 0.1 / 60.2 | 100.0 | 8.3 | 96.4 | 100.0 | 1.94 / 1.00 | 0.02 / 0.01 | 68.0 |
| gobuild | 162.2 / 253.6 | 99.5 | 13.0 | 98.6 | 51.3 | 297.43 / 10.36 | 3.89 / 0.20 | 19.6 |
| gotest | 164.5 / 271.2 | 99.4 | 13.7 | 98.7 | 51.2 | 301.03 / 11.56 | 3.71 / 0.17 | -9.1 |
| npm | 84.4 / 207.1 | 100.0 | 0.2 | 87.9 | 100.0 | 16.91 / 7.04 | 0.99 / 0.79 | 31.5 |
| ripgrep | 1153.0 / 1160.1 | 94.4 | 0.7 | 83.2 | 3.3 | 732.55 / 43.13 | 119.22 / 17.08 | 93.5 |
| uvsync | 7.0 / 71.9 | 100.0 | 0.1 | 89.5 | 99.5 | 5.14 / 1.53 | 0.15 / 0.06 | 43.1 |

Hit rates are shares of extent lookups; stall is seconds of waiting at 5 ms per cold extent, summed over threads / the busiest thread. Local wall times and all other columns are in the tables below.

## Method

### The tool

`tools/iotrace` records the file accesses of a command and analyses them. It is
Go, standard library only, and needs no privilege.

- **Recording.** `strace --seccomp-bpf -f -ttt -T -y -s 0` with a filter on the
  system calls that name a file or move its bytes. The seccomp filter stops the
  traced program only on those calls, so everything else runs at full speed.
  Tracing slows a run by up to ten times (the dataset table gives each run's
  factor), so wall times come from untraced runs; durations taken from a traced
  run are labelled as such. Kernel tracing (perf, eBPF) needs
  privileges the lab host does not grant an ordinary user; `ptrace` needs none.
- **Normalising.** Every call becomes an event with the columns
  `workflow run t_ns dur_ns tid op path offset length aux`, plus a file of
  sizes. The tracker follows what strace does not print: the position of each
  file description (shared across `dup` and `fork`, advanced by `read` and
  `write`, set by `lseek`, ignored by `pread`), the descriptor table of each
  thread (shared when `CLONE_FILES` is set; read from a first pass over the
  output because a child can log before its parent's `clone` returns), the
  directory descriptors of relative paths, and the sizes in `stat` results.
  Pipes, sockets and `/proc`, `/sys`, `/dev` are dropped. A `mmap` or `exec` of
  a file counts as touching its whole mapped range, because strace cannot see
  which pages fault in: that is an upper bound, and every table that depends on
  it says so or shows the reads-only figure beside it.
- **Base reads.** The analysis separates *base* reads, of data that existed
  before the run, from reads of data the run wrote itself. It follows
  `rename`, `link`, `unlink` and truncation so a file keeps its identity, and a
  byte range written by the run is never counted as something a prefetcher
  could have fetched. Build outputs written and read back by the same run
  (many GiB for the builds) therefore do not count as disk traffic.
- **Cold starts.** Before every run the workspace is rebuilt at the same path,
  build caches start empty, package caches are copied from a populated one, and
  the clean page-cache pages of every file the previous run read are dropped
  with `posix_fadvise(DONTNEED)`, which needs no privilege. Both runs of a
  workflow therefore start from the same disk and the same cold cache, and
  differ only in what the program does.
- **Timing.** After the traced runs, the command is timed untraced, cold and
  then warm (straight after): three pairs for the search, install and Python
  sync, five for checkout, one for Go, Rust and the agent mix, whose runs take
  minutes. The report gives the median and the range. The difference between cold and warm is the wall time the local
  disk costs. The host is shared; its load average is recorded in each manifest
  and appears in the dataset table.

### Workflows

Each ran twice on a fresh state, for two different projects of the same
toolchain (labels `a` and `b`). Package installs run from a populated package
cache (`--offline`, lifecycle scripts off), because a cold install reads nothing
from disk and so has nothing to prefetch.

| workflow | command | projects |
|---|---|---|
| `checkout` | `git checkout <commit>` between two commits | repositories with 3.9k and 2.2k files changed |
| `npm` | `npm ci --offline --ignore-scripts` | projects with 213 and 842 locked packages |
| `uvsync` | `uv sync --offline --frozen` | two small Python projects |
| `gobuild` | `go build ./...`, empty build cache, populated module cache | modules of 413 and 246 source files |
| `gotest` | `go test -count=1 ./...`, same | the same modules |
| `cargocheck` | `cargo check --offline --locked --all-targets`, empty target | crates with 242 and 315 locked packages |
| `ripgrep` | `rg -c` over a tree, hidden and ignored files included | trees of 67k and 51k files |
| `agent` | find, cat of 40 files, `git status`, an edit, `go build ./...`, `git diff` | the two Go modules |

`pnpm install` was not run; the Node workflow is `npm ci`.

### The simulator

The simulator replays a run's base reads against a cache. Its choices are the
ones a vfs engine would face and are stated so the tables can be read without
the code.

1. *The guest page cache comes first.* A page the run has read stays in a 1 GiB
   LRU of 4 KiB pages; only the pages it misses reach the engine, grouped into
   one request per read call. Without it a repeated read would be counted as
   engine traffic that no engine sees.
2. *The unit is a fixed extent* of 1 MiB of one file (256 KiB and 4 MiB in the
   sensitivity table). A small file is one extent of its own size. The engine's
   chunks are content-defined and average about 580 KiB, so this is the right
   order of size, not the same thing.
3. *A cold fetch costs a fixed latency*, 1, 5 or 20 ms, plus its bytes over a
   link of 750 MB/s shared by all fetches (a sensitivity row varies it from
   250 MB/s to unlimited). A demand fetch goes ahead of queued prefetches and
   delays them.
4. *Time is the trace's own.* Reads happen at the instants the local run made
   them, and a stall does not delay later reads. A stalled program would really
   run longer, which would give a prefetcher more time. The stall figures are
   therefore a bound on what misses cost, not the wall time of a run.
5. *Stall.* The time reads waited, reported two ways: summed over threads (what
   a single-threaded program would add to its wall time) and the busiest
   thread's own (what the wall time must grow by at least).
6. *Policies.* None; LRU of 64 MiB, 512 MiB, 4 GiB; sequential read-ahead
   (starts after two consecutive extents of a file, doubles to 8); trace
   prefetch; and each of these on a node that ran the previous run just before.
   Trace prefetch fetches the extents of the previous run in the order it first
   touched them, 64 at a time, while no more than 256 MiB of what it fetched is
   unread, and skips what the run has read. It has no cursor: an earlier design
   that followed the previous order and jumped when the run ran ahead lost the
   parallel workflows, whose order differs from run to run (a Go build hit
   22% with it and 98.6% without).
7. *Amplification* is bytes fetched over the bytes the run needed, by 4 KiB
   page. *Wasted* is prefetched and never read.

### What the study cannot say

- It sees system calls, not page faults: mapped files are an upper bound, shown
  separately. Git, which maps packs, is the case where it matters most; the
  checkout runs map a pack in 1 MiB windows (`core.packedGitWindowSize`) so
  that the mapped range is nearer the touched range.
- Kernel read-ahead and FUSE request sizes are not modelled.
- Fetching is per extent of one file; vfs packs hold many chunks, and fetching a
  pack range serves many small files at once. The merged-request row assumes
  entries adjacent in the trace are adjacent in a pack, which this study cannot
  check.
- The host is an aarch64 box with 20 cores shared with other jobs; absolute
  times are not those of a sandbox. Counts, bytes and rates of repetition do
  not depend on the host.

## Tables

Generated by `iotrace report`. Each table says what it counts; the method section says how. Sample sizes are in the (n) columns: runs, files, pages, extents, read calls, transitions.

### Dataset

| workflow | run | events | files read | MiB read (pages, with maps) | traced s | untraced cold s | tracing slowdown | load before |
|---|---|---|---|---|---|---|---|---|
| agent | a1 | 850517 | 28865 | 290.2 | 67.48 | 33.19 | 2.03x | 10.84 16.97 24.83 |
| agent | a2 | 849441 | 28821 | 276.4 | 114.12 | 33.19 | 3.44x | 41.61 24.49 26.77 |
| agent | b1 | 1337792 | 22448 | 418.5 | 218.43 | 67.79 | 3.22x | 11.92 21.41 25.46 |
| agent | b2 | 1336392 | 22267 | 417.8 | 218.91 | 67.79 | 3.23x | 33.07 23.90 25.13 |
| cargocheck | a1 | 592614 | 3377 | 781.6 | 97.90 | 16.16 | 6.06x | 22.61 29.65 27.98 |
| cargocheck | a2 | 592495 | 3377 | 781.6 | 83.70 | 16.16 | 5.18x | 6.52 21.70 25.29 |
| cargocheck | b1 | 1921135 | 6051 | 947.8 | 249.25 | 113.11 | 2.20x | 22.61 29.65 27.98 |
| cargocheck | b2 | 1920948 | 6051 | 947.8 | 322.30 | 113.11 | 2.85x | 19.83 18.47 23.33 |
| checkout | a1 | 30017 | 146 | 61.4 | 2.56 | 1.19 | 2.15x | 17.89 27.45 30.61 |
| checkout | a2 | 30017 | 146 | 61.4 | 3.06 | 1.19 | 2.57x | 17.82 27.27 30.54 |
| checkout | b1 | 13293 | 130 | 59.0 | 3.03 | 0.50 | 6.03x | 22.91 26.92 30.22 |
| checkout | b2 | 13293 | 130 | 59.0 | 2.97 | 0.50 | 5.92x | 25.22 27.29 30.31 |
| gobuild | a1 | 1310691 | 21971 | 246.9 | 195.79 | 79.63 | 2.46x | 22.39 19.05 25.30 |
| gobuild | a2 | 1310742 | 22047 | 247.3 | 213.89 | 79.63 | 2.69x | 11.92 21.41 25.46 |
| gobuild | b1 | 843440 | 28638 | 260.0 | 62.13 | 87.00 | 0.71x | 35.18 30.89 27.78 |
| gobuild | b2 | 843419 | 28626 | 259.9 | 101.62 | 87.00 | 1.17x | 15.36 25.84 26.28 |
| gotest | a1 | 7012657 | 22153 | 258.6 | 666.58 | 193.30 | 3.45x | 22.39 19.05 25.30 |
| gotest | a2 | 7014045 | 22158 | 258.6 | 675.00 | 193.30 | 3.49x | 12.26 24.12 25.69 |
| gotest | b1 | 1451491 | 28827 | 284.1 | 210.59 | 104.57 | 2.01x | 43.28 28.80 26.46 |
| gotest | b2 | 1451398 | 28737 | 283.9 | 202.83 | 104.57 | 1.94x | 16.06 26.62 26.52 |
| npm | a1 | 71736 | 1387 | 246.8 | 9.22 | 4.31 | 2.14x | 50.85 34.85 26.52 |
| npm | a2 | 71735 | 1387 | 246.8 | 9.41 | 4.31 | 2.18x | 45.48 34.23 26.41 |
| npm | b1 | 74086 | 1626 | 167.5 | 27.56 | 7.20 | 3.83x | 38.24 33.80 26.63 |
| npm | b2 | 74090 | 1626 | 167.5 | 15.07 | 7.20 | 2.09x | 34.12 33.45 26.82 |
| ripgrep | a1 | 246023 | 65858 | 945.2 | 35.01 | 3.65 | 9.59x | 6.53 20.05 21.11 |
| ripgrep | a2 | 246046 | 65586 | 930.8 | 30.55 | 3.65 | 8.37x | 6.61 17.94 20.33 |
| ripgrep | b1 | 195238 | 50149 | 1367.1 | 32.11 | 4.01 | 8.01x | 24.54 20.78 21.15 |
| ripgrep | b2 | 195272 | 50137 | 1389.5 | 26.66 | 4.01 | 6.65x | 40.76 25.40 22.69 |
| uvsync | a1 | 18754 | 439 | 72.0 | 4.67 | 1.92 | 2.44x | 53.08 32.29 25.26 |
| uvsync | a2 | 18750 | 438 | 72.0 | 8.87 | 1.92 | 4.63x | 50.59 32.12 25.24 |
| uvsync | b1 | 13883 | 568 | 71.9 | 5.00 | 1.43 | 3.48x | 49.73 33.34 25.83 |
| uvsync | b2 | 13882 | 568 | 71.9 | 4.48 | 1.43 | 3.13x | 47.11 33.07 25.78 |

### Predictability: what run 2 reads that run 1 of the same workflow read

| workflow | project | view | files (n) | files % | pages (n) | pages % | 1 MiB extents (n) | 1 MiB % | 4 MiB extents (n) | 4 MiB % | run 1 pages used by run 2 % | paths touched (n) | paths % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| agent | a | reads | 28794 | 99.0 | 43885 | 99.3 | 28798 | 99.0 | 28794 | 99.0 | 99.2 | 63675 | 91.5 |
| agent | a | reads+maps | 28821 | 99.0 | 70763 | 99.6 | 28914 | 99.0 | 28839 | 99.0 | 94.8 | 63675 | 91.5 |
| agent | b | reads | 22178 | 99.2 | 40092 | 99.6 | 22199 | 99.2 | 22184 | 99.2 | 99.1 | 46023 | 89.0 |
| agent | b | reads+maps | 22267 | 99.2 | 106964 | 99.8 | 22525 | 99.2 | 22325 | 99.2 | 99.7 | 46023 | 89.0 |
| cargocheck | a | reads | 3340 | 100.0 | 20206 | 100.0 | 3369 | 100.0 | 3348 | 100.0 | 100.0 | 10948 | 88.7 |
| cargocheck | a | reads+maps | 3377 | 100.0 | 200081 | 100.0 | 4076 | 100.0 | 3539 | 100.0 | 100.0 | 10948 | 88.7 |
| cargocheck | b | reads | 6016 | 100.0 | 33471 | 100.0 | 6043 | 100.0 | 6022 | 100.0 | 100.0 | 20568 | 88.5 |
| cargocheck | b | reads+maps | 6051 | 100.0 | 242626 | 100.0 | 6865 | 100.0 | 6242 | 100.0 | 100.0 | 20568 | 88.5 |
| checkout | a | reads | 24 | 100.0 | 26 | 100.0 | 26 | 100.0 | 26 | 100.0 | 100.0 | 13152 | 100.0 |
| checkout | a | reads+maps | 146 | 100.0 | 15729 | 100.0 | 203 | 100.0 | 158 | 100.0 | 100.0 | 13152 | 100.0 |
| checkout | b | reads | 22 | 100.0 | 26 | 100.0 | 23 | 100.0 | 23 | 100.0 | 100.0 | 3761 | 100.0 |
| checkout | b | reads+maps | 130 | 100.0 | 15111 | 100.0 | 188 | 100.0 | 144 | 100.0 | 100.0 | 3761 | 100.0 |
| gobuild | a | reads | 22036 | 98.7 | 39506 | 99.3 | 22053 | 98.7 | 22039 | 98.7 | 99.5 | 45199 | 89.1 |
| gobuild | a | reads+maps | 22047 | 98.7 | 63298 | 99.5 | 22147 | 98.7 | 22068 | 98.7 | 99.7 | 45199 | 89.1 |
| gobuild | b | reads | 28615 | 98.8 | 43516 | 99.2 | 28618 | 98.8 | 28615 | 98.8 | 99.1 | 63214 | 91.7 |
| gobuild | b | reads+maps | 28626 | 98.8 | 66529 | 99.5 | 28709 | 98.8 | 28643 | 98.8 | 99.4 | 63214 | 91.7 |
| gotest | a | reads | 22146 | 98.6 | 40304 | 99.2 | 22163 | 98.6 | 22149 | 98.6 | 99.2 | 252130 | 17.3 |
| gotest | a | reads+maps | 22158 | 98.6 | 66204 | 99.5 | 22266 | 98.6 | 22181 | 98.6 | 99.5 | 252130 | 17.3 |
| gotest | b | reads | 28722 | 99.2 | 43926 | 99.4 | 28725 | 99.2 | 28722 | 99.2 | 99.1 | 74330 | 84.9 |
| gotest | b | reads+maps | 28737 | 99.2 | 72673 | 99.3 | 28840 | 99.2 | 28758 | 99.2 | 99.2 | 74330 | 84.9 |
| npm | a | reads | 1384 | 100.0 | 31758 | 100.0 | 1489 | 100.0 | 1407 | 100.0 | 100.0 | 9995 | 100.0 |
| npm | a | reads+maps | 1387 | 100.0 | 63179 | 100.0 | 1612 | 100.0 | 1439 | 100.0 | 100.0 | 9995 | 100.0 |
| npm | b | reads | 1623 | 100.0 | 11458 | 100.0 | 1648 | 100.0 | 1627 | 100.0 | 100.0 | 13317 | 100.0 |
| npm | b | reads+maps | 1626 | 100.0 | 42879 | 100.0 | 1771 | 100.0 | 1659 | 100.0 | 100.0 | 13317 | 100.0 |
| ripgrep | a | reads | 65584 | 98.0 | 236448 | 97.2 | 65840 | 98.0 | 65610 | 98.0 | 95.6 | 82305 | 100.0 |
| ripgrep | a | reads+maps | 65586 | 98.0 | 238273 | 97.2 | 65847 | 98.0 | 65613 | 98.0 | 95.7 | 82305 | 100.0 |
| ripgrep | b | reads | 50135 | 98.0 | 353890 | 92.5 | 50717 | 97.8 | 50213 | 97.9 | 94.0 | 60628 | 100.0 |
| ripgrep | b | reads+maps | 50137 | 98.0 | 355715 | 92.5 | 50724 | 97.8 | 50216 | 97.9 | 94.1 | 60628 | 100.0 |
| uvsync | a | reads | 432 | 99.3 | 1812 | 99.8 | 432 | 99.3 | 432 | 99.3 | 99.8 | 16011 | 92.5 |
| uvsync | a | reads+maps | 438 | 99.3 | 18433 | 100.0 | 500 | 99.4 | 453 | 99.3 | 100.0 | 16011 | 92.5 |
| uvsync | b | reads | 562 | 99.5 | 1774 | 99.8 | 562 | 99.5 | 562 | 99.5 | 99.8 | 10525 | 92.6 |
| uvsync | b | reads+maps | 568 | 99.5 | 18395 | 100.0 | 630 | 99.5 | 583 | 99.5 | 100.0 | 10525 | 92.6 |

### What run 2 read that run 1 did not, by class of path (4 KiB pages, with maps)

| workflow | project | class | MiB run 2 read | MiB run 1 did not | % |
|---|---|---|---|---|---|
| agent | a | cache | 204.6 | 1.2 | 0.6 |
| agent | b | cache | 201.2 | 0.7 | 0.3 |
| gobuild | a | build | 0.1 | 0.1 | 100.0 |
| gobuild | a | cache | 186.7 | 1.1 | 0.6 |
| gobuild | b | cache | 204.5 | 1.4 | 0.7 |
| gotest | a | build | 0.0 | 0.0 | 100.0 |
| gotest | a | cache | 194.8 | 1.2 | 0.6 |
| gotest | b | build | 1.0 | 1.0 | 100.0 |
| gotest | b | cache | 226.4 | 0.9 | 0.4 |
| ripgrep | a | workspace | 923.6 | 26.3 | 2.8 |
| ripgrep | b | workspace | 1382.4 | 103.7 | 7.5 |
| uvsync | a | cache | 4.0 | 0.0 | 0.3 |
| uvsync | b | cache | 3.3 | 0.0 | 0.5 |

### Order: first K distinct 1 MiB extents of run 1 against run 2 (reach % / same-prefix %)

| workflow | project | K=10 | K=100 | K=1000 | K=10000 | run 2 transitions also in run 1 % | transitions (n) |
|---|---|---|---|---|---|---|---|
| agent | a | 100 / 80 | 100 / 100 | 98 / 79 | 99 / 95 | 5.5 | 28913 |
| agent | b | 100 / 90 | 100 / 100 | 100 / 100 | 98 / 90 | 8.8 | 22524 |
| cargocheck | a | 100 / 100 | 100 / 100 | 100 / 99 | 100 / 100 | 74.5 | 4075 |
| cargocheck | b | 100 / 100 | 100 / 100 | 100 / 100 | 100 / 100 | 67.6 | 6864 |
| checkout | a | 100 / 100 | 100 / 100 | 100 / 100 | 100 / 100 | 100.0 | 202 |
| checkout | b | 100 / 100 | 100 / 100 | 100 / 100 | 100 / 100 | 100.0 | 187 |
| gobuild | a | 100 / 100 | 100 / 100 | 100 / 90 | 99 / 95 | 5.5 | 22146 |
| gobuild | b | 100 / 100 | 100 / 100 | 99 / 73 | 99 / 93 | 5.0 | 28708 |
| gotest | a | 100 / 100 | 100 / 100 | 100 / 89 | 98 / 93 | 7.4 | 22265 |
| gotest | b | 100 / 100 | 100 / 86 | 99 / 83 | 99 / 95 | 6.0 | 28839 |
| npm | a | 100 / 100 | 100 / 100 | 100 / 100 | 100 / 100 | 91.1 | 1611 |
| npm | b | 100 / 100 | 100 / 100 | 100 / 100 | 100 / 100 | 78.7 | 1770 |
| ripgrep | a | 100 / 100 | 100 / 50 | 98 / 73 | 98 / 94 | 0.2 | 65846 |
| ripgrep | b | 100 / 100 | 99 / 74 | 99 / 90 | 98 / 66 | 0.6 | 50723 |
| uvsync | a | 100 / 100 | 100 / 100 | 99 / 99 | 99 / 99 | 89.2 | 499 |
| uvsync | b | 100 / 100 | 100 / 100 | 100 / 100 | 100 / 100 | 56.9 | 629 |

### Reuse across projects: bytes (4 KiB pages) run 1 of one project reads that run 1 of the other also reads

| workflow | project | class | MiB read | MiB shared | % of class shared | % of all bytes |
|---|---|---|---|---|---|---|
| agent | b from a | cache | 201.9 | 15.2 | 7.5 | 3.6 |
| agent | b from a | objects | 141.9 | 0.0 | 0.0 | 0.0 |
| agent | b from a | other | 0.0 | 0.0 | 100.0 | 0.0 |
| agent | b from a | system | 63.6 | 63.3 | 99.5 | 15.1 |
| agent | b from a | toolchain | 0.0 | 0.0 | 100.0 | 0.0 |
| agent | b from a | workspace | 11.1 | 0.0 | 0.0 | 0.0 |
| agent | b from a | **all** | 418.5 | 78.5 | 18.8 | 100.0 |
| agent | a from b | build | 0.0 | 0.0 | 0.0 | 0.0 |
| agent | a from b | cache | 218.4 | 15.2 | 7.0 | 5.2 |
| agent | a from b | objects | 4.6 | 0.0 | 0.0 | 0.0 |
| agent | a from b | other | 0.0 | 0.0 | 100.0 | 0.0 |
| agent | a from b | system | 63.3 | 63.3 | 100.0 | 21.8 |
| agent | a from b | toolchain | 0.0 | 0.0 | 100.0 | 0.0 |
| agent | a from b | workspace | 3.8 | 0.0 | 0.0 | 0.0 |
| agent | a from b | **all** | 290.2 | 78.5 | 27.1 | 100.0 |
| cargocheck | b from a | cache | 112.7 | 25.5 | 22.6 | 2.7 |
| cargocheck | b from a | other | 19.4 | 19.3 | 99.6 | 2.0 |
| cargocheck | b from a | system | 320.2 | 317.9 | 99.3 | 33.5 |
| cargocheck | b from a | toolchain | 494.5 | 0.0 | 0.0 | 0.0 |
| cargocheck | b from a | workspace | 1.1 | 0.0 | 0.0 | 0.0 |
| cargocheck | b from a | **all** | 947.8 | 362.6 | 38.3 | 100.0 |
| cargocheck | a from b | cache | 62.2 | 25.5 | 40.9 | 3.3 |
| cargocheck | a from b | other | 400.4 | 19.3 | 4.8 | 2.5 |
| cargocheck | a from b | system | 317.9 | 317.9 | 100.0 | 40.7 |
| cargocheck | a from b | workspace | 1.0 | 0.0 | 0.0 | 0.0 |
| cargocheck | a from b | **all** | 781.6 | 362.6 | 46.4 | 100.0 |
| checkout | b from a | objects | 48.6 | 0.0 | 0.0 | 0.0 |
| checkout | b from a | other | 0.0 | 0.0 | 100.0 | 0.0 |
| checkout | b from a | system | 10.1 | 10.1 | 100.0 | 17.1 |
| checkout | b from a | workspace | 0.3 | 0.0 | 0.0 | 0.0 |
| checkout | b from a | **all** | 59.0 | 10.1 | 17.1 | 100.0 |
| checkout | a from b | objects | 50.3 | 0.0 | 0.0 | 0.0 |
| checkout | a from b | other | 0.0 | 0.0 | 100.0 | 0.0 |
| checkout | a from b | system | 10.1 | 10.1 | 100.0 | 16.4 |
| checkout | a from b | workspace | 1.0 | 0.0 | 0.0 | 0.0 |
| checkout | a from b | **all** | 61.4 | 10.1 | 16.5 | 100.0 |
| gobuild | b from a | build | 0.1 | 0.0 | 0.0 | 0.0 |
| gobuild | b from a | cache | 204.5 | 15.0 | 7.3 | 5.8 |
| gobuild | b from a | other | 0.0 | 0.0 | 100.0 | 0.0 |
| gobuild | b from a | system | 53.6 | 53.6 | 100.0 | 20.6 |
| gobuild | b from a | toolchain | 0.0 | 0.0 | 100.0 | 0.0 |
| gobuild | b from a | workspace | 1.8 | 0.0 | 0.0 | 0.0 |
| gobuild | b from a | **all** | 260.0 | 68.6 | 26.4 | 100.0 |
| gobuild | a from b | cache | 186.4 | 15.0 | 8.1 | 6.1 |
| gobuild | a from b | other | 0.0 | 0.0 | 100.0 | 0.0 |
| gobuild | a from b | system | 53.8 | 53.6 | 99.5 | 21.7 |
| gobuild | a from b | toolchain | 0.0 | 0.0 | 100.0 | 0.0 |
| gobuild | a from b | workspace | 6.6 | 0.0 | 0.0 | 0.0 |
| gobuild | a from b | **all** | 246.9 | 68.6 | 27.8 | 100.0 |
| gotest | b from a | build | 0.7 | 0.0 | 0.0 | 0.0 |
| gotest | b from a | cache | 227.0 | 15.1 | 6.7 | 5.3 |
| gotest | b from a | other | 0.0 | 0.0 | 100.0 | 0.0 |
| gotest | b from a | system | 53.6 | 53.6 | 100.0 | 18.8 |
| gotest | b from a | toolchain | 0.0 | 0.0 | 100.0 | 0.0 |
| gotest | b from a | workspace | 2.9 | 0.0 | 0.0 | 0.0 |
| gotest | b from a | **all** | 284.1 | 68.7 | 24.2 | 100.0 |
| gotest | a from b | build | 0.0 | 0.0 | 0.0 | 0.0 |
| gotest | a from b | cache | 194.7 | 15.1 | 7.8 | 5.8 |
| gotest | a from b | other | 0.0 | 0.0 | 75.0 | 0.0 |
| gotest | a from b | system | 53.9 | 53.6 | 99.3 | 20.7 |
| gotest | a from b | toolchain | 0.0 | 0.0 | 100.0 | 0.0 |
| gotest | a from b | workspace | 9.9 | 0.0 | 0.0 | 0.0 |
| gotest | a from b | **all** | 258.6 | 68.7 | 26.6 | 100.0 |
| npm | b from a | cache | 37.4 | 0.1 | 0.2 | 0.0 |
| npm | b from a | other | 2.6 | 2.6 | 100.0 | 1.5 |
| npm | b from a | system | 127.2 | 127.2 | 100.0 | 75.9 |
| npm | b from a | workspace | 0.3 | 0.0 | 0.0 | 0.0 |
| npm | b from a | **all** | 167.5 | 129.8 | 77.5 | 100.0 |
| npm | a from b | cache | 116.9 | 0.1 | 0.1 | 0.0 |
| npm | a from b | other | 2.6 | 2.6 | 100.0 | 1.0 |
| npm | a from b | system | 127.2 | 127.2 | 100.0 | 51.5 |
| npm | a from b | workspace | 0.1 | 0.0 | 0.0 | 0.0 |
| npm | a from b | **all** | 246.8 | 129.8 | 52.6 | 100.0 |
| ripgrep | b from a | system | 7.1 | 7.1 | 100.0 | 0.5 |
| ripgrep | b from a | workspace | 1360.0 | 0.0 | 0.0 | 0.0 |
| ripgrep | b from a | **all** | 1367.1 | 7.1 | 0.5 | 100.0 |
| ripgrep | a from b | system | 7.1 | 7.1 | 100.0 | 0.8 |
| ripgrep | a from b | workspace | 938.1 | 0.0 | 0.0 | 0.0 |
| ripgrep | a from b | **all** | 945.2 | 7.1 | 0.8 | 100.0 |
| uvsync | b from a | cache | 3.3 | 0.0 | 0.2 | 0.0 |
| uvsync | b from a | other | 45.4 | 45.4 | 100.0 | 63.2 |
| uvsync | b from a | system | 19.7 | 19.7 | 100.0 | 27.4 |
| uvsync | b from a | toolchain | 2.9 | 0.0 | 1.2 | 0.0 |
| uvsync | b from a | workspace | 0.6 | 0.0 | 0.0 | 0.0 |
| uvsync | b from a | **all** | 71.9 | 65.2 | 90.7 | 100.0 |
| uvsync | a from b | cache | 4.0 | 0.0 | 0.2 | 0.0 |
| uvsync | a from b | other | 45.4 | 45.4 | 100.0 | 63.1 |
| uvsync | a from b | system | 19.7 | 19.7 | 100.0 | 27.4 |
| uvsync | a from b | toolchain | 2.8 | 0.0 | 1.2 | 0.0 |
| uvsync | a from b | workspace | 0.1 | 0.0 | 0.0 | 0.0 |
| uvsync | a from b | **all** | 72.0 | 65.2 | 90.5 | 100.0 |

Candidate base-image warm set (toolchain, system and cache files read by both projects, summed over workflows, deduplicated): 531.9 MiB.

| workflow | warm set MiB | files |
|---|---|---|
| agent | 78.5 | 2977 |
| cargocheck | 343.3 | 751 |
| checkout | 10.1 | 8 |
| gobuild | 68.6 | 2918 |
| gotest | 68.7 | 2943 |
| npm | 127.3 | 689 |
| ripgrep | 7.1 | 5 |
| uvsync | 19.7 | 28 |

### Read calls: size and sequentiality (all runs of the workflow)

| workflow | reads (n) | mean B | <=4K calls / bytes % | <=16K | <=64K | <=128K | <=1M | >1M | sequential calls % | sequential bytes % | sequential run KiB p50 / p90 / p99 | runs (n) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| agent | 1759554 | 10079 | 79.1 / 28.8 | 1.5 / 1.3 | 19.4 / 61.7 | 0.0 / 0.1 | 0.0 / 0.8 | 0.0 / 7.2 | 27.0 | 67.7 | 4 / 4 / 55 | 1282415 |
| cargocheck | 1448887 | 7647 | 90.1 / 42.1 | 6.6 / 7.5 | 2.1 / 9.7 | 0.6 / 7.2 | 0.5 / 20.4 | 0.1 / 13.1 | 25.2 | 39.1 | 4 / 8 / 106 | 1083911 |
| checkout | 220 | 715 | 100.0 / 100.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 | 0.0 | 0 / 2 / 3 | 220 |
| gobuild | 1730475 | 10180 | 78.8 / 28.4 | 1.4 / 1.3 | 19.7 / 62.1 | 0.0 / 0.1 | 0.0 / 0.8 | 0.0 / 7.2 | 26.1 | 67.6 | 4 / 4 / 56 | 1276264 |
| gotest | 9837418 | 6521 | 93.1 / 54.4 | 0.7 / 1.0 | 6.0 / 29.6 | 0.1 / 1.1 | 0.1 / 4.5 | 0.0 / 9.4 | 11.5 | 40.1 | 4 / 4 / 16 | 8698888 |
| npm | 9118 | 37521 | 82.1 / 2.9 | 13.4 / 2.6 | 2.8 / 2.5 | 0.5 / 1.4 | 0.8 / 6.9 | 0.4 / 83.7 | 31.5 | 10.4 | 2 / 11 / 271 | 6248 |
| ripgrep | 473779 | 9064 | 86.7 / 4.8 | 8.9 / 7.7 | 3.1 / 10.7 | 0.5 / 4.6 | 0.7 / 26.0 | 0.1 / 46.2 | 50.0 | 97.5 | 1 / 13 / 230 | 231722 |
| uvsync | 4834 | 8337 | 67.7 / 6.5 | 20.3 / 20.7 | 9.6 / 36.9 | 1.8 / 18.2 | 0.6 / 17.7 | 0.0 / 0.0 | 28.5 | 4.7 | 2 / 30 / 117 | 3432 |

### Cache policies on run 2 (cold node unless stated; 1 MiB extents, 1024 MiB guest page cache, 64 prefetches in flight)

hit, late and miss are shares of extent lookups at a 5 ms fetch; amplification is bytes fetched over the bytes the run needed by 4 KiB page; stall is the time reads waited, summed over threads / the busiest thread, for a 1, 5 and 20 ms fetch.

| workflow | policy | reads to engine (n) | hit % | late % | miss % | fetched MiB | amplification | wasted MiB | stall s @1 ms | @5 ms | @20 ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| agent | no cache | 59500 | 0.0 | 0.0 | 100.0 | 2400.8 | 3.46 | 0.0 | 59.81 / 2.03 | 297.50 / 10.13 | 1190.00 / 40.54 |
| agent | LRU 64 MiB | 59500 | 1.1 | 3.8 | 95.1 | 1371.4 | 1.98 | 0.0 | 57.43 / 2.02 | 291.28 / 10.12 | 1179.13 / 40.52 |
| agent | LRU 512 MiB | 59500 | 8.3 | 2.4 | 89.3 | 1302.5 | 1.88 | 0.0 | 53.68 / 2.00 | 270.97 / 10.01 | 1092.69 / 40.10 |
| agent | LRU 4 GiB | 59500 | 12.4 | 1.6 | 86.0 | 1260.6 | 1.82 | 0.0 | 51.58 / 1.96 | 259.33 / 9.83 | 1042.67 / 39.35 |
| agent | LRU 4 GiB + read-ahead | 59500 | 12.6 | 1.9 | 85.5 | 1260.6 | 1.82 | 0.0 | 51.55 / 1.96 | 258.93 / 9.83 | 1038.90 / 39.35 |
| agent | LRU 512 MiB + trace prefetch | 59500 | 86.6 | 0.9 | 12.5 | 1480.3 | 2.13 | 177.6 | 7.61 / 0.66 | 39.21 / 3.32 | 150.43 / 11.70 |
| agent | LRU 4 GiB + trace prefetch | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 25.5 | 1.96 / 0.40 | 9.88 / 2.03 | 39.98 / 8.17 |
| agent | LRU 4 GiB + read-ahead + trace prefetch | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 25.5 | 1.96 / 0.40 | 9.88 / 2.03 | 39.98 / 8.17 |
| agent | LRU 4 GiB + trace prefetch, merged 4 MiB | 59500 | 96.7 | 0.1 | 3.2 | 1286.0 | 1.85 | 25.5 | 1.95 / 0.39 | 9.78 / 1.99 | 39.57 / 8.00 |
| agent | LRU 4 GiB + trace of the other project | 59500 | 18.9 | 1.5 | 79.5 | 1773.3 | 2.55 | 512.7 | 47.74 / 1.79 | 240.04 / 8.98 | 965.43 / 35.95 |
| agent | LRU 4 GiB + base warm set | 59500 | 22.7 | 1.4 | 75.9 | 1063.2 | 1.53 | 0.0 | 45.58 / 1.79 | 229.11 / 8.98 | 921.30 / 35.95 |
| agent | same node, LRU 64 MiB | 59500 | 1.3 | 3.8 | 94.9 | 1369.7 | 1.97 | 0.0 | 57.31 / 2.02 | 290.68 / 10.12 | 1176.73 / 40.52 |
| agent | same node, LRU 512 MiB | 59500 | 52.7 | 1.3 | 46.0 | 737.7 | 1.06 | 0.0 | 27.68 / 1.90 | 139.94 / 9.51 | 564.33 / 38.09 |
| agent | same node, LRU 4 GiB | 59500 | 99.2 | 0.0 | 0.8 | 8.2 | 0.01 | 0.0 | 0.47 / 0.03 | 2.33 / 0.13 | 9.34 / 0.52 |
| cargocheck | no cache | 9819 | 0.0 | 0.0 | 100.0 | 1903.6 | 1.10 | 0.0 | 9.87 / 0.99 | 49.09 / 4.92 | 196.38 / 19.68 |
| cargocheck | LRU 64 MiB | 9819 | 2.3 | 1.1 | 96.5 | 1715.1 | 0.99 | 0.0 | 9.53 / 0.83 | 47.62 / 4.21 | 191.76 / 17.33 |
| cargocheck | LRU 512 MiB | 9819 | 2.3 | 1.1 | 96.5 | 1715.1 | 0.99 | 0.0 | 9.53 / 0.83 | 47.62 / 4.21 | 191.76 / 17.33 |
| cargocheck | LRU 4 GiB | 9819 | 2.3 | 1.1 | 96.5 | 1715.1 | 0.99 | 0.0 | 9.53 / 0.83 | 47.62 / 4.21 | 191.76 / 17.33 |
| cargocheck | LRU 4 GiB + read-ahead | 9819 | 2.3 | 13.8 | 83.9 | 1755.1 | 1.01 | 40.0 | 9.53 / 0.83 | 47.62 / 4.21 | 191.76 / 17.33 |
| cargocheck | LRU 512 MiB + trace prefetch | 9819 | 97.9 | 1.3 | 0.7 | 1718.0 | 0.99 | 2.8 | 0.12 / 0.03 | 0.58 / 0.15 | 1.97 / 0.50 |
| cargocheck | LRU 4 GiB + trace prefetch | 9819 | 98.5 | 1.3 | 0.2 | 1715.1 | 0.99 | 0.0 | 0.05 / 0.02 | 0.27 / 0.10 | 1.03 / 0.38 |
| cargocheck | LRU 4 GiB + read-ahead + trace prefetch | 9819 | 98.5 | 1.3 | 0.2 | 1755.1 | 1.01 | 40.0 | 0.05 / 0.02 | 0.27 / 0.10 | 1.08 / 0.38 |
| cargocheck | LRU 4 GiB + trace prefetch, merged 4 MiB | 9819 | 98.5 | 1.4 | 0.2 | 1715.1 | 0.99 | 0.0 | 0.05 / 0.02 | 0.27 / 0.10 | 1.05 / 0.38 |
| cargocheck | LRU 4 GiB + trace of the other project | 9819 | 7.1 | 1.5 | 91.4 | 2228.3 | 1.29 | 513.1 | 9.03 / 0.71 | 45.05 / 3.54 | 180.43 / 14.16 |
| cargocheck | LRU 4 GiB + base warm set | 9819 | 22.1 | 0.2 | 77.8 | 1029.1 | 0.60 | 0.0 | 7.96 / 0.71 | 39.71 / 3.54 | 158.94 / 14.16 |
| cargocheck | same node, LRU 64 MiB | 9819 | 2.3 | 1.1 | 96.5 | 1715.1 | 0.99 | 0.0 | 9.53 / 0.83 | 47.62 / 4.21 | 191.76 / 17.33 |
| cargocheck | same node, LRU 512 MiB | 9819 | 2.5 | 1.1 | 96.4 | 1711.8 | 0.99 | 0.0 | 9.51 / 0.83 | 47.52 / 4.21 | 191.38 / 17.33 |
| cargocheck | same node, LRU 4 GiB | 9819 | 100.0 | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| checkout | no cache | 388 | 0.0 | 0.0 | 100.0 | 176.6 | 1.47 | 0.0 | 0.43 / 0.22 | 1.94 / 1.00 | 7.76 / 4.02 |
| checkout | LRU 64 MiB | 388 | 8.3 | 3.6 | 88.1 | 129.7 | 1.08 | 0.0 | 0.40 / 0.21 | 1.83 / 0.96 | 7.38 / 3.85 |
| checkout | LRU 512 MiB | 388 | 8.3 | 3.6 | 88.1 | 129.7 | 1.08 | 0.0 | 0.40 / 0.21 | 1.83 / 0.96 | 7.38 / 3.85 |
| checkout | LRU 4 GiB | 388 | 8.3 | 3.6 | 88.1 | 129.7 | 1.08 | 0.0 | 0.40 / 0.21 | 1.83 / 0.96 | 7.38 / 3.85 |
| checkout | LRU 4 GiB + read-ahead | 388 | 22.1 | 6.1 | 71.8 | 141.7 | 1.18 | 12.0 | 0.32 / 0.17 | 1.56 / 0.80 | 6.31 / 3.21 |
| checkout | LRU 512 MiB + trace prefetch | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.0 | 0.01 / 0.01 | 0.02 / 0.01 | 0.19 / 0.15 |
| checkout | LRU 4 GiB + trace prefetch | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.0 | 0.01 / 0.01 | 0.02 / 0.01 | 0.19 / 0.15 |
| checkout | LRU 4 GiB + read-ahead + trace prefetch | 388 | 96.4 | 3.6 | 0.0 | 141.7 | 1.18 | 12.0 | 0.01 / 0.01 | 0.02 / 0.01 | 0.19 / 0.15 |
| checkout | LRU 4 GiB + trace prefetch, merged 4 MiB | 388 | 96.2 | 3.8 | 0.0 | 129.7 | 1.08 | 0.0 | 0.01 / 0.01 | 0.04 / 0.03 | 0.19 / 0.15 |
| checkout | LRU 4 GiB + trace of the other project | 388 | 15.1 | 5.9 | 79.1 | 239.2 | 1.99 | 109.5 | 0.37 / 0.20 | 1.69 / 0.89 | 6.93 / 3.61 |
| checkout | LRU 4 GiB + base warm set | 388 | 16.4 | 2.3 | 81.3 | 109.5 | 0.91 | 0.0 | 0.37 / 0.20 | 1.72 / 0.91 | 6.94 / 3.65 |
| checkout | same node, LRU 64 MiB | 388 | 58.8 | 2.3 | 39.0 | 51.2 | 0.42 | 0.0 | 0.18 / 0.17 | 0.83 / 0.82 | 3.37 / 3.33 |
| checkout | same node, LRU 512 MiB | 388 | 100.0 | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| checkout | same node, LRU 4 GiB | 388 | 100.0 | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| gobuild | no cache | 59485 | 0.0 | 0.0 | 100.0 | 2250.0 | 4.44 | 0.0 | 59.79 / 2.07 | 297.43 / 10.36 | 1189.70 / 41.42 |
| gobuild | LRU 64 MiB | 59485 | 1.2 | 3.9 | 95.0 | 1190.4 | 2.35 | 0.0 | 57.27 / 2.04 | 291.05 / 10.30 | 1178.73 / 41.36 |
| gobuild | LRU 512 MiB | 59485 | 9.1 | 2.4 | 88.5 | 1112.2 | 2.19 | 0.0 | 53.23 / 2.01 | 268.77 / 10.15 | 1082.87 / 40.77 |
| gobuild | LRU 4 GiB | 59485 | 13.0 | 1.7 | 85.3 | 1076.4 | 2.12 | 0.0 | 51.19 / 1.92 | 257.57 / 9.67 | 1035.42 / 38.71 |
| gobuild | LRU 4 GiB + read-ahead | 59485 | 13.2 | 1.8 | 85.0 | 1076.4 | 2.12 | 0.0 | 51.16 / 1.92 | 257.30 / 9.67 | 1032.35 / 38.71 |
| gobuild | LRU 512 MiB + trace prefetch | 59485 | 84.7 | 0.9 | 14.4 | 1201.3 | 2.37 | 88.9 | 8.83 / 0.67 | 44.93 / 3.42 | 236.66 / 14.47 |
| gobuild | LRU 4 GiB + trace prefetch | 59485 | 98.6 | 0.1 | 1.3 | 1089.0 | 2.15 | 12.6 | 0.77 / 0.04 | 3.89 / 0.20 | 163.50 / 14.47 |
| gobuild | LRU 4 GiB + read-ahead + trace prefetch | 59485 | 98.6 | 0.1 | 1.3 | 1089.0 | 2.15 | 12.6 | 0.77 / 0.04 | 3.89 / 0.20 | 163.50 / 14.47 |
| gobuild | LRU 4 GiB + trace prefetch, merged 4 MiB | 59485 | 98.7 | 0.1 | 1.3 | 1089.0 | 2.15 | 12.6 | 0.77 / 0.04 | 3.86 / 0.19 | 15.58 / 0.77 |
| gobuild | LRU 4 GiB + trace of the other project | 59485 | 20.5 | 1.6 | 78.0 | 1579.9 | 3.12 | 503.5 | 46.85 / 1.75 | 235.65 / 8.78 | 969.49 / 35.15 |
| gobuild | LRU 4 GiB + base warm set | 59485 | 23.3 | 1.4 | 75.3 | 898.2 | 1.77 | 0.0 | 45.25 / 1.75 | 227.63 / 8.77 | 915.33 / 35.11 |
| gobuild | same node, LRU 64 MiB | 59485 | 2.7 | 3.9 | 93.5 | 1177.7 | 2.32 | 0.0 | 56.37 / 2.03 | 286.58 / 10.27 | 1160.86 / 41.24 |
| gobuild | same node, LRU 512 MiB | 59485 | 51.3 | 1.3 | 47.4 | 724.9 | 1.43 | 0.0 | 28.51 / 1.86 | 143.96 / 9.40 | 579.29 / 37.75 |
| gobuild | same node, LRU 4 GiB | 59485 | 98.9 | 0.0 | 1.0 | 11.9 | 0.02 | 0.0 | 0.63 / 0.03 | 3.17 / 0.15 | 12.79 / 0.60 |
| gotest | no cache | 60206 | 0.0 | 0.0 | 100.0 | 2308.3 | 4.26 | 0.0 | 60.51 / 2.32 | 301.03 / 11.56 | 1204.12 / 46.24 |
| gotest | LRU 64 MiB | 60206 | 1.4 | 4.1 | 94.6 | 1226.5 | 2.26 | 0.0 | 57.81 / 2.27 | 293.79 / 11.40 | 1191.42 / 46.02 |
| gotest | LRU 512 MiB | 60206 | 9.1 | 2.5 | 88.4 | 1155.7 | 2.13 | 0.0 | 53.68 / 2.25 | 271.44 / 11.32 | 1096.95 / 45.68 |
| gotest | LRU 4 GiB | 60206 | 13.7 | 1.7 | 84.6 | 1109.9 | 2.05 | 0.0 | 51.33 / 2.13 | 258.41 / 10.67 | 1039.89 / 42.71 |
| gotest | LRU 4 GiB + read-ahead | 60206 | 13.9 | 1.8 | 84.3 | 1109.9 | 2.05 | 0.0 | 51.30 / 2.13 | 258.04 / 10.67 | 1036.34 / 42.71 |
| gotest | LRU 512 MiB + trace prefetch | 60206 | 90.3 | 1.0 | 8.8 | 1245.2 | 2.30 | 89.5 | 5.41 / 0.36 | 28.21 / 1.82 | 81.66 / 4.32 |
| gotest | LRU 4 GiB + trace prefetch | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 13.6 | 0.73 / 0.03 | 3.71 / 0.17 | 21.77 / 2.12 |
| gotest | LRU 4 GiB + read-ahead + trace prefetch | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 13.6 | 0.73 / 0.03 | 3.71 / 0.17 | 21.77 / 2.12 |
| gotest | LRU 4 GiB + trace prefetch, merged 4 MiB | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 13.6 | 0.73 / 0.03 | 3.70 / 0.17 | 14.90 / 0.66 |
| gotest | LRU 4 GiB + trace of the other project | 60206 | 21.1 | 1.6 | 77.3 | 1621.1 | 2.99 | 511.2 | 46.95 / 1.93 | 236.36 / 9.66 | 952.26 / 38.67 |
| gotest | LRU 4 GiB + base warm set | 60206 | 23.9 | 1.4 | 74.7 | 931.4 | 1.72 | 0.0 | 45.36 / 1.93 | 228.37 / 9.66 | 919.23 / 38.67 |
| gotest | same node, LRU 64 MiB | 60206 | 3.0 | 4.1 | 92.9 | 1212.1 | 2.23 | 0.0 | 56.83 / 2.26 | 288.85 / 11.37 | 1171.65 / 45.90 |
| gotest | same node, LRU 512 MiB | 60206 | 51.2 | 1.4 | 47.3 | 759.8 | 1.40 | 0.0 | 28.75 / 2.16 | 145.55 / 10.83 | 588.10 / 43.72 |
| gotest | same node, LRU 4 GiB | 60206 | 99.1 | 0.0 | 0.9 | 12.3 | 0.02 | 0.0 | 0.56 / 0.02 | 2.82 / 0.11 | 11.34 / 0.44 |
| npm | no cache | 3382 | 0.0 | 0.0 | 100.0 | 419.5 | 1.01 | 0.0 | 3.39 / 1.41 | 16.91 / 7.04 | 67.64 / 28.14 |
| npm | LRU 64 MiB | 3382 | 0.2 | 9.6 | 90.2 | 407.5 | 0.98 | 0.0 | 3.18 / 1.34 | 16.63 / 6.96 | 67.28 / 28.07 |
| npm | LRU 512 MiB | 3382 | 0.2 | 9.6 | 90.2 | 407.5 | 0.98 | 0.0 | 3.18 / 1.34 | 16.63 / 6.96 | 67.28 / 28.07 |
| npm | LRU 4 GiB | 3382 | 0.2 | 9.6 | 90.2 | 407.5 | 0.98 | 0.0 | 3.18 / 1.34 | 16.63 / 6.96 | 67.28 / 28.07 |
| npm | LRU 4 GiB + read-ahead | 3382 | 0.4 | 18.7 | 80.9 | 407.5 | 0.98 | 0.0 | 3.18 / 1.34 | 16.63 / 6.96 | 67.28 / 28.07 |
| npm | LRU 512 MiB + trace prefetch | 3382 | 87.9 | 7.4 | 4.7 | 407.5 | 0.98 | 0.0 | 0.18 / 0.15 | 0.99 / 0.79 | 4.15 / 3.37 |
| npm | LRU 4 GiB + trace prefetch | 3382 | 87.9 | 7.4 | 4.7 | 407.5 | 0.98 | 0.0 | 0.18 / 0.15 | 0.99 / 0.79 | 4.15 / 3.37 |
| npm | LRU 4 GiB + read-ahead + trace prefetch | 3382 | 87.9 | 10.2 | 1.8 | 407.5 | 0.98 | 0.0 | 0.18 / 0.15 | 1.00 / 0.80 | 4.14 / 3.36 |
| npm | LRU 4 GiB + trace prefetch, merged 4 MiB | 3382 | 88.1 | 11.9 | 0.0 | 407.5 | 0.98 | 0.0 | 0.19 / 0.15 | 0.97 / 0.76 | 3.68 / 2.89 |
| npm | LRU 4 GiB + trace of the other project | 3382 | 70.0 | 7.4 | 22.6 | 561.1 | 1.35 | 153.6 | 0.73 / 0.15 | 3.71 / 0.79 | 15.03 / 3.37 |
| npm | LRU 4 GiB + base warm set | 3382 | 48.1 | 4.8 | 47.1 | 156.3 | 0.38 | 0.0 | 1.72 / 0.60 | 8.95 / 3.15 | 36.19 / 12.70 |
| npm | same node, LRU 64 MiB | 3382 | 0.2 | 9.6 | 90.2 | 407.5 | 0.98 | 0.0 | 3.18 / 1.34 | 16.63 / 6.96 | 67.28 / 28.07 |
| npm | same node, LRU 512 MiB | 3382 | 100.0 | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| npm | same node, LRU 4 GiB | 3382 | 100.0 | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| ripgrep | no cache | 146511 | 0.0 | 0.0 | 100.0 | 3615.7 | 1.56 | 0.0 | 146.91 / 8.64 | 732.55 / 43.13 | 2930.22 / 172.54 |
| ripgrep | LRU 64 MiB | 146511 | 0.7 | 20.2 | 79.1 | 2100.3 | 0.91 | 0.0 | 129.30 / 7.85 | 706.24 / 41.92 | 2895.62 / 170.99 |
| ripgrep | LRU 512 MiB | 146511 | 0.7 | 20.2 | 79.1 | 2100.3 | 0.91 | 0.0 | 129.30 / 7.85 | 706.24 / 41.92 | 2895.62 / 170.99 |
| ripgrep | LRU 4 GiB | 146511 | 0.7 | 20.2 | 79.1 | 2100.3 | 0.91 | 0.0 | 129.30 / 7.85 | 706.24 / 41.92 | 2895.62 / 170.99 |
| ripgrep | LRU 4 GiB + read-ahead | 146511 | 0.7 | 20.5 | 78.8 | 2135.9 | 0.92 | 35.6 | 129.26 / 7.85 | 706.07 / 41.90 | 2895.18 / 170.99 |
| ripgrep | LRU 512 MiB + trace prefetch | 146511 | 72.7 | 6.0 | 21.2 | 2664.8 | 1.15 | 564.5 | 40.52 / 3.61 | 192.84 / 19.96 | 697.71 / 79.55 |
| ripgrep | LRU 4 GiB + trace prefetch | 146511 | 83.2 | 3.7 | 13.1 | 2218.3 | 0.96 | 117.9 | 21.08 / 3.04 | 119.22 / 17.08 | 632.28 / 86.95 |
| ripgrep | LRU 4 GiB + read-ahead + trace prefetch | 146511 | 82.4 | 4.0 | 13.6 | 2249.2 | 0.97 | 148.9 | 22.16 / 3.63 | 124.74 / 19.90 | 652.29 / 87.52 |
| ripgrep | LRU 4 GiB + trace prefetch, merged 4 MiB | 146511 | 83.8 | 3.6 | 12.6 | 2218.3 | 0.96 | 117.9 | 21.01 / 3.04 | 114.91 / 16.48 | 469.81 / 67.36 |
| ripgrep | LRU 4 GiB + trace of the other project | 146511 | 0.7 | 20.2 | 79.1 | 2613.7 | 1.13 | 513.4 | 129.29 / 7.85 | 706.19 / 41.92 | 2895.50 / 170.99 |
| ripgrep | LRU 4 GiB + base warm set | 146511 | 0.7 | 20.2 | 79.1 | 2086.0 | 0.90 | 0.0 | 129.28 / 7.85 | 706.16 / 41.92 | 2895.30 / 170.99 |
| ripgrep | same node, LRU 64 MiB | 146511 | 0.7 | 20.2 | 79.1 | 2100.3 | 0.91 | 0.0 | 129.30 / 7.85 | 706.24 / 41.92 | 2895.62 / 170.99 |
| ripgrep | same node, LRU 512 MiB | 146511 | 3.3 | 19.6 | 77.1 | 2080.3 | 0.90 | 0.0 | 125.95 / 7.85 | 687.59 / 41.92 | 2818.64 / 170.99 |
| ripgrep | same node, LRU 4 GiB | 146511 | 97.9 | 0.4 | 1.7 | 122.8 | 0.05 | 0.0 | 2.65 / 0.24 | 14.55 / 1.29 | 59.70 / 5.24 |
| uvsync | no cache | 1029 | 0.0 | 0.0 | 100.0 | 145.4 | 1.01 | 0.0 | 1.03 / 0.31 | 5.14 / 1.53 | 20.58 / 6.14 |
| uvsync | LRU 64 MiB | 1029 | 0.1 | 1.9 | 98.0 | 141.3 | 0.98 | 0.0 | 1.02 / 0.30 | 5.13 / 1.53 | 20.57 / 6.13 |
| uvsync | LRU 512 MiB | 1029 | 0.1 | 1.9 | 98.0 | 141.3 | 0.98 | 0.0 | 1.02 / 0.30 | 5.13 / 1.53 | 20.57 / 6.13 |
| uvsync | LRU 4 GiB | 1029 | 0.1 | 1.9 | 98.0 | 141.3 | 0.98 | 0.0 | 1.02 / 0.30 | 5.13 / 1.53 | 20.57 / 6.13 |
| uvsync | LRU 4 GiB + read-ahead | 1029 | 0.1 | 12.1 | 87.8 | 141.3 | 0.98 | 0.0 | 1.02 / 0.30 | 5.13 / 1.53 | 20.57 / 6.13 |
| uvsync | LRU 512 MiB + trace prefetch | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 1.0 | 0.03 / 0.01 | 0.15 / 0.06 | 0.60 / 0.24 |
| uvsync | LRU 4 GiB + trace prefetch | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 1.0 | 0.03 / 0.01 | 0.15 / 0.06 | 0.60 / 0.24 |
| uvsync | LRU 4 GiB + read-ahead + trace prefetch | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 1.0 | 0.03 / 0.01 | 0.15 / 0.06 | 0.60 / 0.24 |
| uvsync | LRU 4 GiB + trace prefetch, merged 4 MiB | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 1.0 | 0.03 / 0.01 | 0.15 / 0.06 | 0.60 / 0.24 |
| uvsync | LRU 4 GiB + trace of the other project | 1029 | 7.5 | 10.2 | 82.2 | 153.5 | 1.07 | 12.2 | 0.97 / 0.29 | 4.87 / 1.47 | 19.50 / 5.88 |
| uvsync | LRU 4 GiB + base warm set | 1029 | 9.5 | 0.3 | 90.2 | 102.0 | 0.71 | 0.0 | 0.95 / 0.29 | 4.76 / 1.47 | 19.06 / 5.88 |
| uvsync | same node, LRU 64 MiB | 1029 | 0.1 | 1.9 | 98.0 | 141.3 | 0.98 | 0.0 | 1.02 / 0.30 | 5.13 / 1.53 | 20.57 / 6.13 |
| uvsync | same node, LRU 512 MiB | 1029 | 99.5 | 0.0 | 0.5 | 0.0 | 0.00 | 0.0 | 0.01 / 0.00 | 0.03 / 0.01 | 0.12 / 0.06 |
| uvsync | same node, LRU 4 GiB | 1029 | 99.5 | 0.0 | 0.5 | 0.0 | 0.00 | 0.0 | 0.01 / 0.00 | 0.03 / 0.01 | 0.12 / 0.06 |

### Sensitivity (LRU 4 GiB + trace prefetch, 5 ms fetch)

| workflow | extent | page cache | prefetches in flight | unread bytes held | ranges | link | reads to engine (n) | hit % | late % | miss % | fetched MiB | amplification | stall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| agent | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59500 | 98.9 | 0.1 | 1.0 | 1088.0 | 1.57 | 3.11 |
| agent | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 9.88 |
| agent | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59500 | 96.4 | 0.1 | 3.5 | 1383.1 | 1.99 | 10.56 |
| agent | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 158004 | 99.1 | 0.0 | 0.9 | 1286.0 | 1.85 | 9.89 |
| agent | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 9.88 |
| agent | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 9.88 |
| agent | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 9.88 |
| agent | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 59500 | 92.3 | 0.3 | 7.4 | 1286.0 | 1.85 | 22.50 |
| agent | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 59500 | 99.2 | 0.0 | 0.8 | 1286.1 | 1.85 | 2.36 |
| agent | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 59455 | 97.1 | 0.0 | 2.8 | 935.3 | 2.85 | 8.45 |
| agent | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 59500 | 96.6 | 0.1 | 3.3 | 1286.0 | 1.85 | 9.93 |
| agent | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 9.88 |
| agent | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 59500 | 96.7 | 0.1 | 3.3 | 1286.0 | 1.85 | 9.88 |
| cargocheck | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 9819 | 96.7 | 1.7 | 1.7 | 1710.9 | 0.99 | 0.27 |
| cargocheck | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 9819 | 98.5 | 1.3 | 0.2 | 1715.1 | 0.99 | 0.27 |
| cargocheck | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 9819 | 99.2 | 0.6 | 0.2 | 1723.1 | 1.00 | 0.26 |
| cargocheck | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 327244 | 100.0 | 0.0 | 0.0 | 1715.1 | 0.99 | 0.34 |
| cargocheck | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 9819 | 98.5 | 1.3 | 0.2 | 1715.1 | 0.99 | 0.27 |
| cargocheck | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 9819 | 98.5 | 0.7 | 0.8 | 1715.1 | 0.99 | 0.27 |
| cargocheck | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 9819 | 98.5 | 1.3 | 0.2 | 1715.1 | 0.99 | 0.27 |
| cargocheck | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 9819 | 92.2 | 3.6 | 4.3 | 1715.1 | 0.99 | 0.77 |
| cargocheck | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 9819 | 98.7 | 1.3 | 0.0 | 1715.1 | 0.99 | 0.17 |
| cargocheck | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 11161 | 99.9 | 0.1 | 0.0 | 249.1 | 1.19 | 0.02 |
| cargocheck | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 9819 | 92.8 | 4.2 | 3.0 | 1715.1 | 0.99 | 1.61 |
| cargocheck | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 9819 | 99.2 | 0.6 | 0.2 | 1715.1 | 0.99 | 0.19 |
| cargocheck | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 9819 | 99.3 | 0.5 | 0.2 | 1715.1 | 0.99 | 0.15 |
| checkout | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 388 | 94.3 | 5.7 | 0.0 | 120.0 | 1.00 | 0.02 |
| checkout | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.02 |
| checkout | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 388 | 97.7 | 2.3 | 0.0 | 142.7 | 1.18 | 0.02 |
| checkout | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 531 | 96.8 | 3.2 | 0.0 | 129.7 | 1.08 | 0.02 |
| checkout | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.02 |
| checkout | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.02 |
| checkout | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.02 |
| checkout | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.02 |
| checkout | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 388 | 96.4 | 3.6 | 0.0 | 129.7 | 1.08 | 0.02 |
| checkout | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 52 | 98.1 | 1.9 | 0.0 | 9.9 | 48.61 | 0.00 |
| checkout | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 388 | 92.3 | 7.7 | 0.0 | 129.7 | 1.08 | 0.11 |
| checkout | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 388 | 97.5 | 2.5 | 0.0 | 129.7 | 1.08 | 0.01 |
| checkout | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 388 | 97.5 | 2.5 | 0.0 | 129.7 | 1.08 | 0.01 |
| gobuild | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59485 | 98.5 | 0.2 | 1.3 | 890.7 | 1.76 | 3.82 |
| gobuild | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59485 | 98.6 | 0.1 | 1.3 | 1089.0 | 2.15 | 3.89 |
| gobuild | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59485 | 98.7 | 0.0 | 1.3 | 1186.0 | 2.34 | 3.90 |
| gobuild | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 155355 | 99.6 | 0.0 | 0.3 | 1089.0 | 2.15 | 3.90 |
| gobuild | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 59485 | 98.6 | 0.1 | 1.3 | 1089.0 | 2.15 | 3.89 |
| gobuild | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 59485 | 86.7 | 1.1 | 12.2 | 1089.0 | 2.15 | 38.47 |
| gobuild | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 59485 | 98.7 | 0.1 | 1.3 | 1089.0 | 2.15 | 3.87 |
| gobuild | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 59485 | 97.2 | 0.1 | 2.7 | 1089.0 | 2.15 | 8.20 |
| gobuild | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 59485 | 98.8 | 0.1 | 1.1 | 1089.0 | 2.15 | 3.35 |
| gobuild | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 59465 | 98.7 | 0.0 | 1.3 | 921.2 | 2.84 | 3.80 |
| gobuild | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 59485 | 98.6 | 0.1 | 1.3 | 1089.0 | 2.15 | 4.03 |
| gobuild | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 59485 | 98.6 | 0.1 | 1.3 | 1089.0 | 2.15 | 3.89 |
| gobuild | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 59485 | 98.6 | 0.1 | 1.3 | 1089.0 | 2.15 | 3.89 |
| gotest | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 60206 | 98.7 | 0.2 | 1.1 | 924.6 | 1.70 | 3.48 |
| gotest | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 3.71 |
| gotest | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 60206 | 98.6 | 0.0 | 1.4 | 1221.4 | 2.25 | 4.27 |
| gotest | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 422505 | 99.9 | 0.0 | 0.1 | 1123.4 | 2.07 | 3.73 |
| gotest | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 3.71 |
| gotest | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 60206 | 98.2 | 0.1 | 1.8 | 1123.4 | 2.07 | 5.42 |
| gotest | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 3.71 |
| gotest | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 60206 | 96.5 | 0.1 | 3.3 | 1123.4 | 2.07 | 10.25 |
| gotest | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 60206 | 99.0 | 0.1 | 0.9 | 1123.4 | 2.07 | 2.83 |
| gotest | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 60179 | 98.9 | 0.0 | 1.1 | 925.9 | 2.81 | 3.28 |
| gotest | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 3.71 |
| gotest | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 3.71 |
| gotest | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 60206 | 98.7 | 0.1 | 1.2 | 1123.4 | 2.07 | 3.71 |
| npm | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 3382 | 75.9 | 5.8 | 18.3 | 407.5 | 0.98 | 0.97 |
| npm | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 3382 | 87.9 | 7.4 | 4.7 | 407.5 | 0.98 | 0.99 |
| npm | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 3382 | 92.2 | 6.1 | 1.7 | 407.5 | 0.98 | 0.99 |
| npm | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 4539 | 89.3 | 7.1 | 3.6 | 407.5 | 0.98 | 1.30 |
| npm | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 3382 | 87.9 | 7.4 | 4.7 | 407.5 | 0.98 | 0.99 |
| npm | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 3382 | 87.9 | 3.3 | 8.8 | 407.5 | 0.98 | 1.00 |
| npm | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 3382 | 88.1 | 11.9 | 0.0 | 407.5 | 0.98 | 0.96 |
| npm | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 3382 | 87.0 | 8.3 | 4.7 | 407.5 | 0.98 | 0.99 |
| npm | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 3382 | 87.9 | 7.4 | 4.7 | 407.5 | 0.98 | 0.99 |
| npm | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 3364 | 99.9 | 0.1 | 0.0 | 167.7 | 0.99 | 0.01 |
| npm | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 3382 | 75.6 | 10.3 | 14.1 | 407.5 | 0.98 | 3.28 |
| npm | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 3382 | 92.7 | 4.2 | 3.1 | 407.5 | 0.98 | 0.15 |
| npm | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 3382 | 92.9 | 4.0 | 3.1 | 407.5 | 0.98 | 0.07 |
| ripgrep | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 146511 | 83.2 | 3.6 | 13.2 | 2165.9 | 0.93 | 117.12 |
| ripgrep | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 146511 | 83.2 | 3.7 | 13.1 | 2218.3 | 0.96 | 119.22 |
| ripgrep | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 146511 | 82.6 | 3.9 | 13.6 | 2326.2 | 1.00 | 123.93 |
| ripgrep | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 231533 | 83.6 | 8.1 | 8.3 | 2218.3 | 0.96 | 177.58 |
| ripgrep | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 146511 | 83.2 | 3.7 | 13.1 | 2218.3 | 0.96 | 119.22 |
| ripgrep | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 146511 | 78.1 | 5.1 | 16.8 | 2218.3 | 0.96 | 155.44 |
| ripgrep | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 146511 | 83.7 | 3.5 | 12.7 | 2218.3 | 0.96 | 115.29 |
| ripgrep | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 146511 | 48.5 | 11.3 | 40.2 | 2202.5 | 0.95 | 365.40 |
| ripgrep | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 146511 | 96.0 | 0.8 | 3.2 | 2218.3 | 0.96 | 28.40 |
| ripgrep | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 146501 | 83.2 | 3.7 | 13.1 | 2207.4 | 0.96 | 119.09 |
| ripgrep | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 146511 | 78.2 | 5.2 | 16.6 | 2218.3 | 0.96 | 154.31 |
| ripgrep | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 146511 | 83.5 | 3.6 | 12.9 | 2218.3 | 0.96 | 117.29 |
| ripgrep | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 146511 | 83.5 | 3.6 | 12.9 | 2218.3 | 0.96 | 117.29 |
| uvsync | 256 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 1029 | 73.7 | 10.0 | 16.3 | 141.6 | 0.98 | 0.15 |
| uvsync | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 0.15 |
| uvsync | 4096 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 1029 | 95.2 | 4.2 | 0.6 | 145.4 | 1.01 | 0.14 |
| uvsync | 1024 KiB | 0 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 2174 | 94.3 | 5.4 | 0.3 | 142.4 | 0.99 | 0.20 |
| uvsync | 1024 KiB | 4096 MiB | 64 | 256 MiB | reads+maps | 750 MB/s | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 0.15 |
| uvsync | 1024 KiB | 1024 MiB | 16 | 256 MiB | reads+maps | 750 MB/s | 1029 | 89.5 | 4.1 | 6.4 | 142.4 | 0.99 | 0.15 |
| uvsync | 1024 KiB | 1024 MiB | 256 | 256 MiB | reads+maps | 750 MB/s | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 0.15 |
| uvsync | 1024 KiB | 1024 MiB | 64 | 64 MiB | reads+maps | 750 MB/s | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 0.15 |
| uvsync | 1024 KiB | 1024 MiB | 64 | 1024 MiB | reads+maps | 750 MB/s | 1029 | 89.5 | 10.0 | 0.5 | 142.4 | 0.99 | 0.15 |
| uvsync | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads | 750 MB/s | 1003 | 98.8 | 0.6 | 0.6 | 16.4 | 1.17 | 0.04 |
| uvsync | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 250 MB/s | 1029 | 89.2 | 10.2 | 0.5 | 142.4 | 0.99 | 0.16 |
| uvsync | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | 3000 MB/s | 1029 | 89.6 | 9.9 | 0.5 | 142.4 | 0.99 | 0.14 |
| uvsync | 1024 KiB | 1024 MiB | 64 | 256 MiB | reads+maps | unlimited | 1029 | 90.3 | 9.2 | 0.5 | 142.4 | 0.99 | 0.07 |

### Working set: what a run needs from the disk it starts with (run 2 of each project)

| workflow | project | files (n) | MiB read calls | MiB with maps and execs | MiB in 1 MiB extents | MiB written | MiB read back from own writes |
|---|---|---|---|---|---|---|---|
| agent | a | 28821 | 171.4 | 276.4 | 758.8 | 1870.6 | 4069.9 |
| agent | b | 22267 | 156.6 | 417.8 | 501.8 | 2575.5 | 7784.1 |
| cargocheck | a | 3377 | 78.9 | 781.6 | 776.9 | 410.9 | 5204.2 |
| cargocheck | b | 6051 | 130.7 | 947.8 | 938.2 | 1172.8 | 12789.4 |
| checkout | a | 146 | 0.1 | 61.4 | 64.0 | 81.5 | 0.0 |
| checkout | b | 130 | 0.1 | 59.0 | 65.7 | 21.4 | 0.0 |
| gobuild | a | 22047 | 154.3 | 247.3 | 337.4 | 2575.5 | 7731.9 |
| gobuild | b | 28626 | 170.0 | 259.9 | 739.0 | 1870.7 | 4066.3 |
| gotest | a | 22158 | 157.4 | 258.6 | 344.1 | 7881.1 | 41239.8 |
| gotest | b | 28737 | 171.6 | 283.9 | 765.8 | 3254.3 | 9516.5 |
| npm | a | 1387 | 124.1 | 246.8 | 243.6 | 365.7 | 0.0 |
| npm | b | 1626 | 44.8 | 167.5 | 163.9 | 192.6 | 0.1 |
| ripgrep | a | 65586 | 923.6 | 930.8 | 797.7 | 0.0 | 0.0 |
| ripgrep | b | 50137 | 1382.4 | 1389.5 | 1302.6 | 0.0 | 0.0 |
| uvsync | a | 438 | 7.1 | 72.0 | 71.0 | 3.2 | 1.6 |
| uvsync | b | 568 | 6.9 | 71.9 | 70.4 | 1.6 | 0.5 |

### Time waiting for file reads on local NVMe

Cold and warm are untraced runs: cold after the page cache of every file the workflow reads was dropped, warm straight after. Their difference is the wall time the disk costs. The strace columns sum the time inside read calls with the 11000 ns strace adds to an empty call removed; strace serialises the stops of all threads, so they overstate the wait of multi-threaded workflows and are shown only as the per-thread view.

| workflow | project | read calls (n) | traced s | cold s median (min-max) | warm s median (min-max) | cold - warm s | % of cold wall | strace read wait s (thread-seconds) | strace any thread reading s | strace open/stat/list wait s |
|---|---|---|---|---|---|---|---|---|---|---|
| agent | a | 227303 | 114.12 | 33.19 (33.19-33.19) | 27.12 (27.12-27.12) | 6.07 | 18.3 | 83.28 | 50.16 | 90.05 |
| agent | b | 652145 | 218.91 | 67.79 (67.79-67.79) | 117.57 (117.57-117.57) | -49.78 | -73.4 | 76.29 | 57.35 | 127.90 |
| cargocheck | a | 200031 | 83.70 | 16.16 (16.16-16.16) | 22.00 (22.00-22.00) | -5.84 | -36.1 | 43.49 | 30.97 | 39.02 |
| cargocheck | b | 524408 | 322.30 | 113.11 (113.11-113.11) | 107.70 (107.70-107.70) | 5.41 | 4.8 | 119.59 | 92.25 | 242.81 |
| checkout | a | 55 | 3.06 | 1.19 (1.00-1.68) | 0.35 (0.29-0.68) | 0.84 | 70.3 | 0.02 | 0.02 | 1.26 |
| checkout | b | 55 | 2.97 | 0.50 (0.43-0.82) | 0.19 (0.11-0.23) | 0.31 | 62.6 | 0.02 | 0.02 | 0.56 |
| gobuild | a | 638273 | 213.89 | 79.63 (79.63-79.63) | 71.37 (71.37-71.37) | 8.26 | 10.4 | 101.10 | 72.36 | 103.55 |
| gobuild | b | 226973 | 101.62 | 87.00 (87.00-87.00) | 62.56 (62.56-62.56) | 24.44 | 28.1 | 74.98 | 45.56 | 68.36 |
| gotest | a | 4418161 | 675.00 | 193.30 (193.30-193.30) | 223.26 (223.26-223.26) | -29.95 | -15.5 | 1138.15 | 446.89 | 582.00 |
| gotest | b | 500560 | 202.83 | 104.57 (104.57-104.57) | 101.63 (101.63-101.63) | 2.94 | 2.8 | 156.79 | 95.28 | 147.40 |
| npm | a | 2140 | 9.41 | 4.31 | 4.90 | -0.59 | -13.7 | 0.58 | 0.49 | 1.57 |
| npm | b | 2419 | 15.07 | 7.20 | 2.99 | 4.22 | 58.5 | 3.01 | 2.51 | 4.68 |
| ripgrep | a | 134619 | 30.55 | 3.65 | 0.22 | 3.43 | 94.0 | 92.88 | 28.52 | 27.05 |
| ripgrep | b | 102299 | 26.66 | 4.01 | 0.28 | 3.73 | 93.0 | 86.94 | 25.21 | 25.06 |
| uvsync | a | 1105 | 8.87 | 1.92 | 1.12 | 0.80 | 41.8 | 0.59 | 0.49 | 1.17 |
| uvsync | b | 1312 | 4.48 | 1.43 | 0.79 | 0.64 | 44.9 | 0.52 | 0.43 | 1.08 |

## The interface a prefetcher needs from the engine

The engine owns the cache, the fetching and every budget. A prefetcher is a
policy beside the read path: it watches reads, proposes chunks, and hands back a
trace. Nothing here changes the on-disk layout. The sketch is Go for precision;
the normative statements are in the specification notes.

```go
// Seen is one read the engine served, after the guest page cache.
type Seen struct {
	At      time.Duration // since attach
	Thread  uint32        // calling thread, as the mount reports it
	Inode   uint64
	Offset  int64
	Length  int64
	Chunks  []ChunkID     // the chunks that served it, in order
	Outcome Outcome       // Hit, Late (waited on a fetch in flight), Miss
	Waited  time.Duration // time the caller waited for data
}

type Prefetcher interface {
	// Observe is called on the read path. It must return at once; the
	// engine drops observations when the policy falls behind.
	Observe(Seen)

	// Plan returns the next chunks to fetch, most urgent first. The
	// engine calls it when it has room, under a context it cancels on
	// detach, suspend, loss of the write fence, and publication delay.
	Plan(ctx context.Context, room Budget) []ChunkID

	// Finish returns the trace to keep when the attachment ends.
	Finish() []ChunkID
}

// Budget is set by the engine, never by the prefetcher.
type Budget struct {
	Unread   int64 // bytes fetched by prefetch and not yet read (default 256 MiB)
	InFlight int   // fetches outstanding (default 64)
}
```

**What it sees.** Chunk identity, order, timing and outcome of each read; the
workflow key. Not paths, not contents, not who. The measurements show this is
enough: a trace of first-touch order is what the policy that worked uses, and
the outcomes tell it how far ahead it is running.

**What it may fetch.** Chunks of its own volume that are reachable from the
published root. The engine checks reachability, deduplicates against the cache
and against fetches in flight, and may merge adjacent chunks of one pack into a
ranged request.

**Cancellation.** Per plan, and wholesale by the engine on detach, suspend, end
of run, loss of the write fence, and when the oldest unpublished write is older
than half of the publication bound. A read that needs a queued chunk promotes
it; it does not cancel it.

**Byte budget.** Two numbers the engine sets: unread bytes and fetches in
flight, drawn from the one worker budget that the chunking, hashing, sealing and
upload pipeline also draws on. Prefetch has the lowest priority in it.

**Interaction with the cache.** Prefetched chunks enter a probationary segment
and become protected on their first read, so a wrong guess evicts other wrong
guesses first. Dirty and unpublished data is not in the evictable set at all.

**Interaction with the durability watermarks.** The three sequence numbers of
the journal design (`visible`, `local_durable`, `s3_committed`) are untouched by
prefetch. It reads only data at or below `s3_committed`, since nothing above it
exists in the store; data between `local_durable` and `s3_committed` is served
from the journal or the local cache. It never takes a lock the writer, the
group-commit path or the publisher takes, so a held request to the store stalls
prefetch and nothing else. When publication falls behind, prefetch yields first.

**The trace.** Kept per volume, sealed with the volume's key, replaced
atomically at detach, addressed by the workflow key the attaching party
supplies. It lists chunk identifiers in first-touch order. It is useless to
anyone without the volume's key and carries no name of any file.

## Specification notes

This section is the part a third party could implement and test from. The
words MUST, SHOULD and MAY are used as in RFC 2119. Everything above it is
the evidence; the rationale after each group says which measurement it comes
from and is not normative.

### Terms

- **Read** is a read the volume engine serves, after the guest kernel's page
  cache has answered what it holds.
- **Chunk** is the unit the engine stores and fetches: a sealed,
  content-addressed piece of one volume.
- **Prefetcher** is a policy that decides which chunks to fetch before a read
  asks for them. The engine owns the cache, the fetching and every budget; the
  prefetcher only proposes.
- **Trace** is the ordered list of distinct chunks a run read, first touch
  first.
- **Workflow key** is an opaque string the party attaching the volume supplies
  to say "this run repeats that one".
- **Unread bytes** are bytes fetched by prefetch that no read has used yet.

### S1. Observation

1. The engine MUST give the prefetcher, for every read, the time since attach,
   the calling thread, the inode, the byte range, the chunks that served it in
   order, the outcome (`hit`, `late` when the read waited for a fetch already in
   flight, `miss` when it caused a fetch) and the time the caller waited.
2. Observation MUST NOT block a read. The engine delivers observations through a
   bounded queue, drops the newest when it is full and counts the drops.
3. The engine MUST NOT give the prefetcher file contents, path names or user
   identity.

*Acceptance.* With a prefetcher that never returns from `Observe`, the p99 of a
warm 4 KiB read is within 5% of the same read with no prefetcher, the dropped
count rises, and no goroutine of the read path is blocked on it.

*Rationale.* A trace needs only chunk identity and order, and the sequence of
outcomes is what lets a policy measure itself. A read path that waits for its
observer would put the policy's worst case on every read.

### S2. Fetching

1. A prefetcher MUST ask for chunks only by identifier, in priority order. The
   engine MUST fetch a requested chunk only if it is reachable from the volume's
   current published root; it MUST ignore the rest and report how many it
   ignored.
2. The engine MUST NOT fetch a chunk that is already cached or already in
   flight, and MUST NOT issue two requests for one chunk.
3. A read that needs a chunk the prefetcher queued but has not started MUST be
   served by a demand fetch no later than a read with no prefetcher would be.
   The queued entry is promoted, not cancelled, and is not fetched twice.
4. A prefetch fetch MUST use the lowest priority class of the engine's worker
   budget. It MUST NOT hold the volume lock, the journal lock or the publisher's
   lock, and MUST NOT delay a journal append, a group fsync, an object upload of
   the publisher or the publication of the root.

*Acceptance.* (a) A plan naming 1000 chunks of which 100 are unreachable fetches
900 and reports 100 ignored. (b) A read of a queued chunk completes within one
fetch latency plus 1 ms of the moment it is issued, with one request on the
wire for that chunk. (c) With prefetch saturating its budget, the p99 of fsync
and the age of the oldest unpublished write stay within 5% of the run without
prefetch, and the 5 s publication bound still holds while the store is healthy.

*Rationale.* Content addressing makes a stale trace harmless: a changed file has
new chunk identifiers, so the old ones are simply not reachable or not asked
for again. The priority rule is the durability watermark's rule: reads of the
past must never slow the publication of the present.

### S3. Budgets

1. The engine, not the prefetcher, MUST set and enforce three bounds: the
   unread bytes (default 256 MiB), the fetches in flight (default 64) and the
   share of the node's worker budget.
2. Prefetch MUST stop starting fetches while the unread bytes are at or above
   its bound, and MUST resume as reads consume them or the cache evicts them.
3. A prefetch plan MUST end when its entries run out. Entries the run already
   read MUST be skipped without a request.
4. The bytes a prefetcher wastes on a run that diverges MUST NOT exceed the
   unread bound plus one chunk.

*Acceptance.* Unread bytes, sampled at every fetch completion, never exceed the
bound plus one chunk. A plan of 1 GiB against a run that reads none of it
fetches at most the bound plus one chunk and then issues no more requests. A run
that reads the plan's chunks in the reverse order still finds all of them
fetched.

*Rationale.* The measured workflows differ in order between runs far more than
in content: 92.5% to 100% of pages repeat while 0.2% to 9% of consecutive extent
pairs do in the parallel workflows. The policy that follows the previous order
loses them (22% hit on a Go build) and the one bounded by bytes does not
(98.6%). A bound of 64 MiB lowered the hit rate of the tree search to 49% and
of the agent mix to 92%; 256 MiB is the knee for the rest, and the tree search
reaches 96% at 1 GiB. 16 fetches in flight lowered the tree search from 83% to
78% and a Go build from 98.6% to 87%; 256 gave 84% and 98.7%.

### S4. Cancellation

1. Every plan MUST carry a cancellation. Cancelling a plan MUST remove its
   unstarted entries and abort its requests in flight where the store supports
   it, and MUST return the bytes it held to the budget.
2. The engine MUST cancel all plans on detach, suspend, end of run, loss of the
   write fence, and whenever the volume enters a degraded state in which the
   oldest unpublished write is older than half the publication bound.
3. After `Detach` returns, no prefetch request of that attachment MUST remain
   outstanding.

*Acceptance.* A plan cancelled with 10 000 queued entries returns within 10 ms
with none started afterwards. After `Detach`, a trace of the store's requests
shows none issued by prefetch. Raising the oldest unpublished write's age past
half the bound drains the prefetch queue within one fetch latency and no new
entry starts until the age falls.

### S5. Cache

1. Prefetched chunks MUST enter the cache in a probationary segment and move to
   the protected segment on their first read. When the cache needs room, it MUST
   evict unread probationary chunks before any protected chunk.
2. Dirty or unpublished data MUST NOT be in the evictable set.
3. A chunk that has been read MUST count as read for the unread budget even if
   it is later evicted.

*Acceptance.* With a cache of N bytes full of protected chunks and a plan of 2N
bytes, no protected chunk is evicted by prefetch. Writing while prefetch runs
never evicts a byte that has not been published.

### S6. Traces

1. The engine SHOULD record the trace of an attachment and, at detach, store it
   as one extension object of the volume (docs/SPECIFICATION.md 3.1: `PutExt`,
   extension `trace`), keyed by the attachment's workflow key. The object is at
   `ext/trace/<64 hex digits of a hash keyed from the volume's seed>` (3.1 of the
   specification: the same workflow key is another name in every volume, and the store
   cannot confirm a guess at a key), and is sealed with the volume's key to the volume,
   the extension and that name. With no workflow key the
   engine MUST NOT prefetch from a trace; read-ahead (S7) still applies.
2. A trace object is one version byte, `0x01`, and then chunk identifiers of 32
   bytes each, in first-touch order, nothing else that identifies content. It
   MUST be bounded: at most 1 000 000 entries, the earliest kept, so that the
   object is 32 000 001 bytes at most and the extension object cap of 32 MiB
   (specification 3.1) leaves room for a longer header in a later version. (1 048 576
   entries of 32 bytes are exactly 32 MiB, which would leave no room for any
   header.) A reader refuses a trace whose first byte it does not know, and the
   attachment goes without a trace.
3. A trace belongs to its volume. The engine MUST NOT use one volume's trace to
   fetch for another, except through a shared read-only base image the same
   party owns, whose chunks are the same for every volume built on it.
4. Replacing a trace MUST be atomic: a reader sees the old trace or the new one.
5. A new trace MUST replace the old one only if the run finished with a zero
   exit status reported by the attaching party, or the party asks for it.
6. **Retention.** The engine never deletes a trace. A trace lives until the same
   workflow key replaces it (4) or the volume is deleted, so a volume holds one
   trace for each distinct workflow key that ever finished a run: a party that
   wants the number bounded uses a key per image or per kind of job, not one per
   run. A trace is a hint (specification 3.1, Fencing), so an operator can
   delete the objects under `ext/trace/` at any time, by prefix, and the cost is
   the prefetch of the next run of each key.
7. **Cost of a read.** The name of the object needs the volume's seed, which the
   Head holds, so the read of a trace is one `GET` after the Head has been read
   and cannot be issued with it: an attachment that prefetches from a trace
   waits one serial round trip to the store before its first prefetch request.

*Acceptance.* A trace object, decrypted, is the version byte and identifiers only,
at most 32 000 001 bytes, and a trace of 1 000 001 identifiers is stored as its
first 1 000 000; a volume opened with another volume's key cannot read it; killing
the engine during the write leaves the previous trace intact.

*Rationale.* The traces were recorded in a way that exposes paths; the engine
does not need them, and keeping them out makes the trace safe to store beside
the data it describes. Per-volume sealing keeps the rule that no plaintext or
chunk identity is shared across tenants.

### S7. Sequential read-ahead

1. Read-ahead MAY start after two reads of consecutive chunks of one file, and
   MUST NOT exceed 8 chunks ahead. Its window MUST reset on a non-consecutive
   read.
2. Read-ahead MUST draw on the same budgets and the same cancellation as S3 and
   S4.

*Acceptance.* A random read pattern issues no read-ahead. A sequential pattern
reaches the window of 8 after four reads.

*Rationale.* Sequential runs on one file are short at the median (0 to 4 KiB)
and long only at the tail (99th percentile 3 to 271 KiB), and read-ahead alone
moved the cold hit rate by 0.3 points or less in seven of eight workflows. It
is a tail optimisation (see the read size table).

### S8. Fetch granularity

1. The engine MAY merge prefetch entries that are adjacent in the trace into one
   ranged request when they are also adjacent in one pack, up to 4 MiB. Merging
   MUST NOT change which bytes are fetched or their order of priority.

*Acceptance.* Bytes fetched are identical with and without merging, and the
number of requests falls. On a search over a tree of 50 000 files with a 20 ms
fetch, the summed stall falls by at least 20%.

*Rationale.* Merging cut the summed stall of the tree search at 20 ms from 632 s
to 470 s and changed nothing measurable for the other seven workflows or at 5 ms.
It assumes that entries adjacent in the trace are adjacent in a pack, which the
study could not check; the engine knows the layout and can.

## Reproducing

```sh
go build -o iotrace ./tools/iotrace
iotrace calibrate -dir "$SCRATCH" -out "$DATA/calibration.json"

# one project, one workflow: two traced runs, then three untraced cold/warm pairs
iotrace run -workflow gobuild -label a -project "$PROJECT_A" -state "$SCRATCH/a" -data "$DATA" -runs 2 -plain 3
iotrace run -workflow gobuild -label b -project "$PROJECT_B" -state "$SCRATCH/b" -data "$DATA" -runs 2 -plain 3

# package installs also take a populated cache, a checkout takes two commits
iotrace run -workflow npm -label a -project "$P" -cache "$POPULATED_NPM_CACHE" ...
iotrace run -workflow checkout -label a -project "$REPO" -from "$OLD" -to "$NEW" ...

iotrace report -data "$DATA" > tables.md
```

`iotrace record -out DIR -workflow W -run R -- command args...` traces any
command and writes the same files. `iotrace evict PATH...` drops the page cache
of the files under the paths. The project directory is never written: a
workflow copies it into `-state`, which is deleted afterwards. The traces and
the manifests (which hold paths) stay with the person who recorded them; the
report prints classes of path and aggregates only.

## References

- ThinkAhead. USENIX FAST 2026. (Trace-guided preloading of a virtual disk for
  repeatable tasks; cited for the idea of replaying a previous run's accesses.
  No code was consulted; the policy here is an independent design and has not
  been compared with the paper's.)
- Patterson, Gibson, Ginting, Stodolsky, Zelenka. Informed prefetching and
  caching. SOSP 1995. (Disclosed hints and a bounded prefetch depth.)
- Johnson, Shasha. 2Q: a low overhead high performance buffer management
  replacement algorithm. VLDB 1994. (Probationary and protected segments.)
- Karedla, Love, Wherry. Caching strategies to improve disk system performance.
  IEEE Computer 27(3), 1994. (Segmented LRU.)
- Linux manual pages `posix_fadvise(2)`, `mincore(2)`, `strace(1)`,
  `ptrace(2)`.
