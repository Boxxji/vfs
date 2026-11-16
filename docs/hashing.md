# Hashing

The engine names every chunk and every shard by its BLAKE3 digest. On arm64 the
library it links runs the portable compression, one chunk at a time; in the write
path profiles that is 15 to 31 percent of the CPU. `internal/blake3` is the same
function, bit for bit, with a vector kernel on arm64, a streaming hasher, and a
parallel hash for large buffers. On amd64 it is the library the engine already
links, because nothing measured here beats it there. Refs #21.

## Result

Per core, GB/s, one worker, interleaved runs. Full tables below.

| | this package | library linked today | official C |
|---|---|---|---|
| arm64 Cortex-X925, 64 KiB to 16 MiB | **2.21 to 2.23** | 0.92 | 1.9 to 2.05 |
| arm64 Cortex-A725, 64 KiB to 16 MiB | **1.44 to 1.46** | 0.45 | 1.13 |
| arm64 X925, 4 KiB | **1.85** | 0.81 | 1.19 |
| amd64 Xeon E5-2680 v4 (AVX2), 64 KiB to 16 MiB | 2.0 to 2.2 | 2.0 to 2.2 (the same call) | 2.6 to 2.9 |

- **arm64: 2.4x to 3.2x the library today**, 10 to 30 percent above the official C
  implementation compiled with gcc -O3, and no C toolchain: the kernel is NEON
  assembly for the Go assembler, derived from the reference `blake3_neon.c`
  (Apache-2.0 or CC0).
- **amd64: this package is the library.** The tree over the AVX2 kernel of
  `lukechampine.com/blake3/guts`, which the first version of this package used,
  measured 25 to 38 percent slower than the library's own AVX2 code from 64 KiB
  up on the reference machine (1.6 to 1.9 against 2.5 to 2.7 GB/s in
  `go test -bench`, pinned to one core), and 9 percent faster at 4 KiB. A
  regression is not shipped: on amd64 `Sum` is the library's `Sum256`, `Hasher` is
  the library's hasher, and the tests that hold the tree to the specification run
  on amd64 as well, so either implementation can be swapped back in. `Parallel` keeps the tree, which the library cannot offer; it beats
  the library from two workers (below). The way to beat the library on amd64 is
  the next section.
- **cgo is not an option.** The image is built with `CGO_ENABLED=0` into
  `FROM scratch` (`Dockerfile`), so the official C implementation, which is the
  fastest on amd64, cannot ship. It is measured as a reference only.
- **Recommendation.** Take `internal/blake3` on both architectures: 2.4x to 3.2x
  on arm64, and on amd64 a single API with the speed of the library the engine
  uses. In the engine the better use of cores is hashing independent chunks on a
  bounded worker pool, one `Sum` per worker, which scales with the cores (below);
  `Parallel` is for a buffer of a megabyte or more that nothing else is hashing.

### The amd64 port that would beat the library

The official C implementation reaches 2.6 to 2.9 GB/s on the reference machine,
20 to 28 percent above the library, with hand-scheduled AVX2 assembly. A port of
that kernel to the Go assembler (`hash_many` for AVX2, eight chunks per call) is
the one change that would make `internal/blake3` faster than the library on
amd64. It replaces `library_amd64.go` when `go test -bench 'Sum|Stream'`, pinned
to one core on that machine and on an AVX-512 machine, shows it at or above the
library at 4 KiB, 64 KiB, 1 MiB and 16 MiB, with the tests of this package passing
unchanged on the tree path. Until then amd64 uses the library.

## Measurements

arm64 method: interleaved windows (every implementation in turn, 25 ms of CPU time
each), one worker pinned to one core, GOMAXPROCS=1, buffers of random bytes. The
tables are from an independent run on a quiet machine (median of the runs); our
runs on a busy one (load average 28) agree within 5 percent, except the official C
at 4 KiB, which was 0.87 there.

### arm64 (20 cores: 10 Cortex-X925, 10 Cortex-A725)

Cortex-X925, one core:

| GB/s | 4 KiB | 64 KiB | 1 MiB | 16 MiB |
|---|---|---|---|---|
| library linked today, Sum256 | 0.81 | 0.92 | 0.93 | 0.92 |
| second pure-Go library | 0.37 | 0.69 | 0.76 | 0.75 |
| official C, NEON, gcc -O3 | 1.19 | 1.94 | 2.05 | 1.93 |
| **this package, Sum** | **1.85** | **2.21** | **2.23** | **2.22** |
| this package, Hasher, 32 KiB writes | 1.57 | 2.17 | 2.22 | 2.22 |
| library linked today, Hasher, 32 KiB writes | 0.83 | 0.91 | 0.91 | 0.89 |

Cortex-A725, one core:

| GB/s | 4 KiB | 64 KiB | 1 MiB | 16 MiB |
|---|---|---|---|---|
| library linked today | 0.42 | 0.46 | 0.45 | 0.45 |
| official C | 0.85 | 1.13 | 1.14 | 1.13 |
| **this package** | **1.19** | **1.46** | **1.44** | **1.44** |

N workers hashing N independent buffers (wall clock, aggregate GB/s, X925 cores,
load average 6 to 20, so read the ratios, not the digits):

| workers | buffer | library linked today | official C | this package |
|---|---|---|---|---|
| 4 | 64 KiB | 1.9 | 3.3 | 4.7 |
| 4 | 1 MiB | 1.9 | 4.1 | 4.6 |
| 8 | 64 KiB | 3.8 | 6.0 | 8.5 |
| 8 | 1 MiB | 3.5 | 7.6 | 8.3 |

One buffer, `Parallel` with N workers (X925 cores; best of 30 windows of 150 ms,
with the median beside it because the machine was at load average 34 to 42 and
other jobs took the cores):

| workers | 1 MiB best / median | 16 MiB best / median |
|---|---|---|
| 1 (`Sum`) | 2.2 | 2.2 |
| 2 | 2.6 / 1.8 | 3.2 / 2.4 |
| 4 | 4.6 / 2.8 | 6.9 / 4.9 |
| 8 | 5.2 / 3.0 | 7.0 / 4.1 |

### amd64 (Xeon E5-2680 v4, 2.4 GHz, AVX2, no AVX-512; load average 10 to 15 of 56 threads)

`go test -bench`, wall clock, pinned to one core, the best of 6 runs, a binary built
with Go 1.26. Sum of this package is the library's `Sum256`; the second row is what
the first version of the package did, the tree over the `guts` kernel:

| GB/s | 4 KiB | 64 KiB | 1 MiB | 16 MiB |
|---|---|---|---|---|
| library linked today, Sum256 | 1.08 | 2.56 | 2.64 | 2.62 |
| **this package, Sum** | **1.10** | **2.67** | **2.60** | **2.52** |
| the tree over the `guts` kernel (first version) | 1.18 | 1.93 | 1.87 | 1.62 |
| second library | 0.27 | 1.02 | 1.60 | 1.66 |

Interleaved, wall clock, one pinned core, 16 rounds (median; three different cores
gave the same ratios within 2 percent):

| GB/s, core 39 | 4 KiB | 64 KiB | 1 MiB | 16 MiB |
|---|---|---|---|---|
| library linked today, Sum256 | 0.71 | 2.10 | 2.23 | 1.99 |
| **this package, Sum** | **0.71** | **2.06** | **2.21** | **2.00** |
| official C, AVX2 assembly | 0.80 | 2.57 | 2.86 | 2.79 |

The official C is 20 to 28 percent above the library from 64 KiB up. An independent
run in CPU time on the same machine puts it at 0.83, 2.30, 2.62 and 2.77 against the
library's 0.80, 2.17, 2.29 and 2.26, and the first version of this package at 0.73,
1.92, 1.85 and 2.01.

Throughput of the library on this machine moves with things the caller does not
control: the alignment of the buffer (up to 20 percent), the toolchain (a binary
built with Go 1.25 ranks the library's one-shot and streaming calls the other way
round from one built with Go 1.26), and the shape of the call: through a function
variable it measures 15 to 20 percent slower than called directly. That is why `Sum`
on amd64 is a plain call to the library (`library_amd64.go`), which the compiler can
inline, and not a value the package assigns at start-up.

`Parallel`, one buffer, N workers on 8 cores (best of 5):

| workers | 1 MiB | 16 MiB |
|---|---|---|
| 1 (`Sum`) | 2.65 | 2.61 |
| 2 | 2.90 | 3.65 |
| 4 | 5.03 | 7.16 |
| 8 | 4.95 | 14.35 |

AVX-512 is **untested**: neither the machine nor the emulator available here has
it. On amd64 the only code that can reach it is `Parallel` and `TestKernel` (the
`guts` kernel selects it at run time when the CPU has it); `Sum` and `Hasher` use
the library, which has no AVX-512 code. The repository's CI runs
`go test -race ./...` on an amd64 runner, which includes `internal/blake3`; the log
does not say whether that runner has AVX-512. darwin/arm64 is also untested; the
NEON kernel is the same code, but the assembly has only been run on Linux.

To measure on the fleet:

```
go test -run '^$' -bench 'Sum|Stream|Parallel|Concurrent' -count 10 ./internal/blake3
```

pin the process to one core with `taskset -c N`, and compare the best ns/op of
`Sum/this` with `Sum/zeebo` and `Sum/luke`.

### Where the NEON kernel stands

The kernel is limited by the length of the dependency chain, not by throughput:
one round of the compression is about 30 dependent vector operations, and the core
issues them at about 2.4 per cycle where it could issue 6. Four lanes give four
independent chains; filling the core would take eight, which does not fit in the
32 vector registers with the message and the rotation scratch. Two measured steps
got what the registers allowed: the byte permutation (`TBL`) for the rotate by 8
(+4 percent), and a software prefetch of the next group of chunks (+45 percent at
16 MiB, where the hardware prefetcher loses the four short streams). Two ideas are
left: a fused xor-and-rotate (the SVE2 `XAR`) shortens the chain by half but is
available on some cores only; an interleaved eight-lane kernel with the state
partly in memory. Neither is needed to beat the library by 2.4x.

## Design

`blake3.go` (API), `node.go` (the tree), `compress.go` (the portable compression),
`kernel_arm64.{go,s}`, `kernel_amd64.go` (the `guts` kernel for the tree),
`library_amd64.go` (the library on amd64), `sum_other.go` (`Sum` elsewhere).

The tree hash is portable Go and is the same on every architecture. It calls one
function, the **kernel**, for every run of up to sixteen chunks, and the kernel
returns the topmost node of that run: the last block of the chunk if there is one
chunk, otherwise the parent whose block is the chaining values of its two children.
Everything above sixteen chunks (a parent per 16 KiB) is Go. A kernel is allowed to
be anything that returns the same node; the portable one is the test oracle for the
others.

The arm64 kernel (`hash4`) compresses four chunks at once. It loads 16 bytes from
each of the four, transposes them so that vector register *i* holds message word *i*
of all four, and runs the seven rounds on all four; after the 16 blocks it transposes
the four chaining values back. The same function hashes four parents at once (one
block each) when a level has four or more pairs. Message words are read from a small
transposed scratch block in memory because the state takes 16 registers and the
rotations 8; the chaining value lives in 7 registers and one memory slot.

`Parallel` splits the buffer into jobs of 128 chunks, each a complete subtree, uses
at most half as many workers as there are jobs (a worker with one job loses what it
costs to start), hands the jobs to the workers (the caller's included) from an atomic
counter, and joins the chaining values of the jobs in the order the tree would.

`Sum` is defined once per architecture: a call to the library in `library_amd64.go`,
a call to the tree in `sum_other.go` (every other architecture). `Hasher` makes its
state through a variable, `stream`, that `library_amd64.go` replaces in `init`. The
zero value of `Hasher` is an unkeyed hasher: it makes its state on first use.

## Integration plan (for the PR that replaces the library)

The engine's call sites are not edited here. Each of them moves to this package with
the change in the right-hand column, and gives the same bytes (H7):

| call site | library today | this package |
|---|---|---|
| chunk id (`chunk.go` `sumChunk`), shard ref (`shard.go`), key id (`cipher.go` `ageKeyID`), `pkg/backend/file` | `blake3.Sum256(p)` | `blake3.Sum(p)` |
| seal context (`head.go` `bind`) | `blake3.New()`, `h.WriteString`, `h.Write`, `h.Sum(nil)` | the same calls |
| gear table (`chunk.go` `newGear`) | `h.Digest().Read(raw[:])` | `h.Expand(raw[:])` |
| tree boundaries (`tree.go` `boundaryKey`, `ends`), extension names (`ext.go` `extRest`), seal tag (`cipher.go` `tagKey`, `tag`) | `blake3.DeriveKey(context, material, key[:])` | the same call |
| the keyed hashes of those three | `h, _ := blake3.NewKeyed(key[:])` | `h := blake3.Keyed(&key)` (no error: the key is an array) |
| the pooled hasher of `ends` | `h.Reset()`, `h.Write`, `h.Sum(sum[:0])` | the same calls |

1. The gear table in `chunk.go` is derived from the seed with the domain string,
   the seed and the extendable output, in that order. The recipe is the engine's own
   `newGear`, and `TestGearRecipe` requires its bytes for 12 seeds, on every path:

   ```go
   h := blake3.New()
   h.WriteString(domain)      // "hanzoai/vfs chunker gear table", as today
   h.Write(seed[:])           // the Volume's seed is hashed, not overwritten
   var raw [256 * 8]byte
   h.Expand(raw[:])           // the table: 2048 bytes of output
   ```

   Passing the seed to `Expand` instead would overwrite it and leave the table
   unfilled, the same for every Volume, and every chunk id would move.
2. `github.com/zeebo/blake3` stays in `go.mod`: on amd64 it is the implementation of
   `Sum` and `Hasher`, and the tests use it as an independent one.
3. Hash chunks on the engine's bounded worker pool, one `Sum` per chunk. Use
   `Parallel` only where one buffer of a megabyte or more is hashed alone.
4. The on-disk format does not change: the digests are identical, so every chunk id,
   shard name, cut, boundary and extension name already written is still valid.
5. The image carries `THIRD_PARTY_NOTICES` and `LICENSE` in `/usr/share/licenses/vfs`.
   The notices list the modules the binary links today and what `internal/blake3`
   will link once the engine imports it; make the list again with the command in the
   file when the dependencies change.

## Tests and CI

`go test -race ./internal/blake3` takes about 11 seconds on an arm64 workstation and
the amd64 tests run natively in the same time. The repository's gates run
`internal/blake3` and `internal/bufpool` as the gate `internal` (`hanzo.yml`), under the
race detector, beside the others.

## Limits

- **The race detector does not see the assembly kernels.** The NEON kernel (and the
  `guts` AVX2 kernel) read their input in assembly, which `-race` does not
  instrument: a real race between `Sum(buf)` and a writer of `buf` is reported with
  the portable kernel and silently missed with the native one. A use of a buffer after
  it went back to a pool can therefore pass `go test -race` unnoticed. The buffer
  layer checks its sharing rules by ordering, not by the detector
  (`TestHashOfPinnedSlice`, `TestCacheEvictionUnderReader` in `internal/bufpool`).
- **Untested hardware:** AVX-512 and darwin/arm64 (above).
- **amd64 speed** is the library's, which is below the official C implementation.

## Specification notes

These are the normative statements; the tests named in each acceptance line decide
them.

**H1. Digest.** `Sum(p)` is the 32-byte BLAKE3 hash of `p`, as the BLAKE3
specification defines it, for every length from 0 to 2^64-1 bytes. A `Hasher`
that has been written the bytes of `p`, in any split into calls of any lengths
including zero, returns the same 32 bytes from `Sum`, and `Expand(out)` fills `out`
with the first `len(out)` bytes of the specification's extendable output, whose
first 32 bytes are the digest, for every `len(out)` including 1. A keyed `Hasher`
returns the specification's keyed hash under its 32-byte key. `DeriveKey(context,
material, out)` fills `out`, of any length, with the specification's key derivation
output for the context string and the material. Acceptance: every case
of the official test vectors (inputs of 0 to 102400 bytes) passes through `Sum`,
`Parallel`, `Hasher`, keyed `Hasher` and `DeriveKey` with 131 bytes of output, on each of the
exported functions, the tree with the kernel of the architecture, and the tree with
the portable kernel (`TestVectors`); every output length from 0 to 130 bytes is
exact and writes nothing past its end (`TestExpandLengths`). On amd64 `Sum` and
`Hasher` run in the library and `Parallel` in the tree, which `TestAMD64Routing`
checks by counting the calls to the kernel.

**H2. Cross-checking.** The package agrees with an independent implementation for
random contents at every length from 0 to 4224; at 1024k+d for d in {-65, -64, -1,
0, 1, 63, 64, 65} and every k from 5 to 70, every k within one of a power of two, and
every 31st k, up to 256 KiB; at 16 KiB times a power of two, plus d for d in {-129,
-128, -65, -64, -63, -2, -1, 0, 1, 2, 63, 64, 65, 127, 128, 129}, up to 256 KiB; at
24 further random lengths up to 1 MiB; at 1 MiB + 1, 4 MiB + 12345 and 5 MiB on the
paths with a native kernel; and for streams cut into pieces of 1, 2, 63, 64, 65, 1023,
1024, 1025, 2048, 4097, 16384, 16385 and 33000 bytes and into 200 random cuts
(`TestAgainstReference`, `TestRandomLengths`, `TestLarge`, `TestStreaming`,
`FuzzHasher`), on the three paths of H1. A differential against the official C
implementation over every length to 20000, 6,111 boundary lengths to 64 MiB, keyed and
extended output, with guard pages, was run by review and is not in the repository,
because it needs a C compiler.

**H2a. Reads.** No function reads outside the slice it is given, whatever the length,
including the lanes the NEON kernel pads when a group has fewer than four chunks:
each input is placed against an inaccessible page with capacity equal to length, and
a read past the end faults. Acceptance: `TestGuardPages` (Linux), for `Sum`, the
tree, the streamer, `Hasher` and `Parallel` at every length to 3*16 KiB+2000 in steps
and at several megabytes.

**H3. Kernels.** Every kernel returns the same node as the portable one for every
length from 0 to 16 KiB, at chunk counters 0, 1, 5 and around 2^32, 2^40 and 2^64
(those that carry into the high 32 bits of the counter included), and for keyed and
unkeyed input. On arm64 the kernel's block is checked lane by lane against the
portable compression for 1, 2, 3, 16 and 40 blocks per lane and for flags and keys
the tree never produces (`TestKernel`, `TestHash4`).

**H4. State.** `Sum` and `Expand` on a `Hasher` do not change it: more bytes may be
written after either and the digest is that of all of them. `Reset` returns it to
the state of a new `Hasher` with the same key. The zero value of `Hasher` is an
unkeyed `Hasher`: it hashes as `New()` does, before and after `Reset`; hashing the
three bytes "abc" gives the digest 6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85
(`TestZeroAbc`). A `Hasher`
is not safe for concurrent use; `Sum` and `Parallel` are, since they share nothing
between calls. Acceptance: `TestSumKeepsState` (a prefix at every step, plain and
keyed, then `Reset`), `TestZeroValue`, and `TestConcurrentSums` under the race
detector.

**H5. Parallel.** `Parallel(p, w)` returns exactly `Sum(p)` for every `w`, including
`w` below 2, which hashes on the calling goroutine alone. The jobs are 128 chunks
(128 KiB). It uses at most the smaller of `w` and half the number of jobs as workers,
the caller included, and with fewer than two it is `Sum`. When it returns, no
goroutine it started is still reading `p` (it may not yet have exited); it does not
modify `p`. The join splits the list of job values at the largest power of two below
its length, as the tree splits chunks, so the result does not depend on how the jobs
were scheduled. Acceptance: `TestParallel` over 15 lengths around the job boundaries
and 6 worker counts on the three paths; `TestParallelBounded`, which wraps the kernel
to count the calls in flight and requires at most `w` of them for 64 jobs and at most
2 for 4 jobs with a worker count of 200,000; and `TestParallelStartsNoMoreThanJobs`,
which samples the goroutines alive; all under the race detector.

**H6. Failure.** No function returns an error or panics for any byte input of any
length; `Keyed` requires a non-nil key. `Hasher.Write` always returns `len(p), nil`.
Acceptance: `TestStreaming` checks the result of every `Write`; `FuzzHasher` runs
arbitrary data and cut sizes through every entry point and fails on a panic.

**H7. The modes the volume layout uses.** The layout (docs/SPECIFICATION.md of the
volume) uses BLAKE3 in four ways, and this package gives the bytes the library the
engine links gives for each: the plain hash with 32 bytes of output (chunk ids, shard
refs, key ids, the seal contexts); the extendable output of the plain hash (the gear
table, 2048 bytes over the domain string and the seed); the keyed hash under a key
that is itself derived (the tree boundaries and the extension names, both over a key
derived from the Volume's seed); and the key derivation mode (that derivation, and the
key of the seal tag). Acceptance, with the derivations written from the text of the
layout over this package and compared with the same derivations over the library, for
random seeds and every path: `TestSpecDerivations` (gear table, boundaries, extension
names, seal contexts and tags, chunk ids), `TestSpecChunker` (the cut of a window with
the table of this package), `TestSpecExtensionNames` (sixty vectors that a third BLAKE3
library computed from the text alone, `testdata/extnames.json`, the file the layout's
own test uses), `TestGearRecipe` (the engine's `newGear`, line for line) and
`TestDeriveKey` (every context, material and output length around a block).

## References

- J. O'Connor, J.-P. Aumasson, S. Neves, Z. Wilcox-O'Hearn, "BLAKE3: one function,
  fast everywhere", 2020. Specification and reference implementations (Apache-2.0
  or CC0-1.0): the `BLAKE3-team/BLAKE3` repository. The official test vectors in
  `internal/blake3/testdata/test_vectors.json` are from it.
- The NEON kernel is derived from its `c/blake3_neon.c`; the attribution is in
  `kernel_arm64.s` and `THIRD_PARTY_NOTICES`.
- `lukechampine.com/blake3/guts` (MIT) is the amd64 kernel of the tree, used by
  `Parallel`; `github.com/zeebo/blake3` (CC0) is the amd64 implementation of `Sum`
  and `Hasher`.
