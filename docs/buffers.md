# Buffers

Where the write path of the engine spends its allocations and copies, and
`internal/bufpool`, the pool that removes most of them. Refs #22.

## What the engine does today

Profiled on the engine of PR #8 (head 264b91c, since merged), driven through its Go API with the
shapes of the benchmark harness (no kernel file-system layer), against a backend that
keeps the small objects and drops the packs. Sealing is the engine's hybrid
post-quantum recipient. 8 cores of an arm64 machine at load average 21 to 28; the
shares below are of CPU samples and of bytes, which that load does not change.

| workload | written | allocated | per byte written | mallocs |
|---|---|---|---|---|
| sequential, 256 MiB in 128 KiB writes, one flush | 256 MiB | 4,063 MiB | **15.9x** | 96 thousand |
| small files, 3,000 of 1 to 16 KiB, a flush after each close (as the mount does) | 24 MiB | 8,919 MiB | **359x** | 11.3 million |
| small files, one flush at the end | 24 MiB | 304 MiB | 12x | 178 thousand |
| random 4 KiB writes, 20,000 over a 256 MiB file, one flush | 80 MB | 7,921 MiB | **101x** | 370 thousand |
| random 4 KiB write, 300 times, a flush after each | 1.2 MB | 6,310 MiB | 5,384x | 831 thousand |

### Allocation sites

Share of the engine's allocated bytes (the benchmark's own data and its backend are
left out). `bytes.growSlice` is the buffer the cipher writes into, growing by
doubling as it fills.

| site | what it does | sequential | random | small, flush per close |
|---|---|---|---|---|
| `Volume.write` | a write is copied into the file's dirty span, which grows by append | 39.7% | 12.9% | |
| `pack.add` | the sealed frame is copied into the pack, which grows to 16 MiB by append | 29.4% | 30.9% | |
| cipher `Seal` (`bytes.growSlice`) | ciphertext accumulates in a growing buffer | 18.6% | 18.6% | 29.7% |
| `seal` | the frame (header, key id, ciphertext) is built in a new slice and copied | 5.9% | 6.0% | 8.7% |
| `putChunk` | a copy of the plaintext for the chunk cache | 5.8% | 5.9% | |
| cipher `Open` (`io.ReadAll`, `readChunk`) | the plaintext of a read chunk grows by doubling | | 12.7% | |
| `recut` | a 4 MiB window made on every settle | 0.4% | | |
| shard `writer.raw`, `builder.add` | metadata nodes encoded into growing buffers | | | 46.5% |
| `tree.ends` | one slice per key range | | | 70% of the objects |

The pack buffer alone allocates 3.8 bytes for each byte it holds, because Go grows
a large slice by a quarter and copies each time. Two more shares: with one flush
after the small files, `Volume.write` is 10.5% and `settle` 10.4%; with a flush
after each of 300 random writes, the 4 MiB `recut` window is 10.8% and the old chunk
read back by `Open` 22%.

### Copies and CPU

`runtime.memmove` and its siblings, share of CPU samples, with who calls it:

| workload | memmove | memclr | pure-Go BLAKE3 | cipher | GC scan |
|---|---|---|---|---|---|
| sequential | 37.5% | 5.0% | 15.6% | 16.9% | |
| random, one flush | 24.4% | 6.9% | 25.4% | 19.3% | |
| random, flush each | 10.7% | 6.6% | 31.5% | 23.8% | |
| small, flush per close | 12.1% | 3.8% | 18.2% | 8.7% | 15.7% |

Callers of the sequential `memmove`: slice growth 52% (the dirty span 28%, the pack
27%), reading the dirty span into the recut window 35%, the other copies 13%. On
random writes the largest caller is `readBase`, which copies the old chunk into the
window before the overwrite is applied (33%), then slice growth (21%).

The copies a byte makes between the kernel's write buffer and the backend, for one
chunk: into the dirty span (1), into the recut window (2), into the cipher's buffer
(3), into the frame (4), into the pack (5), into the cache (6), and every growth of
the span, of the cipher's buffer and of the pack copies the bytes held so far again.
The minimum is two (into the span, and into the pack as ciphertext) plus the cache's
own copy.

## bufpool

`internal/bufpool` has three parts.

- **`Budget`**: the idle memory of every `Pool` drawn from it. `NewBudget(limit,
  decay)` holds at most `limit` idle bytes in all its pools, and lets go of a slice
  that has been idle for `decay`. When the limit is reached the slice that has been
  idle longest goes, whatever its pool or class, so a workload that changes chunk
  size is not locked out by the sizes it used before. One process-wide `Budget` is
  the intended use: 128 volumes with two pools each draw on one cap, not 128 times
  384 MiB. Decay is driven by one deadline timer that is armed only while some
  slice is idle and stopped when none is; there is no ticker.
- **`Pool`**, from `Budget.Pool(zero)`: size classes a quarter of a power of two apart
  (4 KiB to 64 MiB), so a slice is at most 25 percent larger than asked for. `Get(n)`
  returns a slice of length `n`; `Put(b)` takes back **only what `Get` gave out**: it
  records every slice it hands out, and refuses a slice that is not exactly one of
  them (same start, same capacity, same pool), a view of one, one that is already
  back, or one the collector has taken. A refused slice is left alone, not cleared and
  not kept, and counted in `Stats().Refused`. With `zero`, every slice is cleared in
  full when it is taken back, so a `Get` sees zeros and no plaintext outlives its
  owner. `Close` lets go of the pool's idle slices when a volume ends.
- **`Buffer`** and **`Shared`**: a `Buffer` is an `io.Writer` that grows through the
  pool. `Bytes()` is a view of its storage and dies at the next growing `Write`, at
  `Reset` (which clears a zeroing pool's bytes) and at `Release` (which is
  idempotent). `Detach()` gives the bytes and their storage to the caller, who owns
  them and puts them back or shares them. `Share(b)` makes a `Shared`, a slice held by
  an owner and any number of readers: readers `Pin` it (which fails once it is gone)
  and `Unpin`; the owner `Release`s it once; it goes back to the pool when the owner
  and the last reader are done.

The pool cannot stop an owner from using a slice after putting it back; that is a
property of Go memory. What it can do is make the wrong `Put` impossible and give the
right sharing a type, which is what `Shared` and `Detach` are for. See the limits.

Cost: a `Get` and `Put` of a warm slice take 0.2 to 0.4 microseconds (`BenchmarkGetPut`);
on a zeroing pool the clear adds about 5 microseconds per MiB, which is the price of
not leaving plaintext in a free list.

Bound: idle memory is bounded by the `Budget`. Memory in flight is not counted;
it is bounded by the pipeline that owns the slices (the dirty limit, the packs being
uploaded, the workers times the largest chunk). That keeps the pool free of any wait,
and so of any deadlock.

### Effect, on a pipeline shaped like the engine

`BenchmarkPipeline` in `internal/bufpool/pipeline_test.go` carries 256 MiB through
the engine's write path in 128 KiB writes with a 64 MiB dirty limit, chunks of 64 KiB
to 4 MiB (mean 580 KiB), BLAKE3 ids, the hybrid recipient, 16 MiB packs and a 32 MiB
chunk cache: once with the buffers the engine allocates today and once with the pool.
The cut points, the hash, the cipher and the upload are the same; a test
(`TestPipelinesAgree`) requires the two to produce the same chunks in the same order
and every frame of both to open to the plaintext whose hash is its id.

Interleaved, median of 12 rounds, per 256 MiB (`BenchmarkPipelineInterleaved`; arm64
on 10 cores at load average 17, amd64 on 8 threads at load average 12):

| | wall | process CPU | allocated |
|---|---|---|---|
| arm64, today | 0.98 s | 0.99 s | 4,160 MB |
| arm64, pooled | 0.65 s (**-34%**) | 0.64 s (**-35%**) | **105 MB** (-97.5%) |
| amd64, today | 2.28 s | 2.37 s | 4,160 MB |
| amd64, pooled | 0.88 s (**-61%**) | 0.96 s (**-60%**) | **105 MB** (-97.5%) |

The allocated bytes fall by 97.5 percent and the time by a third to a half on arm64
(34 percent here; 47 percent in a review run at load average 14, where the baseline
was slower) and 60 percent on amd64, with the same cipher cost. What remains allocated is inside the
cipher (key schedule, small objects) and the slice of chunk ids. The plain
`BenchmarkPipeline` (baseline then pooled, three runs each) measures the same
allocation but a time gain that depends on what else the machine was doing: 27
percent at the median of three on a busy arm64 machine. The collector runs in the
baseline's wall time on spare cores, so load changes the baseline more than the
pooled run; use the interleaved figure.

Run them with

```
go test -run '^$' -bench 'Pipeline' -benchmem -benchtime 6x -count 3 ./internal/bufpool
```

`BenchmarkPipelineInterleaved` alternates the two pipelines (order flipped each round)
so that load on the machine falls on both, and reports wall time, process CPU time and
allocation as medians.

## Integration plan (for the PR that wires it into the engine)

One process-wide `Budget`, and per `Volume` two pools: `plain` (zeroing) for everything
that holds file bytes, and `sealed` (not zeroing) for ciphertext; `Close` on both when
the volume ends.

| site | change | owner of the slice |
|---|---|---|
| `fileState.dirty` spans, `Volume.write` | a span's storage is a `plain.Buffer`; append and merge grow through the pool | the file state, until the span is settled, truncated away, or the file is removed |
| `Volume.recut` window, `Volume.read` | when a chunk lies inside one span and no settled chunk overlaps, `recut` slices the span (no copy); otherwise it takes a `plain.Get(min(size, maxChunk))` window for the length of the recut | the recut |
| `putChunk` | the plaintext is borrowed for the call: hashed, sealed, copied for the cache | the caller of `putChunk`; `putChunk` must not retain it |
| `seal` and the cipher | the cipher writes its output to an `io.Writer` (the pack's buffer) and reports its key id beforehand, so the frame header goes in first and the ciphertext follows in place: the `Cipher` interface gains `KeyID() string` and `SealTo(dst io.Writer, plaintext []byte) error`; no intermediate buffer, no frame copy | the pack |
| `pack.add`, `pack.upload` | the pack is a `sealed.Buffer` sized `packTarget + maxChunk + 64 KiB` so it never regrows; after `Backend.Put` returns it is `Reset` and reused | the pack; on a failed `Put` the data stays for the retry, nothing is released |
| chunk cache (`Volume.chunks`) | the entry is a `Shared` over a `plain.Get(n)` copy; eviction `Release`s it; a reader `Pin`s before it copies and `Unpin`s after | the cache, with its readers |
| `getChunk` read path | `Cipher.OpenTo(dst, sealed)` with `dst = plain.Get(extent.n)` so the plaintext is written once at its final size; the ciphertext from `GetRange` goes into a `sealed.Get(w.n)` via a `GetRangeInto(dst)` on the backend | the cache for the plaintext; the call for the ciphertext |
| shards: `node.encode`, `shards.store` | the encoder writes into a `plain.Buffer`, the sealed shard goes to a `sealed.Buffer`, both released after `Put` | the call |
| `settle` inline files | unchanged; they are small and live in the overlay | the overlay |
| `tree.ends` | not a buffer: one slice per range; keep one scratch slice per builder | the builder |

Expected effect on the profiles above: the sites listed remove 90 percent of the
allocated bytes of the sequential and random workloads and the matching share of
`memmove` and `memclr`; the small-file workload with a flush per close is dominated
by the shard encoder and by `tree.ends`, and needs the commit changes of the journal
work as well.

### What the engine PR must do

These are obligations, not suggestions; each has a test in this package that shows
the failure it prevents.

1. **The chunk cache pins its entries.** `lru.get` returns the shared slice and drops
   its lock, `getChunk` runs under the read lock so any number of readers run at once,
   and a reader's miss calls `chunks.add`, which evicts. An entry that goes back to
   the pool on eviction is read, or overwritten, by a reader that has not finished
   copying (a recycled entry was read back as another chunk's bytes). Entries are
   `Shared`: readers `Pin` before copying and `Unpin` after; eviction `Release`s.
   (`TestCacheEvictionUnderReader`, `TestCacheConcurrent`)
2. **A span is taken, never viewed.** `Buffer.Bytes()` dies at the next growing write,
   at `Reset` and at `Release`: the old array goes back to the pool and is cleared or
   reused. The journal's immutable generation, and anything else that holds file
   bytes while writes continue (recut, hash, seal, upload), must `Detach` the span and
   own it, and the file state starts a new `Buffer`. Never `Grow`, `Reset` or `Release`
   a span while a view of it, a hash of it, or a generation made from it is live.
   (`TestBufferViewDiesOnGrow`, `TestBufferDetach`)
3. **One process-wide `Budget` with decay**, shared by all volumes; a pool per volume
   is closed with the volume. (`TestGlobalCap`, `TestDecay`, `TestClassLockout`)
4. **A backend contract test: no retention.** `Put`, `PutIfAbsent` and `PutIfVersion`
   must not use `data` after they return, including after the call is cancelled or
   fails. The engine PR adds to the backend suite a test that, for each backend,
   calls `Put` and then overwrites `data` and checks the stored object is unchanged,
   and does the same with a context cancelled mid-call, under `-race`. The file and
   S3 backends comply; a backend that uploads in the background copies first.
5. **`Put` and `Release` sit behind owners that cannot release twice.** The pool
   refuses a second `Put` and `Buffer.Release` and `Shared.Release` are idempotent,
   so a double release is harmless, but the engine still gives each slice one owner
   (the pack, the file state, the cache entry), and an error path that releases and
   retries goes through that owner, not through a copy of the slice.
6. **Nothing pooled crosses the public API.** `File.ReadAt` copies out of the cache;
   no method returns a slice the engine will recycle.

### Ownership rules

1. A pooled slice has one owner. The owner calls `Put` exactly once and does not use
   the slice afterwards. A slice with several holders is a `Shared`.
2. A slice lent to a callee is read-only for the call; a callee that wants the bytes
   later copies them.
3. `Backend.Put`, `PutIfAbsent` and `PutIfVersion` do not retain `data` after they
   return (obligation 4).
4. Plaintext lives only in `plain` slices; the pool clears them on return, whether
   they are kept or dropped.
5. Memory in flight is bounded by the pipeline: dirty bytes by `Config.Dirty`, packs
   by the uploads in flight times the pack size, windows by the workers times
   `maxChunk`.

## Limits

- **A use after `Put` cannot be prevented, and the race detector may not see it.**
  The pool makes the wrong `Put` impossible; it cannot make a stale reference to a
  slice that went back safe. The hash kernels read their input in assembly, which
  `-race` does not instrument, so a real race between `Sum(buf)` and a writer of `buf`
  (a recycled buffer being refilled) is reported with the portable kernel and silently
  missed with the native one. The tests of this package therefore check the sharing
  rules by ordering: a reader pins, the owner releases, another owner asks for the
  same memory, and the reader must still see its bytes (`TestHashOfPinnedSlice`,
  `TestCacheEvictionUnderReader`, `TestShared`).
- **Provenance is by address and a weak reference.** A record is kept per slice
  handed out and dropped when the collector takes the slice (swept in batches); a
  slice of another pool, or an address reused after a collection, is refused.
- `Close` and the `Budget` forget a slice; one still out is then not tracked, and
  putting it back is refused.

## Specification notes

**B1. Classes.** A request of `n` bytes, `0 < n <= 64 MiB`, is served by a slice
whose capacity is the smallest class size not below `max(n, 4 KiB)`. The class sizes
are 4 KiB and, for each power of two `2^e` from 4 KiB to 32 MiB, `2^e + j * 2^e/4`
for `j` from 1 to 4. A slice is never more than a quarter larger than asked for
(above 4 KiB). A request above 64 MiB is allocated and never kept. `Get(0)` is nil.
Acceptance: `TestClasses`, `TestOversize`, `TestLargestClassIsKept`.

**B2. Reuse.** A slice returned with `Put` is handed out again by a later `Get` of
its class from the same pool; the returned slice has the requested length and the
class capacity. Acceptance: `TestGetPut`.

**B3. Provenance.** `Put` accepts a slice if and only if it is one that `Get` of the
same pool gave out, with the same start and the same capacity, that is not already
back, and that the pool still tracks. Otherwise it does nothing to the slice or to any
other memory, and counts the refusal. In particular a view of a pool slice (any other
start or capacity, including a three-index view with a class-size capacity), a slice
from another pool or from `make`, a second `Put` of one slice, and a slice whose
record the collector has dropped, are refused and no byte of any of them or of their
parent is changed. Acceptance: `TestRefusesWhatIsNotOurs`, `TestProvenance`, `TestDoublePut`,
`TestConcurrentDoublePut` (sixteen goroutines put one slice; one succeeds),
`TestCollectedSliceIsRefused`, `TestRegistryIsSwept`, `TestPutThenClose` and
`TestPutCloseRace` (a `Put` racing `Close` leaves nothing idle), `TestUseAfterClose`.

**B4. Clearing.** On a pool made with `zero`, every accepted slice is cleared in
full, to its capacity, before `Put` returns, whether it is then kept or let go; a
`Get` on such a pool returns zeros. A pool without `zero` does not clear. `Buffer.Reset`
on a zeroing pool clears the bytes that were written, and so do a growth, `Release`
and the last unpin of a `Shared`. Acceptance: `TestZero`, `TestResetClearsPlaintext`,
`TestZeroOnReturn`.

**B5. Exclusive ownership.** Between a `Get` and its `Put`, no other `Get` returns
any of the same memory, under concurrent use. Acceptance: `TestConcurrent` under the
race detector, where every goroutine stamps its slice, yields, and verifies the stamp.

**B6. Budget.** The idle bytes of all pools of a `Budget` never exceed its limit; when
a `Put` would exceed it the slice idle longest, of any pool and class, is let go until
it holds; a limit of 0, or a negative one, keeps nothing. The accounting of the budget
and of its pools agrees with a naive model over long random runs of gets, puts, wrong
puts and the passage of time (`TestModel`). Acceptance: `TestLimit`, `TestNegativeLimit`, `TestZeroLimit`, `TestEvictsOldestAcrossPoolsAndClasses`,
`TestClassLockout` (after a phase of 4 MiB slices fills the budget, 999 of 1000 Gets of
1.25 MiB are served from the pool), `TestGlobalCap` (eight pools of 16 slices of 4 MiB
against 64 MiB).

**B7. Decay.** A slice idle for the decay time is let go; at most one timer exists, it
exists only while some slice is idle, and a timer that was stopped and fires anyway does
nothing. A decay of 0 never drops and never arms a timer. Acceptance: `TestDecay`,
`TestDecayStopsWhenTaken`, `TestNoDecay`, `TestEvictAllDisarms` (an eviction that empties the idle set stops the timer), `TestTimerStorm`, with a controlled clock
and timer.

**B8. Buffer, Detach and Shared.** Writes append; `Bytes` returns exactly the bytes
written so far; growth preserves them, is geometric (about ten slices from the pool for
256 KiB written a byte at a time), and returns the outgrown slice to the pool; `Reset`
keeps the storage; `Release` returns it and is idempotent; bytes taken with `Detach`
are not changed by any later use of the Buffer; a `Write` of a view of the Buffer's
own bytes appends those bytes whether or not it grows the Buffer. A `Shared` goes back to the pool when
the owner has released it and the last reader has unpinned it, not before; `Pin` fails
after that; the owner's `Release` is idempotent. Acceptance: `TestBuffer`,
`TestBufferAmortized`, `TestBufferGrowPreserves`, `TestBufferDetach`, `TestShared`,
`TestSharedConcurrent`, `TestSharedStress`, `TestPinCounts` (an unpin after the end
changes nothing), `TestBufferSelfWrite`, `TestCacheEvictionUnderReader`, `TestHashOfPinnedSlice`.

**B9. Pipeline equivalence.** A write path that takes its buffers from the pool
produces the same chunk ids, in the same order, and frames that open to the same
plaintext, as one that allocates them. Acceptance: `TestPipelinesAgree`.

## References

- Xia et al., "FastCDC: a fast and efficient content-defined chunking approach for
  data deduplication", USENIX ATC 2016: the chunker whose output sizes the pipeline
  uses.
- J. Bonwick, "The Slab Allocator: An Object-Caching Kernel Memory Allocator",
  USENIX Summer 1994: the idea of recycling fixed-size objects instead of allocating.
