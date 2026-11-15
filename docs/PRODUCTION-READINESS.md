# Production readiness of a Volume

Everything here was measured by the test named beside it; nothing is a claim without
one. The layout and protocols themselves are stated in `SPECIFICATION.md`. The previous engine measured 6.4 fsync/s on other hardware; that number is kept
only as history, since the code it described is gone.

Machine: GB10 arm64, 20 cores, Go 1.27.1, shared with other jobs (load average 8 to
16 during the runs, so throughput is a floor, not a peak). The file backend sat on
tmpfs, so its own fsync costs nothing: the figures are the engine's (chunking,
hashing, sealing, shard and Head writes), not a disk's. The real-server figures are a
Hanzo S3 server on the loopback interface, also on tmpfs.

## What is durable when a call returns

"Commit" is the Head advancing in the store: the conditional PUT of the Head has
returned success. This table is also in the package documentation (`doc.go`).

| Call | Durable when it returns |
|---|---|
| `File.WriteAt`, `Truncate`, `Create`, `Mkdir`, `Remove` | nothing. Held in memory and lost if the process dies. Past `Config.Dirty` bytes the data is chunked and uploaded, still unreferenced, still not durable |
| `File.Close` | nothing. It releases the handle; it is not a flush |
| `Volume.Flush` | everything written before the call, committed. It returns only after the Head advanced |
| Fsync on the mount (`FSYNC`) | the same as `Flush`: a full commit. There is no cheaper durable point. A caller interrupted by a signal is answered `EINTR` and the commit still lands |
| Close on the mount (`FLUSH`, every `close(2)`) | the same as `Flush`: a full commit. An interrupted close waits for the commit and returns its result |
| Release on the mount | nothing |
| End of a mount | one `Flush`, then the mount returns |
| `Volume.Close` | nothing. It releases the backend and does not flush: the detach sequence is `Flush`, then `Close` |
| `Attach` (takeover) | every commit the previous owner saw acknowledged is in the new owner's tree, and no commit by an older Round lands afterwards |

Interrupts on the mount. A signal that reaches a caller while it waits on the mount
makes the kernel interrupt the request (Go's runtime sends one every few milliseconds
to preempt). What the answer `EINTR` costs depends on what the caller does with it,
and it was measured: `fsync(2)` is restarted by libc and by Go, so it stops waiting
and is answered `EINTR` while the commit runs on by itself on a context no request
owns; `read(2)` answered `EINTR` reached the caller as `EIO` (20 GiB of Go reads
under signals failed at the first block), so a read waits for its data; `close(2)` is
never restarted (Go's `os.File.Close` returns the error), so a close waits for its
commit; a request that changes the tree runs to the end, since a restarted `create(2)`
with `O_EXCL` would meet its own file. No caller's interrupt cancels a commit or a
fetch.

What a failed `Flush` tells the caller. `nil`: committed. Any other error: the outcome
of that attempt is unknown (a reply can be lost after the write landed) and the
changes stay in memory for the next `Flush`. `ErrStaleRound`: this Volume was fenced
and can never commit again; a commit whose acknowledgement was lost may already be in
the successor's tree, and `Flush` reports `nil` when the store's Head is exactly this
Volume's tree (`TestLostAckThenTakeoverIsNotReportedLost`).

What a later change (a node journal for fast fsync, group commit) must keep. `Flush`
and the detach sequence return only after the Head advanced in the store. A takeover
loses no acknowledged commit. Objects are written once under names that make
repeating a write harmless (a hash of the content, or a random id), so a write that is
repeated or completes after its caller gave up changes nothing. The Head
compare-and-swap stays the only mutation. A journal may make a call cheaper only by
making it durable somewhere else first, and then `Flush` must still be a commit in the
store.

### The kill matrix

A process killed with SIGKILL, or a store call made to fail, at each point of a
commit; then the store is read from another process. In every row the fresh view is
the last committed tree, whole, and a retry commits the next.

| Point | Test |
|---|---|
| before the first pack upload | `TestSigkill/pack` |
| after the first pack upload | `TestSigkill/packed` |
| before the first shard upload | `TestSigkill/shard` |
| after the first shard upload | `TestSigkill/sharded` |
| before the Head write | `TestSigkill/head` |
| after the Head write | `TestSigkill/landed` |
| every write of a commit failing in turn, by kind: the pack, each shard, the Head | `TestFailureAtEveryWrite` |
| Head landed, reply lost, then retry | `TestLostAcknowledgementOfHead`, `TestLostAckNoLostUpdateNoDuplicate` |
| Head landed, reply lost, then takeover | `TestLostAckThenTakeoverIsNotReportedLost` |
| takeover during a stream of commits | `TestTakeoverLosesNoAcknowledgedCommit` |
| a writer on the mount killed mid-write | `TestFUSEKillDuringWrite` |
| twenty files each acknowledged by `fsync(2)` on the mount, then the server killed | `TestFUSEFsyncSurvivesKill`: all twenty in the store |
| killed at a random moment, 15 times over a real server | `TestKillOverRealServer`: every acknowledged file present and byte-exact after each kill |

## What is guaranteed

| Concern | Status | Evidence |
|---|---|---|
| Crash at any point leaves the last committed tree, whole | pass | the kill matrix above |
| A deposed writer cannot commit; takeover loses no acknowledged commit | pass | `TestFenceRefusesOlderRound`, `TestAttachNeedsAHigherRound`, `TestTakeoverLosesNoAcknowledgedCommit`, `TestRacingClaims`, `TestAttachLostAckIsNotStale`, `TestRoundMaxIsRefused`, on the in-memory store, the file backend, an S3 protocol server, and a real Hanzo S3 server |
| Exactly one of N writers on one Head version wins | pass | the conformance suite on every backend; against the real server `TestRealServerRaceStress` and `TestRealServerCAS` (128 racers, identical bodies of 1 B, 1 MiB and 20 MiB) |
| Conditional writes are never silently unconditional | pass | `TestRealServerMultipartIsConditional`, `TestLargeConditional`: above 64 MiB (a multipart upload) a conditional write is refused with `ErrTooLarge` |
| A read returns at most what its kind of object may weigh | pass | `TestHeadBound`, `TestShardBound`, `TestBackendBodyIsBounded`, `TestReadIsBounded`: a hostile store cannot make a read allocate its way out |
| Backend outage surfaces, loses nothing | pass | `TestFailureAtEveryWrite`: an error from the store is an error from `Flush`, the changes stay in memory, the store is untouched |
| Wrong key fails closed; tampered, swapped or foreign objects are refused | pass | `TestWrongKeyFailsClosed`, `TestTamperedObjectsAreRefused`, `TestSwappedChunkIsRefused`, `TestTreeDetectsSwappedShard`, `TestChunkHashIsChecked`, `TestSealedToItsContext`, `TestHeadContextIsExact`, `TestSwappedHeadIsRefused` (a shard, chunk or Head of another Volume or place does not open under a Cipher both share; the context is a fixed-length digest compared for equality, so a Head sealed for `.../vol10` does not open at `.../vol1`) |
| A Cipher that stops sealing is caught at the first Seal | pass | `TestCipherThatStopsSealingIsCaughtAtEverySeal` |
| The recipient alone cannot forge an object | pass | `TestRecipientAloneCannotForge`: every sealed object carries a tag keyed from the secret identity, so integrity rests on the identity, not on the (public) recipient |
| A guest who picks file names cannot multiply the metadata | pass | `TestGroundNamesDoNotBloatTheTree`: names ground against another key write as many shards as ordinary ones (6 and 6; 2,007 and 4 before the boundaries were keyed from the Volume's seed) |
| Every spelling of one place is one place; a moved Volume is resealed | pass | `TestPlaceSpellings`, `TestResealMovesAVolume` |
| Interrupted fsync retries do not pile up | pass | `TestInterruptedFsyncRetriesDoNotPileUp`: 5,000 retries against a stalled store leave at most two commits, one running and one waiting |
| A mount hands the store a context on every path | pass | `TestLookedUpFileReadsOnTheMountContext` (over a store that panics on a nil context), `TestSessionRefusesANilContext`, `TestFUSEReadsExistingFilesOverS3` (a mount over the real server) |
| A Cipher that hides nothing is refused at configuration | pass | `TestIdentityCipherIsRefused` |
| Key rotation | pass | `TestRotation`: a Cipher with the new key and the old one reads everything and seals under the new; once the old is dropped, what only it sealed no longer opens |
| No key, plaintext or name on disk; the Cipher is the only door | pass | `TestNothingKeyShapedOnDisk`, `TestCipherIsTheOnlyDoor`, `TestEveryObjectIsHybridPQ`, `TestCLIRoundTrip` (the key comes from standard input; an environment variable is ignored) |
| A backend never prints a credential | pass | `TestURLCredentialRefused`, `TestURLStringNamesNoQuery` |
| A prefix cannot escape its place | pass | `TestValidPrefix`, `TestURLPrefixValidated`, `TestPrefixConfinement` (on the real server) |
| The on-disk records are extensible | pass | `TestInodeRecordIsExtensible`: a reader checks the fields it knows, carries the ones it does not, and writes them back |
| A hole costs nothing, and reads as zero | pass | `TestSparseExact` (hole, write into a hole, straddling hole/data/chunk/pack boundaries, truncate smaller than a hole then extend, overwrite then truncate, hole at EOF, many holes), `TestSparseRandom` (against a plain model across Flush, reopen and takeover), `TestChunkListIsTheWholeFileCut` |
| The tree is the same whatever order it was built in | pass | `TestTreeMatchesModel`, `TestTreeForcedCuts` (0 root mismatches in 1,200 batches at four node sizes) |
| A model of the whole API, many seeds | pass | `TestModelFuzz`, `TestModelFuzzMany` (300 seeds): random operations against a plain filesystem, with Flush, reopen and takeover; the memory bound is asserted |
| SQLite on the Volume | pass | `TestSQLiteRoundTrip`; over the mount `TestFUSESQLite` (100 rows survive release and remount, `integrity_check` ok) |
| The mount: Lookup, ReadDir (with `.` and `..`), Mkdir, Create, Remove, Setattr size, Read, Write, Flush, Fsync, Release | pass | `TestFUSEPOSIX`, `TestFUSESemantics`, `TestFUSEHostileNames`, `TestFUSEConcurrentSameFile`, `TestFUSEUnlinkVersusRmdir`; after `fsync(2)` a second read-only view of the store sees the file |
| The mount says what it cannot do | pass | `TestFUSEUnsupportedIsHonest`: rename, symlink, hard link, chmod and utimens answer ENOSYS (the kernel words link as EPERM), a file too large is EFBIG |
| A signal reaching a caller is never an `EIO`, and never cancels a commit or a fetch | pass | `TestInterruptIsEINTR`, `TestCommitSurvivesItsCaller` (the caller walks away mid-PUT of the Head; the Head still lands), `TestCloseWaitsForItsCommit`, `TestReadWaitsForItsData`, and through a kernel mount `TestFUSEInterruptsAreNotErrors`: 5,000 fsync+close and 20 GiB of reads, with SIGURG sent to the waiting threads tens of thousands of times a second, zero failures |

Mount tests run only with `-tags fuse`, from a child process that serves the mount
while the test acts as its client; a process that mounts a Volume and also opens a
file on that mount deadlocks. The real-server tests need an `s3` binary on `PATH`.

CI (`hanzo.yml`, `.github/workflows/cicd.yml`) runs thirteen gates as parallel jobs,
each under five minutes: `check`; the engine's unit tests under the race detector
shared three ways (`race-a`, `race-b`, `race-c`); `model`, the random tests against a
plain model, without the race detector (they have no goroutines, and the detector
multiplies their hashing tenfold); `realserver` and `realserver-kill`, which build the
server from a pinned commit of `hanzoai/s3` and fail if a test they exist for did not
pass; `mount`, `mount-signals` and `mount-checkout`; the measurements `scale-a` and
`scale-b`; and `fuzz`. A skip is a failure in every gate that needs a server or a
mount. The runner is a sandbox whose kernel ignores the setuid bit and gives its user
no capability, so the mount gates cannot use `fusermount3`; they run inside a user
namespace (`unshare -Urm`), where the user holds every capability and may mount a
file system, with `tools/fusemount` on `PATH` as `fusermount3`.

## Measurements

| What | Result | Test |
|---|---|---|
| Sequential write, 4 GiB in 1 MiB writes, one handle, Flush every GiB | 143 MB/s; peak heap 361 MiB | `TestSequentialWriteMemory` |
| Sequential write, 50 GiB, same | 87 MB/s under load (10 min 15 s); peak heap 878 MiB against a 1 GiB bound; 5,254 objects | `TestSequentialWrite50GiB` |
| Flush after a 4 KiB overwrite in a 16 MiB file | p50 5 ms, p90 9 ms, p98 44 ms, max 55 ms (100 rounds) | `TestFlushLatency` |
| 64 KiB write then Flush, round-robin over 16 files, 10 s | 274 ops/s, mean 4 ms, p50 2 ms, p99 45 ms, max 78 ms | `TestSustainedWriteWithFsync` |
| Objects written for 1,000 files of 200 bytes | 6 (5 shards, 1 Head) | `TestObjectsPerThousandSmallFiles` |
| Objects written for 1,000 files of 2.5 KiB | 36 (their bytes live in the shards, about one shard per 64 KiB) | same |
| Objects written for 1,000 files of 100 KiB | 14 (6 shards, 7 packs, 1 Head) | same |
| 100,000 files: create, first Flush, cold stat of every file | 0.5 s, 1.7 s (220 shards, 1 Head), 1.4 µs per Lookup + Stat | `TestHundredThousandFiles` |
| Ten changed files among 100,000 | a commit of 21 shards of 220, 61 ms | same |
| The same ten changes at 40,000 and at 120,000 files | 21 and 21 shards: the cost does not grow with the tree | `TestSyncCostDoesNotGrowWithTheTree` |
| 1,000,000 inodes: create and commit | 5.5 s; the store holds 1,195 objects, 72 MiB | `TestMillionInodes` |
| Memory at 1,000,000 inodes | the Volume holds 223 MiB after the commits (peak 403 MiB during one), against a cache bound of 256 MiB; a cold reader holds 1.3 MiB after opening and listing three directories | same |
| Ten changed files among 1,000,000 | a commit of 14 shards | same |
| Chunker, random data | mean chunk 578 KiB (170 chunks in 96 MiB), minimum 64 KiB, maximum 4 MiB | `TestCutBounds` |
| A 100 byte overwrite, insertion or deletion in the middle of 24 MiB (45 chunks) | 1 chunk changes | `TestCutStableUnderEdit` |
| A 6 byte edit in a file | 590 KiB of new packs | `TestSmallEditRewritesOneChunk` |
| Sealing overhead of the default Cipher | +1,676 bytes at 64 KiB, +1,916 at 1 MiB, +2,684 at 4 MiB | `TestAgeFrameOverhead` |

### Under signals, through the mount

| What | Result | Test |
|---|---|---|
| 5,000 small files written, fsynced and closed, two writers, SIGURG at their threads | 62,469 signals in 28.1 s; 0 failures; the callers saw `EINTR` from fsync 29,116 times and restarted it | `TestFUSEInterruptsAreNotErrors` |
| 20 GiB read back in 1 MiB reads of a 1 GiB file, two readers, same signals | 138,816 signals in 35.4 s (607 MB/s); 0 failures | same |

### Holes

| What | Result | Test |
|---|---|---|
| Truncate a file to 64 GiB, then Flush | 7 µs, then 297 µs; no allocation | `TestSparseTruncateIsBoundedWork` |
| A 1 PiB file: truncate, write at the start, the middle and near the end, Flush, reopen, read back | 2 ms in all; 1 shard (3 KiB), 1 pack, 1 Head; heap +1 MiB; ceilings asserted on seconds, shard count and bytes, and memory | `TestSparsePetabyte` |
| The same through the mount, in a child process | truncate, three writes and fsync in 3 ms; the server's RSS +2 MiB; 4 objects in the store | `TestFUSEPetabyteTruncate` |
| 2,000,000 one-byte writes with no Flush | `Stats.Dirty` 62.9 MB (each write span is charged 64 bytes), the heap grew 297 MiB: about 5 times the accounted bytes, so in that pattern memory is bounded by five times `Config.Dirty` | `TestSmallWritesAmplifyMemory` |

### The cost of a small file, through the mount

Every `close(2)` on the mount is a commit (see Not yet built), so a tree written by
`cp -r` or `tar x` pays one Head, a few shards and one pack per file.

| What | Result | Test |
|---|---|---|
| 400 files of 20 KiB written through the mount | 1.1 s (365 files/s); the store went from 2 to 1,269 objects, 3.2 per file; 23.0 MiB held for 7.8 MiB of data (2.94 times) | `TestFUSECommitPerClose` |
| 500 files of 20 KiB, Flush per file, engine alone | 3.6 objects per file (1 Head, 1.6 shards, 1 pack); at 5 ms store latency 36.9 files/s (13.6 s), at none 673 files/s | `TestCommitPerClose` |
| A 5,000-file checkout (284.6 MiB; 55% under 4 KiB, 30% to 32 KiB, 12% to 256 KiB, 3% to 2 MiB), a commit per file, through the mount | 37.8 s (132 files/s); the store holds 25,567 objects (5.11 per file) and 1,232.6 MiB (4.33 times the data) | `TestFUSECheckout` |
| The same 5,000 files, Flush per file, engine alone | 29,143 objects written (5.83 per file: 5,000 Heads, 22,527 shards, 1,616 packs); 24,145 held, 1,402.7 MiB (4.93 times) in 25.4 s | `TestCheckoutCost` |
| The same 5,000 files, one Flush at the end | 211 objects written (0.04 per file: 1 Head, 192 shards, 18 packs); 212 held, 288.7 MiB (1.01 times) in 5.0 s | same |
| Ten rewritten chunked files among 30,000 | a commit of 29 shards, 1 pack, 1 Head (ten inline files: 14 to 21 shards) | `TestChunkedFilesCommitCost` |

The 4 to 5 times is the shards a commit replaces: each commit rewrites the leaves it
touched and their ancestors, and the old ones stay until a collector exists. One
Flush at the end of the same tree leaves nothing behind.

### The amplification of a small change in a large file

| What | Result | Test |
|---|---|---|
| 200 appends of 4 KiB with a Flush each | 64,399 KiB of packs written for 800 KiB of data (80 times): each Flush re-chunks the tail chunk | `TestAppendWithFsyncAmplification` |
| A cold 4 KiB read | fetches the whole chunk holding it: 243 to 584 KiB (61 to 146 times) | `TestColdReadAmplification` |

### The object budget: 100,000 files, 150 GiB

`TestObjectBudget150GiB` builds every metadata record such a tree would have into a
real tree on a counting store, and models the data from the measured chunk mean and
sealing overhead (150 GiB is not written). The previous layout needed 39,321,600
objects for the same bytes.

| Shape | Chunks | Metadata shards | Packs | Objects |
|---|---|---|---|---|
| mixed (70,000 files of 4 KiB, 25,000 of 100 KiB, 4,900 of 2 MiB, 100 huge) | 285,700 | 5,109 (312 MiB) | 9,632 | 14,742 |
| the worst shape: 99,000 files of exactly 16 KiB (all inline) and 1,000 huge | 260,000 | 25,559 (1,585 MiB) | 9,629 | 35,189 |

Both are under 40,000. The second shows where the margin goes: inline bytes cost one
shard per 64 KiB, so a tree dominated by files just under 16 KiB approaches it.

### On a real server

| What | Result | Test |
|---|---|---|
| 300 small files, a commit per file, over HTTP to a Hanzo S3 server | 96 files/s; a commit p50 9.5 ms, p99 32 ms | `TestCommitRateOnRealServer` |
| The same 300 files, one Flush | 218 ms (1,373 files/s) | same |
| Reads that return a version that is not the MD5 of their body, under writers | 0 in 14,660 reads and 8,611 writes | `TestGetPairsBodyWithVersion` |

## The cipher

Every number above was taken with `vfs.Hybrid`: X25519 and ML-KEM-768 together, one
age file per chunk and shard (`TestEveryObjectIsHybridPQ` checks that every object
on disk carries the hybrid stanza and none a classical-only one). It is the only
Cipher the library ships and the only one the command line builds; there is no plain
X25519 option. A caller who wants another Cipher implements the two-method
interface, and a Volume refuses one that does not seal (`ErrBadCipher`). The few
tests that use a cheap stand-in cipher (the tree and chunk-list tests) count nodes
and chunks, which do not depend on the Cipher, and none of them contributes a number
to this document.

Moving a Volume is a copy of its objects and `vfs.Reseal` of its Head: only the Head
is bound to the place (`TestResealMovesAVolume`).

## Upload defaults

A pack is cut at 16 MiB (`packTarget`, the top of the 4 to 16 MiB range that suits an object store) and
uploaded as one request; the S3 backend sends an object as a single request up to 64
MiB and uses a multipart upload only above that (`partSize`), which is also why a
conditional write above 64 MiB is refused rather than made unconditional. Packs are
uploaded one at a time, under the Volume's lock: a pipeline with several streams in
flight is not built yet (see below), so these are the settings it will start from.

## Mutation check

`tools/mutate` breaks the source one way at a time (a node cut by position instead of
content, a chunk-reuse rule, the commit order, the fence comparison, a missing
conditional header, an unchecked hash, a Cipher check removed, a hole that stores
chunks, a Get that reads past its bound, and so on), copies the repository, applies
the change, and runs the test that should catch it. `tools/mutate/mutations.json` is
the list; `go run ./tools/mutate -j 5` runs it. The list has 95 mutations and every one is killed by a test (a run takes about eight minutes on four workers).

## Not yet built

Plain statements of fact; each is its own change.

* POSIX operations beyond the ones above: rename, symlink, hard link,
  chmod/chown/utimens, statfs, xattr, locks, unlink-while-open (handles of a removed
  file fail). The mount answers the missing ones ENOSYS.
* The Volume's lock is held across the store calls of a commit, so a slow or stalled
  store stalls every call on that Volume, `Stat` included (`TestStalledCommitHonoursItsDeadline`:
  a caller's deadline ends its own call, and a `Stat` waited 2.0 s behind the stalled
  commit). There are no deadlines of the library's own, no retry policy of its own,
  and no circuit breaker. The S3 backend's SDK retries a failing request about ten
  times, out of sight: an unreachable endpoint took about 31 s to fail, and the mount
  flushes at its end with no deadline, so an unmount can take that long.
* A commit per `close(2)` on the mount: a file costs a Head, its shards and a pack
  (3.2 objects through the mount, above); group commit is not built. The node cache
  and journal are not built either: reads and writes go to the store, `Fsync` costs a
  commit, uploads are sequential, and there is no read-ahead.
* Concurrent cold readers of one chunk each fetch it: there is no shared in-flight
  fetch (readers hold the Volume's lock shared, and the cache is filled by whoever
  finishes first).
* Partial-chunk reads and writes: a cold 4 KiB read fetches a whole chunk, and each
  Flush after an append re-chunks the tail chunk (the 61 to 146 times and the 80 times
  above).
* Paged directory listing: `ReadDir` returns a whole directory (435 MiB allocated for
  1,000,000 entries, with the Volume locked). Removing a directory checks that it is
  empty without listing it.
* Garbage collection: chunks, packs and shards that a failed or superseded commit
  left, and the chunks of removed files, stay until a collector exists. A deposed
  writer can upload packs until its next commit is refused (`TestDeposedWriterIsToldAtItsNextCommit`:
  of 96 MiB written, 48 MiB of packs reached the store before it was told). Snapshot,
  Clone, Usage, import and export are not built either.
* Detecting a replayed old Head: a store that returns an older Head it once held
  cannot be told from the current one without a pin; a caller that must notice keeps
  the `Seq` of the last Head it saw. `Seq` counts commit attempts and never repeats,
  but it can skip.
* `vfs serve` and the agent.
* Compression.
* A tiny extent costs a whole sealed chunk (a few hundred bytes of header and tag on
  top of its bytes): 100,000 scattered 17-byte writes into a sparse file are 100,000
  chunks, about 171 MiB of packs for about 2 MiB of data, and a first Flush of 12.7 s
  (`TestTinyExtentsCost`). The cost follows the number of extents, not the size of the
  file. Packing tiny extents inline is not built.
* `statfs` answers zeros.

## Running the tests

```bash
go test -short -race ./...             # the unit step of CI
go test ./...                          # adds the scale tests and the full failure sweep
go test -tags fuse ./pkg/mount         # a kernel mount (Linux with /dev/fuse and fusermount)
PATH=$DIR:$PATH go test ./...          # $DIR holds an `s3` built from github.com/hanzoai/s3: adds the real-server tests
go test -run xxx -bench . .            # throughput
go run ./tools/mutate -j 5             # every mutation must be killed by a test
```
