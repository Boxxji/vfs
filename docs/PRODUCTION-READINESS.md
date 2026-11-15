# Production readiness of a Volume (format 2)

This replaces the notes written for format 1 (4 KiB blocks, one metadata blob),
whose numbers no longer describe the code. Everything here was measured on the
tests named; nothing is a claim without one.

Machine: GB10 arm64, 20 cores, Go 1.27.1, shared with other jobs (load average
14 to 20 during the runs, so throughput is a floor, not a peak). The file backend
sat on tmpfs, so its own fsync costs nothing: the figures are the engine's (chunking,
hashing, sealing, shard and Head writes), not a disk's.

## What is guaranteed

| Concern | Status | Evidence |
|---|---|---|
| Crash at any point leaves the last committed tree, whole | pass | `TestSigkill` kills a process with SIGKILL before the first pack write, before the first shard write, before the Head write and after it; `TestFailureAtEveryWrite` fails each of the 14 writes of a Flush in turn; `TestLostAcknowledgementOfHead` loses the reply of a Head that landed. In every case a fresh Open equals the last committed state exactly, and a retry commits the next |
| A deposed writer cannot commit; takeover loses no acknowledged commit | pass | `TestFenceRefusesOlderRound`, `TestAttachNeedsAHigherRound`, `TestTakeoverLosesNoAcknowledgedCommit`, `TestRacingClaims`, on the in-memory store, the file backend, the S3 backend against an S3 protocol server, and a real Hanzo S3 server |
| Exactly one of N writers on one Head version wins | pass | the conformance suite on every backend; against the real server `TestRealServerRaceStress` races 16 clients for 150 rounds on one version and for 100 keys on creation: exactly one winner every time |
| Backend outage surfaces, loses nothing | pass | the same failure sweeps: an error from the store is an error from `Flush`, the changes stay in memory, the store is untouched |
| Wrong key fails closed; tampered or swapped objects are refused | pass | `TestWrongKeyFailsClosed`, `TestTamperedObjectsAreRefused`, `TestSwappedChunkIsRefused`, `TestTreeDetectsSwappedShard` |
| Key rotation | pass | `TestRotation`: a Cipher with the new key and the old one reads everything and seals under the new; once the old is dropped, what only it sealed no longer opens |
| No key, plaintext or name on disk; the Cipher is the only door | pass | `TestNothingKeyShapedOnDisk`, `TestCipherIsTheOnlyDoor`, `TestCLIRoundTrip` (the key comes from standard input; an environment variable is ignored) |
| SQLite on the Volume | pass | `TestSQLiteRoundTrip`; over FUSE `TestFUSESQLite` (100 rows survive release and remount, `integrity_check` ok) |
| FUSE: Lookup, ReadDir, Mkdir, Create, Remove, Setattr size, Read, Write, Flush, Fsync, Release | pass | `TestFUSEPOSIX`: after `fsync(2)` a second read-only view of the store sees the file |

The tests that need a kernel mount skip where there is no `/dev/fuse` or no
`fusermount`; the real-server tests skip where no `s3` binary is on PATH. Both ran
for these results.

### Flush and Fsync

`Flush` returns only after the Head naming everything written so far has advanced
in the store. `Fsync` is `Flush`: there is no node-local journal yet, so no cheaper
durable point exists. A FUSE mount calls it on every close and every fsync. Writes are held
in memory until then, bounded by `Config.Dirty`; a bound on staleness is a deadline
timer owned by the caller of the Volume.

## Measurements

| What | Result | Test |
|---|---|---|
| Sequential write, 128 MiB of distinct data, 1 MiB writes, file backend | 84 to 111 MB/s | `BenchmarkSequentialWrite` (3 runs of 5) |
| Sequential read, same file, cold read-only Volume | 152 to 230 MB/s | `BenchmarkSequentialRead` |
| Flush after a 4 KiB overwrite in a 16 MiB file | p50 5 ms, p90 12 ms, p98 54 ms, max 55 ms (100 rounds) | `TestFlushLatency` |
| Format 1's workload: 64 KiB write then Flush, round-robin over 16 files, 10 s | 167 ops/s, mean 6 ms, p50 5 ms, p99 42 ms, max 58 ms. Format 1 recorded 6.4 ops/s, mean 157 ms, on a different machine and a disk, so read this as an order of magnitude, not a ratio | `TestSustainedWriteWithFsync` |
| Objects written for 1,000 files of 200 bytes | 6 (5 shards, 1 Head) | `TestObjectsPerThousandSmallFiles` |
| Objects written for 1,000 files of 2.5 KiB | 36 (their bytes live in the shards, about one shard per 64 KiB) | same |
| Objects written for 1,000 files of 100 KiB | 14 (6 shards, 7 packs, 1 Head) | same |
| 100,000 files: create, first Flush, cold stat of every file | 0.7 s, 1.3 s (197 shards, 1 Head), 2.1 µs per Lookup + Stat | `TestHundredThousandFiles` |
| Ten changed files among 100,000 | a commit of 21 shards of 197, plus the Head | same |
| The same ten changes at 40,000 and at 120,000 files | 19 and 21 shards: the cost does not grow with the tree | `TestSyncCostDoesNotGrowWithTheTree` |
| 1,000,000 inodes: create and commit | 5 s; the store holds 1,030 objects, 62 MiB | `TestMillionInodes` |
| Memory at 1,000,000 inodes | the Volume holds about 200 MiB after the commits (peak 267 MiB at one), bounded by `Config.Cache` (256 MiB, split between shards and chunks), not by the inode count; a cold reader holds 1.3 MiB after opening and listing three directories | same |
| Ten changed files among 1,000,000 | a commit of 14 shards | same |
| Chunker, random data | mean chunk 581 KiB (169 chunks in 96 MiB), minimum 64 KiB, maximum 4 MiB | `TestCutBounds` |
| A 100 byte overwrite, insertion or deletion in the middle of 24 MiB (43 chunks) | 1 chunk changes | `TestCutStableUnderEdit` |
| A 6 byte edit in a 16 MiB file | 235 KiB of new packs | `TestSmallEditRewritesOneChunk` |
| Sealing overhead of the default Cipher | about 1,660 bytes + 16 per 64 KiB of chunk | `TestAgeFrameOverhead` |

### The object budget: 100,000 files, 150 GiB

`TestObjectBudget150GiB` builds every metadata record such a tree would have into a
real tree on a counting store, and models the data from the measured chunk mean and
sealing overhead (150 GiB is not written). Format 1 needed 39,321,600 objects.

| Shape | Chunks | Metadata shards | Packs | Objects |
|---|---|---|---|---|
| mixed (70,000 files of 4 KiB, 25,000 of 100 KiB, 4,900 of 2 MiB, 100 huge) | 294,500 | 5,085 | 9,632 | 14,718 |
| the worst shape: 99,000 files of exactly 16 KiB (all inline) and 1,000 huge | 270,000 | 25,543 | 9,629 | 35,173 |

Both are under the ~40,000 budget. The second shows where the margin goes: inline
bytes cost one shard per 64 KiB, so a tree dominated by files just under 16 KiB
approaches it.

## Mutation check

47 deliberate breakages of the engine (a node cut by position instead of content,
the chunk-reuse rules, the commit order, the fence comparison, a missing
conditional header, an unchecked hash, a key id ignored, a Get that asks for the
version before the body, and so on) were applied one at a
time to a copy and the tests that should catch each were run: all 47 fail a test.
Six first survived and each exposed a missing or weak test, since fixed:
a shifted-boundary tree edit (the first test changed a value without changing a
record count), a chunk list that differed from a cut of the whole file (now
`TestChunkListIsTheWholeFileCut` compares it with the reference after every kind of
edit), stale chunk references after a shrink, reads served from the open pack (the cache hid
them), a lost-race retry in the fence that a scheduler decided (now forced, as is the
same retry in a same-round write), and one clamp the callers make redundant (removed).

## Not yet built

Plain statements of fact, each its own change:

* POSIX operations beyond the ones above: rename, symlink, hard link,
  chmod/chown/utimens, statfs, xattr, locks, unlink-while-open (handles of a removed
  file fail).
* The node cache and journal: reads and writes go to the store; `Fsync` costs a commit;
  uploads are sequential; there is no read-ahead.
* `vfs serve` and the agent.
* Garbage collection: chunks, packs and shards that a failed or superseded commit
  left, and chunks of removed files, stay until a collector exists. Nor are
  Snapshot, Clone, Usage, import or export built.
* Retries with backoff, a circuit breaker, metrics, handling of a store's slow-down
  responses: the first store error is returned to the caller.
* Sparse files: extending a file with a truncate writes zero chunks (all alike, so
  stored once).
* Compression.

## Running the tests

```bash
go test -short -race ./...             # what CI runs (a couple of minutes)
go test ./...                          # adds the scale tests and the full failure sweep
go test -tags fuse ./pkg/mount         # a kernel mount (Linux with /dev/fuse and fusermount)
PATH=$DIR:$PATH go test ./...          # $DIR holds an `s3` built from github.com/hanzoai/s3: adds the real-server tests
go test -run xxx -bench . .            # throughput
```
