# hanzoai/vfs — AI Engineering Guide

## What this is

A filesystem (a **Volume**) on an object-store prefix, sealed with a post-quantum
key. One writer at a time, fenced by a **Round**. The store is any S3-compatible
service or a directory. The engine knows nothing of who mounts it, what runs on
it, or what it costs: it exposes a small POSIX-shaped Go API, a FUSE front, and a
command line.

The `replica/` subpackage (HIP-0107) replicates SQLite snapshots and WAL to an object
store; it shares the Round fence with the Volume (`pkg/fence`).

## On-disk layout

There is one layout and no version number: nothing in an object says which
library wrote it, and no code path reads any other layout. Extension is by
tagged records (below), not by numbered revisions. A Volume is a
prefix holding three kinds of object. Every byte is sealed by the Volume's
`Cipher`; the backend sees ciphertext only.

| Key | What | Name |
|---|---|---|
| `head` | the one mutable object: root shard ref, next inode, Seq, Volume id, chunking seed; its fence header carries the Round | fixed |
| `meta/<2 hex>/<hash>` | metadata shard, immutable | blake3 of the sealed bytes |
| `pack/<2 hex>/<id>` | pack of sealed chunks, immutable | 128 random bits |

**Chunks** (`chunk.go`). File bytes are cut by FastCDC (gear hash, normalized
chunking) into chunks of 64 KiB to 4 MiB, mean 512 to 580 KiB. A cut depends only
on the bytes since the previous cut, so an edit rewrites the chunks it touches and
the cuts re-synchronize after it (a 100 byte overwrite, insertion or deletion in
24 MiB changes one chunk). The gear table is drawn per Volume from the seed in the
Head, so chunk lengths do not identify a known file across Volumes. A chunk id is
the blake3 of its plaintext and appears only inside sealed shards; equal ids in one
Volume mean equal bytes (dedupe). Volumes never share chunks.

**Packs** (`pack.go`). Each chunk is sealed on its own and appended to the open
pack, uploaded at 16 MiB or at Flush. A 150 GiB tree is ~9.6k pack PUTs. Where a
chunk lies is the chunk table, not a pack header.

**Small files** (at most `small` = 16 KiB) are not chunked: their bytes are a
record beside their inode. A file's form is a function of its size alone.

**Holes.** A file's size is its inode's. Only runs of bytes that were written have
chunks; a byte no chunk covers is a hole and reads as zero. Each run is cut on its
own from its first byte. So a truncate to 1 PiB, or a write at an offset of a
petabyte, costs the written runs and nothing for the gap: no chunks, no records, no
memory (`TestSparsePetabyte`). The stored chunk list is, after any sequence of
edits, exactly what a cut of the file's final bytes gives
(`TestChunkListIsTheWholeFileCut`). `MaxSize` is 2^62.

**Metadata** (`record.go`, `tree.go`, `shard.go`). All metadata is records in one
ordered Merkle search tree (a prolly tree) of immutable shards:

```
[1][ino]                 inode: tagged fields (mode, size, times; room for more)
[2][dir ino][name]       directory entry: child ino, type
[3][ino]                 inline file bytes
[3][ino][offset]         chunk reference: chunk id, length
[4][chunk id]            chunk table: pack id, offset, sealed length
```

The inode record is a sequence of `[tag][length][value]` fields in ascending tag
order. A reader checks the tags it knows, carries the ones it does not, and writes
them back on rewrite, so owner, link count, device, symlink target and the like are
new tags. New record kinds take the next `[table]` byte.

Node boundaries are decided by the entries (a keyed hash of the key under a
threshold proportional to the entry's size, the key derived from the Volume's seed),
average 64 KiB, so the tree's shape is a pure function of its records and the seed.
The key is what stops a guest who picks file names from picking names that each end
their own node (2,000 such names cost 2,007 shards instead of 4). A change rewrites the changed leaves and their ancestors
and nothing else; the new root equals the root built from scratch
(`TestTreeMatchesModel`, `TestTreeForcedCuts`).

**Sealing context.** Every object is sealed together with what it is: a 32-byte
digest of its kind and of the length-framed fields that name it (a Head: its place,
`Backend.String()`; a shard: its Volume id; a chunk: its Volume id and its own id).
The reader compares it for equality, so no context is a prefix of another. An object
from another Volume or place does not open even under a Cipher both share, and every
Seal checks that the Cipher did not return its input. Only the Head is bound to the
place: a Volume copied to another place does not open there until `vfs.Reseal`
rewrites its Head for it. The place string is canonical: absolute and symlink-free
for a directory, `s3://bucket/prefix` without slashes at the ends for a bucket.

## Commit, Flush, Fsync

`Flush` settles every written file into chunks, uploads the open pack, writes the
shards the changes touched, then replaces `head` with **one conditional PUT**
(`If-Match` on the version read; `If-None-Match: *` for the first). Until that
write lands `head` names the previous tree, which nothing touched: a crash at any
point leaves the Volume as the last Flush left it. Uploaded chunks, packs and
shards of a failed Flush are unreferenced garbage, never corruption.

The package documentation (`doc.go`) holds the **durability table**: what is
durable when each call returns, and what a later journal must keep. In short: only
`Flush` is durable; `Fsync` and the FUSE close are `Flush`; `Close` is not; `Attach`
loses no acknowledged commit. Objects are written once under hash or random names,
so repeating a write, or one that completes after its caller gave up, is harmless;
the Head compare-and-swap is the only mutation.

* On the mount an interrupt (a signal reaching a caller that waits) ends only fsync's
  wait (EINTR; the commit runs on by itself). Read, write, close and the calls that
  change the tree are never answered EINTR: a read answered EINTR reaches the caller as
  EIO, close is not restarted, a restarted O_EXCL create would meet its own file. The
  details are in `pkg/mount/errno.go`; `TestFUSEInterruptsAreNotErrors` hammers the
  mount with signals.
* On error the changes stay in memory for the next Flush and the outcome of that
  attempt is unknown. `ErrStaleRound` means this Volume was fenced; if the store's
  Head is exactly its tree, Flush says nil (the commit landed, the reply was lost).
* `Head.Seq` numbers commit attempts and never repeats; it can skip.
* A store that returns an older Head cannot be told from the current one by this
  library; a caller that must notice keeps the Seq it last saw.
* Nothing polls. A deadline timer belongs to the caller.
* Flush holds the Volume exclusively for its duration, including its store calls.
* Memory is bounded without a commit: past `Config.Dirty` bytes written (each
  separate write is charged a fixed overhead too) the files are chunked and
  uploaded; past `Config.Overlay` pending records they are written into shards.

## Fence

`pkg/fence` is the only fencing implementation: one object per key, a Round header
and a claimant token in front of the payload, admitted iff `round >= recorded`, in
one conditional write.

* `Attach(ctx, Config{Round: r})` calls `Claim`: it reads the last landed Head and
  re-seals it at `r` by compare-and-set. `r` must exceed the recorded Round
  (strict, `ErrStaleRound`); 0 and 2^64-1 are refused. No acknowledged commit is
  lost; every commit by a lower Round is then refused. A claim that landed with its
  reply lost is recognised by its token, byte for byte.
* A fenced Volume answers every write with `ErrStaleRound`.
* `Open` is read-only and claims nothing.
* The Round comes from the caller.

## Keys

`Cipher` (`cipher.go`): `Seal(plaintext) (sealed, keyID, err)`, `Open(sealed,
keyID)`. The engine frames every object as `[len keyID][keyID][sealed]`, so a
Cipher holding several keys keeps opening old objects while sealing new ones under
its current key. `Hybrid(current, retired...)` is the default: luxfi/age hybrid
X25519 + ML-KEM-768, one age file per chunk and shard. A Volume checks its Cipher
when configured and refuses one that returns its input, does not open what it
sealed, or opens altered bytes (`ErrBadCipher`). The engine never reads a key from
a file or the environment and never stores one. `Provider` is the seam a caller
implements to fetch a Cipher. The CLI's provider reads **standard input**: one
`AGE-SECRET-KEY-PQ-1...` identity per line, the first seals, the others are
retired. Why stdin: it cannot leave a key in argv, the environment or a file, and
it composes with any secret store that can print to a pipe.

## Go API

```go
vfs.Create(ctx, cfg)                 // empty Volume; refuses an existing one
v, _ := vfs.Open(ctx, cfg)           // read-only
v, _ := vfs.Attach(ctx, cfg)         // cfg.Round > recorded: read-write
v.Stat/Lookup/ReadDir/Mkdir/Create/Remove/Resolve  // by (parent inode, name)
f, _ := v.Open(ctx, ino)             // *File: ReadAt, WriteAt, Truncate, Stat, Close
v.Truncate(ctx, ino, size); v.Flush(ctx); v.Stats(); v.Close()
```

The API is by inode and name, as FUSE is, so rename, symlink, link, attributes and
xattr add methods and record kinds without reshaping anything. Removing a file
while it is open makes its handles fail (no orphan inodes yet).

## Layout

```
vfs/
├── doc.go                   # layout, commit, durability table, crash matrix, fence
├── volume.go head.go        # Create/Open/Attach/Flush; Head and sealing contexts
├── ops.go file.go           # namespace operations; File, settle, recut, holes
├── chunk.go pack.go         # FastCDC; packs, putChunk/getChunk
├── record.go tree.go shard.go overlay.go codec.go lru.go   # metadata tree
├── cipher.go                # Cipher, Provider, Hybrid, the configuration check
├── cmd/vfs/                 # CLI: keygen init put get ls stats mount
├── cmd/vfs-cost/            # cost model (pkg/cost)
├── pkg/backend/             # Backend interface; file/ and s3/ implementations
├── pkg/fence/               # the Round fence
├── pkg/mount/               # FUSE front (build tag fuse): bazil on Linux, jacobsa on macOS
├── replica/                 # SQLite snapshot replication (HIP-0107)
├── internal/storetest/      # Mem store, S3 protocol server, real-server harness, conformance suite
├── tools/mutate/            # the mutation check: breaks the source, expects a test to fail
├── docs/SPECIFICATION.md    # the layout and protocols, normatively
└── docs/PRODUCTION-READINESS.md
```

## Backends

`backend.Backend`: `Get(ctx, key, max)` (bytes and a `Version`; an object larger
than `max` is `ErrTooLarge`), `GetRange`, `Put`, `PutIfAbsent`, `PutIfVersion`,
`Delete`, `List`, `Stat`. The two conditional writes decide and write atomically
(exactly one of N racers on a version wins) or are refused with `ErrTooLarge`;
never silently unconditional. `String()` names the place (bucket and prefix) and
never a credential or endpoint.

| URL | |
|---|---|
| `file:///dir` | atomic via one flock; version = blake3 of content; dev and tests |
| `s3://bucket/prefix?endpoint=host:port[&insecure=1][&region=r]` | S3 API through `hanzos3/go`; ETag as version; `If-Match`/`If-None-Match`; 412 is `ErrConflict`; no credential in the URL (refused); the prefix is validated (no empty, `.` or `..` segment, no control character) |

`s3.New(s3.Config{...})` takes the credential from the caller; the URL form uses
the SDK's standard chain. Objects over 64 MiB go in parts, so a conditional write
that large is refused. The SDK retries a failing request for about half a minute
and hides that from the caller: retries and deadlines are not built yet.

## Tests

```
go test ./...                                    # fast + scale tests
go test -short -race ./...                       # the race detector over the lot (CI runs it on the tests with goroutines)
go test -tags fuse ./pkg/mount                   # needs /dev/fuse + fusermount3
tools/ci/mount.sh ./pkg/mount run '^TestFUSEPOSIX$'   # the same with no helper and no privilege, in a user namespace
PATH=$DIR:$PATH go test ./...                    # $DIR holds the `s3` server (tools/ci/s3server.sh DIR fetches the published image)
go run ./tools/mutate                            # every mutation must be killed by a test
```

The real-server tests start `s3 server` (own-storage mode) in a temp dir with a
throwaway key pair, race 16 and 128 writers on one version, run whole Volumes on it,
and kill a writer at random moments. CI (`hanzo.yml`, `.github/workflows/cicd.yml`)
runs eleven gates as parallel jobs, each under five minutes: `check`, `unit`, `race`,
`model`, `sqlite` (the only gate that builds the SQLite engine), `realserver` and
`realserver-kill` (against the published image of the server, pinned by digest, which
fail if a test did not pass), `scale-a`, `scale-b`, `scale-c` and `fuzz`. The tests on a kernel
mount are not gates: the runner's kernel is a sandbox whose FUSE is not Linux's, and
`docs/PRODUCTION-READINESS.md` says what it does differently. They run on a Linux
kernel, with `go test -tags fuse ./pkg/mount ./internal/sqlitecheck`, or with
`tools/ci/mount.sh` where there is no `fusermount3` (it mounts in a user namespace
through `tools/fusemount`). Crash tests kill a
re-executed test binary with SIGKILL at every point of a commit (the key arrives on
its stdin). A mount test serves the Volume from a child process, always: a process
that mounts a Volume and also opens a file on that mount deadlocks. Go registers the
open file with its poller, the kernel asks the file system for readiness, and the
server needs the same poller to read its next request; nothing in the process can
answer, and without root the threads stay in D state until reboot.

## Not yet built (plain statement of fact)

* POSIX operations beyond Lookup, ReadDir, Mkdir, Create, Remove, Truncate, Read,
  Write, Flush, Fsync: rename, symlink, hard link, chmod/chown/utimens, statfs,
  xattr, locks, unlink-while-open. The mount answers the missing ones ENOSYS.
* The node cache and journal (Fsync costs a commit), group commit, parallel
  uploads, read-ahead, partial-chunk reads.
* A server and agent process around the Volume.
* Garbage collection of chunks, packs and shards no Head names (a deposed writer
  can upload packs until its next commit), Snapshot, Clone, Usage, import, export.
* Retries with backoff, deadlines on store calls, a circuit breaker, metrics,
  handling of a store's slow-down answers. The Volume lock is held across the store
  calls of a commit.
* Paged directory listing: ReadDir returns a whole directory.
* Re-sealing a Volume's Head for a new place.
* Compression.

## Rules

1. Never store plaintext or a key. Everything crosses `Cipher`.
2. One way: one fence, one metadata tree, one commit (a single conditional PUT).
3. Orphans are garbage for a collector, never corruption: write order is chunks and
   packs, then shards, then `head`.
4. No polling. A deadline timer is the caller's.
5. No test-only environment variables or build tags.
6. LLM.md is the one project memory file; CLAUDE.md / AGENTS.md are local symlinks.
