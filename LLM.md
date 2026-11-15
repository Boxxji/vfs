# hanzoai/vfs — AI Engineering Guide

## What this is

The Disk engine: a filesystem (a **Volume**) on an object-store prefix, sealed
with a per-Volume post-quantum key. One writer at a time, fenced by a **Round**.
The store is any S3-compatible service (Hanzo S3 first) or a directory. The
engine knows nothing of sandboxes, runtimes, quotas or billing: the cloud agent
mounts and leases Volumes, a runtime (Firecracker, kata, visor) consumes the same
Disk through FUSE.

Companion to `~/work/hanzo/replicate` (SQLite WAL to S3): `vfs` is the whole
filesystem. The `replica/` subpackage (HIP-0107) is the older per-org SQLite HA
substrate and shares the fence with the Volume (`pkg/fence`).

## Format (version 2)

A Volume is a prefix holding three kinds of object. Every byte is sealed by the
Volume's `Cipher`; the backend sees ciphertext only.

| Key | What | Name |
|---|---|---|
| `head` | the one mutable object: root shard ref, next inode, commit count; fence header carries the Round | fixed |
| `meta/<2 hex>/<hash>` | metadata shard, immutable | blake3 of the sealed bytes |
| `pack/<2 hex>/<id>` | pack of sealed chunks, immutable | 128 random bits |

**Chunks** (`chunk.go`). File bytes are cut by FastCDC (gear hash, normalized
chunking) into chunks of 64 KiB to 4 MiB; measured mean on random bytes 581 KiB.
A cut depends only on the bytes since the previous cut, so an edit rewrites the
chunks it touches and the cuts re-synchronize after it (measured: a 100 byte
overwrite, insertion or deletion in 24 MiB changes 1 chunk). The gear table is
derived from blake3's XOF over a fixed string and pinned by a test: it is part of
the format. A chunk id is the blake3 of its plaintext; ids appear only inside
sealed shards. Equal ids in one Volume mean equal bytes: that is dedupe. Volumes
never share chunks (per-Volume key; cross-Volume dedupe is off).

**Packs** (`pack.go`). Each chunk is sealed on its own and appended to the open
pack; the pack is uploaded at 16 MiB or at Flush. A 150 GiB tree is therefore
~9.6k pack PUTs, not one per chunk. Where a chunk lies is the chunk table, not a
pack header. Packs stay under the S3 backend's single-request limit.

**Small files** (at most `inlineMax` = 16 KiB) are not chunked: their bytes are a
record beside their inode. A file's form is a function of its size alone.

**Metadata** (`record.go`, `tree.go`, `shard.go`). All metadata is records in one
ordered Merkle search tree (a prolly tree) of immutable shards:

```
[1][ino]                 inode: mode, size, mtime, atime, ctime
[2][dir ino][name]       directory entry: child ino, type
[3][ino]                 inline file bytes
[3][ino][offset]         chunk reference: chunk id, length
[4][chunk id]            chunk table: pack id, offset, sealed length
```

Node boundaries are decided by the entries (a hash of the key under a threshold
proportional to the entry's size), average 64 KiB, so the tree's shape is a pure
function of its records. A change rewrites the changed leaves and their
ancestors and nothing else; the new root equals the root built from scratch from
the same records (`TestTreeMatchesModel` checks this after every random batch).
Measured: ten changed files in 100,000 write ~21 shards of 197, and the cost does
not grow with the tree (`TestSyncCostDoesNotGrowWithTheTree`).

## Commit, Flush, Fsync

`Flush` settles every written file into chunks, uploads the open pack, writes the
shards the changes touched, then replaces `head` with **one conditional PUT**
(`If-Match` on the version read; `If-None-Match: *` for the first). Until that
write lands `head` names the previous tree, which nothing touched: a crash at any
point leaves the Volume exactly as the last Flush left it. Uploaded chunks, packs
and shards of a failed Flush are unreferenced garbage, never corruption.

* `Flush` returns only after `head` advanced in the store. On error the changes
  stay in memory for the next Flush; `ErrStaleRound` means they never land.
* `Fsync` is `Flush`. There is no node journal yet (comes with V4), so no cheaper
  durable point exists. FUSE `Flush` (every close) and `Fsync` both call it.
* A max-age flush is a deadline timer owned by the caller of `Volume` (the agent),
  not a poll inside the engine. Nothing here polls.
* Flush holds the Volume exclusively for its duration.
* Memory is bounded without a commit: past `Config.Dirty` bytes written the files
  are chunked and uploaded; past `Config.Overlay` pending records they are written
  into shards. Both leave `head` alone.

## Fence

`pkg/fence` is the only fencing implementation (it was `replica.FencedStore`):
one object per key, a Round header in front of the payload, admitted iff
`round >= recorded`, in one conditional write.

* `Attach(ctx, Config{Round: r})` calls `CarryForward`: it reads the last landed
  Head and re-seals it at `r` by compare-and-set. `r` must exceed the recorded
  Round (strict, `ErrStaleRound`); round 0 is never attached. No acknowledged
  commit is lost; every commit by a lower Round is then refused.
* A fenced Volume answers every write with `ErrStaleRound`.
* `Open` is read-only and claims nothing.
* The Round comes from the caller (cloud mints it from its row's compare-and-set).

## Keys

`Cipher` (`cipher.go`): `Seal(plaintext) (sealed, keyID, err)`, `Open(sealed,
keyID)`. The engine frames every object as `[len keyID][keyID][sealed]`, so a
Cipher holding several keys keeps opening old objects while sealing new ones under
its current key: rotation without re-encryption. `NewAgeCipher(current,
retired...)` is the default: luxfi/age hybrid X25519 + ML-KEM-768, one age file per
chunk and shard. The engine never reads a key from a file or the environment and
never stores one. `Provider` is the seam the caller implements (the agent with Hanzo
KMS). The CLI's provider reads **standard input**: one `AGE-SECRET-KEY-PQ-1...`
identity per line, the first seals, the others are retired. Why stdin: it cannot
leave a key in argv, the environment or a file, it needs no exec of a user string,
and `hanzo kms secrets get ... | vfs ...` composes with it.

## Go API

```go
vfs.Create(ctx, cfg)                 // empty Volume; refuses an existing one
v, _ := vfs.Open(ctx, cfg)           // read-only
v, _ := vfs.Attach(ctx, cfg)         // cfg.Round > recorded: read-write
v.Stat/Lookup/ReadDir/Mkdir/Create/Remove/Resolve  // by (parent inode, name)
f, _ := v.Open(ctx, ino)             // *File: ReadAt, WriteAt, Truncate, Stat, Close
v.Truncate(ctx, ino, size); v.Flush(ctx); v.Stats(); v.Close()
```

The API is by inode and name, as FUSE is, so V1 (Rename, Symlink, Link, chmod,
Statfs, xattr, locks) adds methods and record kinds without reshaping anything.
Removing a file while it is open makes its handles fail (no orphan inodes yet).

## Layout

```
vfs/
├── volume.go head.go        # format docs, Create/Open/Attach/Flush, Head
├── ops.go file.go           # namespace operations; File, settle, recut
├── chunk.go pack.go         # FastCDC; packs, putChunk/getChunk
├── record.go tree.go shard.go overlay.go codec.go lru.go   # metadata tree
├── cipher.go                # Cipher, Provider, NewAgeCipher
├── cmd/vfs/                 # CLI: keygen init put get ls stats mount
├── cmd/vfs-cost/            # cost model (pkg/cost)
├── pkg/backend/             # Backend interface; file/ and s3/ implementations
├── pkg/fence/               # the Round fence
├── pkg/mount/               # FUSE front (build tag fuse): bazil on Linux, jacobsa on macOS
├── replica/                 # SQLite snapshot replication (HIP-0107)
├── internal/storetest/      # Mem store, S3 protocol server, real-server harness, conformance suite
└── docs/PRODUCTION-READINESS.md
```

## Backends

`backend.Backend`: `Get` (returns bytes and a `Version`), `GetRange`, `Put`,
`PutIfAbsent`, `PutIfVersion`, `Delete`, `List`, `Stat`. The two conditional
writes must decide and write atomically (exactly one of N racers on a version wins).

| URL | |
|---|---|
| `file:///dir` | atomic via one flock; version = blake3 of content; dev and tests |
| `s3://bucket/prefix?endpoint=host:port[&insecure=1][&region=r]` | S3 API through `hanzos3/go`; ETag as version; `If-Match`/`If-None-Match`; 412 is `ErrConflict`; objects over 64 MiB go multipart and are aborted on failure |

`s3.New(s3.Config{...})` takes the credential from the caller; the URL form uses
the SDK's standard chain. The engine assumes nothing about which store answers: it
receives a bucket and a prefix.

## Tests

```
go test ./...                                   # fast + scale tests
go test -short -race ./...                      # what CI runs
go test -tags fuse ./pkg/mount                  # needs /dev/fuse + fusermount
PATH=$DIR:$PATH go test ./...                   # $DIR holds an `s3` built from hanzoai/s3: adds the real-server tests
```

The real-server tests start `s3 server` (own-storage mode) in a temp dir with a
throwaway key pair, race 16 writers on one version for 150 rounds and 16 creators
of a key for 100 keys, and run whole Volumes on it. Without the binary they skip.
Crash tests kill a re-executed test binary with SIGKILL between pack, shard and
Head writes (the key arrives on its stdin). The mount tests serve the Volume from
a re-executed child for a reason that bites: a process that mounts a Volume and
also opens a file on that mount deadlocks. Go registers the open file with its
poller, the kernel asks the file system for readiness (FUSE_POLL), and the server
needs the same poller to read its next request. Nothing in the process can answer,
and without root (`/sys/fs/fuse/connections/N/abort`) the threads stay in D state
until reboot. The engine is unaffected (the agent never reads its own mount); tests
must never be both sides of one mount.

## Not yet built (plain statement of fact)

V1 POSIX ops (rename, symlink, hardlink, chmod/chown/utimens, statfs, xattr,
locks, unlink-while-open); V4 node cache and journal (Fsync is Flush),
parallel upload pipeline, read-ahead; V7 `vfs serve` / agent; V8 GC of orphan
chunks, packs and shards, Snapshot/Clone/Usage, import/export; V9 retries with
backoff, breaker, metrics, SlowDown handling; sparse files (a truncate-extend
writes zero chunks, deduplicated); compression.

For V1 and V4, `~/work/hanzo/s3/s3/mount` (the s3 repo's own POSIX mount) is the
reference for behaviour and test cases only (file locks, rename across directories,
unlink-while-open, write-buffer back-pressure, metadata flush retry); vfs stays the
single Disk engine and copies nothing from it.

## Rules

1. Never store plaintext or a key. Everything crosses `Cipher`.
2. One way: one fence, one metadata tree, one commit (a single conditional PUT).
3. Orphans are garbage for GC, never corruption: write order is chunks and packs,
   then shards, then `head`.
4. No polling. A deadline timer is the caller's.
5. No test-only environment variables or build tags.
6. LLM.md is the one project memory file; CLAUDE.md / AGENTS.md are local symlinks.
