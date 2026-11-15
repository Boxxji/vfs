# Specification notes

What follows is the behaviour of the on-disk layout and of the commit, fence and
sealing protocols, stated so that a third party can write a compatible reader or
writer. It describes what the reference implementation does today and what its tests
hold it to. "MUST", "SHOULD" and "MAY" are used as in RFC 2119. Sections marked
(informative) explain and bind no one. There is one layout: it carries no version
number, and nothing here describes any other.

## 1. Conventions

* Integers inside a record are varints: an unsigned integer is an unsigned LEB128
  (7 bits per byte, least significant group first, at most 10 bytes); a signed integer
  is that encoding of its zigzag form (`(n << 1) ^ (n >> 63)`). A byte string is a
  varint length and then the bytes. A decoder MUST reject a truncated item and MUST
  reject bytes left over after the last item of a record.
* Integers inside a key, and in the fence header, are big-endian and fixed width.
* A hash is BLAKE3 with a 32-byte output unless said otherwise.
* A Volume lives in one place: a prefix of a bucket of an object store, or a
  directory. The place has a canonical location string, the same for every process that
  opens it (`s3://<bucket>/<prefix>` for an object store, `file://<absolute directory>`
  for a directory). A reader and a writer MUST use the same string.

## 2. What the store must provide

A Volume needs a keyed store of byte strings with these operations. Keys below are
relative to the Volume's prefix, which is joined to them with one `/`.

| Operation | Contract |
|---|---|
| `Get(key, max)` | the bytes at `key` and a version token naming exactly those bytes; absent is `NotFound`; an object longer than `max` is `TooLarge` and is not returned |
| `GetRange(key, off, n)` | exactly `n` bytes from `off`; a range past the end is an error, never a short read |
| `Put(key, data)` | store `data`, replacing any object; used only for objects no one else writes (packs, shards) |
| `PutIfAbsent(key, data)` | store `data` only if `key` does not exist, atomically with the check; `Conflict` otherwise; returns the new version |
| `PutIfVersion(key, data, v)` | store `data` only if `key` exists and its version is `v`, atomically with the check; `Conflict` otherwise; of several calls naming one `v` exactly one succeeds; returns the new version |
| `Delete(key)` | remove; removing an absent key is not an error |

A version token is opaque and compared only for equality: two reads of an unchanged
object return the same token, and any write by anyone changes it. A store that cannot
write an object atomically under a condition (for instance because the object is too
large to send in one request) MUST refuse the conditional write with `TooLarge`; it
MUST NOT perform it unconditionally. The reference object-store backend sends an object
in one request up to 64 MiB and refuses conditional writes above that. The store is
trusted to return the object it holds, and not trusted with confidentiality: it learns
object sizes, key names and timing, and no content.

A prefix is a sequence of `/`-separated segments; no segment is empty, `.` or `..`,
none contains a control character (below 0x20, or 0x7f), and the whole is at most 512
bytes. A prefix that fails this MUST be refused, not cleaned.

## 3. Objects

A Volume is three kinds of object. Every object is a sealed frame (section 4).

| Key | Object | Mutable | Name |
|---|---|---|---|
| `head` | the Head, inside a fence record (section 5) | yes, only by conditional write | fixed |
| `meta/<2 hex>/<64 hex>` | a metadata shard (section 6) | no | the hash of the shard's sealed bytes, hex; the 2 hex digits are its first byte |
| `pack/<2 hex>/<32 hex>` | a pack of chunks (section 7) | no | 128 random bits, hex; the 2 hex digits are its first byte |

Everything but `head` is written once, under a name that is a hash of its bytes or a
random identifier, and never changes. A writer MAY write an immutable object again,
or after its caller gave up; the second write puts the same bytes at the same name. A
reader MUST check a shard against its name (the hash of the sealed bytes) and a chunk
against its id (section 7), and MUST treat a mismatch as corruption, not retry it.

## 4. Sealing

### 4.1 The Cipher

All object bytes pass through a Cipher, the only holder of key material. A Cipher
provides:

* `Seal(plaintext) -> (sealed, keyID)`: authenticated encryption. `keyID` names the key
  used, 1 to 255 bytes, and reveals nothing secret.
* `Open(sealed, keyID) -> plaintext`: fails if `sealed` was not produced by `Seal`
  under the key `keyID` names, or was altered, or if the Cipher does not hold that
  key (a distinct error, "unknown key").

A Cipher MAY hold several keys, sealing under the current one and opening any it
holds; that is key rotation, and needs no re-encryption. A Cipher MUST be safe for
concurrent use. When a Volume is configured it MUST check its Cipher: it seals a
random 64-byte probe and refuses the Cipher unless the sealed bytes contain neither
the probe nor its first 16 bytes, `Open` returns the probe, and `Open` fails when one
bit in the middle of the sealed bytes is flipped. This refuses a Cipher wired to
nothing; it does not prove a Cipher strong.

The reference implementation's default Cipher is a public-key hybrid of X25519 and
ML-KEM-768, producing one self-contained encrypted file per sealed object; its key id
is the first 8 bytes of the hash of the public recipient, in hex. (informative)

### 4.2 The frame and its context

A sealed object is `[len(keyID): 1 byte][keyID][sealed]`, where `sealed` is
`Seal(aad || plaintext)`. The `aad` names what the object is and is not stored: a
reader supplies the one it expects, and after `Open` MUST verify that the plaintext
begins with it, and refuse the object otherwise. So an object of another Volume, of
another kind or in another place fails closed even under a Cipher both Volumes share.

| Object | `aad` |
|---|---|
| Head | the ASCII bytes `vfs head ` followed by the canonical location string of the place |
| shard | the ASCII bytes `vfs shard ` followed by the 16-byte Volume id |
| chunk | the ASCII bytes `vfs chunk ` followed by the 16-byte Volume id and the 32-byte chunk id |

A Head is bound to its place: a Volume copied to another place does not open there
until its Head is sealed again for that place. Shards and chunks are bound to the
Volume, so they move with it.

## 5. The Head, the fence and the commit

### 5.1 The Head

The Head is the root of everything a reader may see: it names the metadata tree, so
the tree it names is exactly the Volume at the instant it was written. Its plaintext
is, in order: `Seq` (uvarint), `Root` (32 bytes), `Next` (uvarint), `ID` (16 bytes), `Seed` (32
bytes), and nothing after. It is sealed (4.2) and placed in a fence record.

* `Seq` numbers commit attempts. A writer takes the next number for every attempt,
  including one whose reply was lost, so no two Heads a Volume writes share a `Seq`; it
  can skip.
* `Root` is the ref (section 6) of the root shard, or 32 zero bytes for the empty tree.
* `Next` is the next inode number to assign.
* `ID` names the Volume. A new Volume draws it at random.
* `Seed` draws the Volume's chunking table (section 7.2). A new Volume draws it at
  random. It is as secret as the Volume.

A reader MUST bound its read of `head` (the reference implementation accepts at most
64 KiB).

### 5.2 The fence record

The Head object is `fence-magic || round || token || sealed-head`:

| Field | Size | |
|---|---|---|
| magic | 6 | the bytes `fence` and 0x00 |
| round | 8 | big-endian; the Round of the writer that wrote this object |
| token | 16 | random per writer process; identifies the writer, not the Volume |
| payload | rest | the sealed Head |

The Round is a number the caller supplies and that only grows. The store's conditional
write is what enforces it:

* **Create**: a new Volume writes `head` with `PutIfAbsent` at round 0.
* **Put at round r**: read `head`; if its bytes equal exactly the record this writer
  would write (same round, same token, same payload), the earlier attempt landed and
  the Put succeeds. Otherwise, if `r` is below the recorded round the Put fails with
  "stale round" and the object is untouched; otherwise write with `PutIfVersion` on
  the version just read. On `Conflict`, read again and repeat, at most 8 times.
* **Claim at round r** (taking the Volume over): read `head`; if `r` is below the
  recorded round fail "stale round"; call the hydrate step with the recorded payload;
  write the record with round `r`, the same payload and this writer's token, with
  `PutIfVersion`. On `Conflict`, re-read and repeat, at most 8 times, carrying forward
  whatever the winner wrote. A retry that finds exactly its own previous bytes has
  landed and succeeds.

The Volume is attached at a Round `r` that MUST be strictly greater than the round
recorded, and MUST NOT be 0 or 2^64-1 (no Round could follow the latter). A Volume
opened without a Round is read-only and claims nothing. After a successor has claimed,
no commit by a lower Round can land: its conditional write finds a different version, or
its read finds a higher round.

A store that returns an older Head that it once held cannot be told from the current
one by this protocol. A caller that must notice a rollback keeps the `Seq` of the last
Head it saw and compares.

### 5.3 The commit

A commit (`Flush`) makes the writer's tree durable. In order:

1. Every file with unsettled writes is settled: its bytes are cut into chunks, the
   chunks are sealed and appended to the open pack, and its records are staged.
2. The open pack, if any, is written with `Put`.
3. The staged records are written into new shards, every changed node of the tree and
   no other, leaf to root. Each is written with `Put`.
4. The Head is written, once, at the writer's Round, with `Seq` the next attempt
   number, `Root` the new root and `Next` the next inode number.

Only step 4 changes any object a reader can reach. A crash, or a failure, before it
leaves the previous Head and so the previous tree whole; the chunks, packs and shards
already written are unreferenced garbage, never corruption. Steps 2 and 3 are
idempotent by content address, so repeating them is harmless.

The outcome of a failed commit: `nil` means committed. Any other error means the
outcome of that attempt is unknown to the caller (a reply can be lost after the write
landed), and the writer keeps its changes for the next attempt. "Stale round" means
the writer was fenced and can never commit again; if the store's Head then holds
exactly the writer's root and Volume id (an earlier attempt landed and a successor
carried it forward), the commit counts as landed.

## 6. The metadata tree

All metadata is records in one ordered tree of immutable shards: a Merkle search tree
(a prolly tree). A record is a key and a value, both byte strings; keys sort
bytewise.

### 6.1 Records

| Key | Value |
|---|---|
| `01` `ino` | inode: a sequence of fields, `[tag: uvarint][length: uvarint][value]`, tags strictly ascending |
| `02` `dir` `name` | directory entry: child inode (uvarint), type bits of the mode, as in an inode (uvarint) |
| `03` `ino` | the whole bytes of a small file, inline |
| `03` `ino` `offset` | extent of a large file: chunk id (32 bytes), length (uvarint) |
| `04` `chunk id` | chunk table entry: pack id (16 bytes), offset (uvarint), length (uvarint) |

`ino`, `dir` and `offset` are 8 bytes, big-endian. `name` is the bytes of the name. The
root directory is inode 1. Inode numbers are assigned in increasing order from
`Head.Next` and are never reused within a Volume. A name is 1 to 255 bytes, contains
no `/` and no NUL, and is not `.` or `..`.

An inode's fields: tag 1 mode (uvarint, a 32-bit value: the low 9 bits are the permissions `rwxrwxrwx`
and bit 31 marks a directory; no other type bit is used yet), 2 size in bytes (uvarint), 3 mtime, 4 atime, 5 ctime (varint, nanoseconds
since the Unix epoch). Tags 1 to 5 MUST be present. A reader MUST check the fields it
knows, MUST reject tags that are not strictly ascending, MUST keep the bytes of fields
it does not know and write them back unchanged when it rewrites the inode, and MUST
NOT fail on them. A new attribute is a new tag. A new kind of record is a new first
byte of the key; a reader MUST ignore records of a table it does not know.

### 6.2 Shards and nodes

A node is `[level: uvarint][count: uvarint]` and then `count` pairs of byte strings
`(key, value)`, with `count` at least 1. Level 0 is a leaf: its pairs are records. At a
higher level each pair is the first key of a child and the child's ref (32 bytes), and
the child is at level one below. A shard is a node, sealed (4.2); its ref is the hash of
its sealed bytes and its object is `meta/<2 hex>/<hex of ref>`. The root's ref is
`Head.Root`. A reader MUST verify the hash of every shard it reads and MUST bound the
read (the reference implementation accepts at most 1 MiB, since a node ends at
512 KiB of plaintext by at most one record more).

### 6.3 Where a node ends

A tree is canonical: its shape depends on its records alone, so the tree built
incrementally by a sequence of changes is identical, shard for shard, to the tree
built from scratch from the same records. A writer MUST produce the canonical shape.
Entries of one level are taken in key order and cut into nodes: an entry ends its node
if either

* `size = len(key) + len(value) + 4` and `u32le(H[0:4]) * 65536 < size * 2^32`, where
  `H = BLAKE3(level_byte || key)`, `level_byte` is the level as one byte, and
  `u32le(H[0:4])` is the first four bytes of `H` as a little-endian unsigned integer
  (65536 is the average node size, 64 KiB); or
* the node's accumulated size (the sum of `size` over its entries) has reached
  512 KiB.

The last node of a level ends there whatever the entries. If a level yields one node,
that node is the root; otherwise its nodes become the entries of the next level up
(first key, ref). The empty tree has no shard and root ref of 32 zero bytes.

## 7. Files

### 7.1 Small files, extents and holes

A file's size is its inode's size and is at most 2^62 bytes. A file of at most 16 KiB
is stored inline: its bytes are the value of its `03 ino` record and nothing else. A
larger file is stored as extents: each record `03 ino offset` says that the file bytes
`[offset, offset+length)` are the chunk with the given id. Extents MUST NOT overlap, and
each `length` MUST equal the length of the chunk's plaintext and be at most 4 MiB. A
byte of the file that no extent covers is a hole and reads as zero; a hole costs no
record, no chunk and no object, however large. Bytes at or beyond the size do not
exist: a read there returns end of file, and a file made larger by truncation reads
zero in the new range. Truncating a file smaller discards extents past the new size
and shortens the one that crosses it; extending it afterwards MUST read zero, never the
discarded bytes.

### 7.2 Cutting chunks

A writer cuts the written bytes of a file into content-defined chunks. Each maximal
run of consecutive written bytes (bytes not separated by a hole) is cut on its own,
starting from its first byte. The cut is FastCDC with normalized chunking (Xia et al.,
USENIX ATC 2016), with these parameters; a reader does not need them, but writers that
agree on them produce the same chunks for the same bytes in the same Volume:

* minimum 64 KiB, average 512 KiB, maximum 4 MiB; `avgBits = 19`.
* The gear table `g[0..255]` is drawn per Volume: the first 2048 bytes of the BLAKE3
  extendable output over the bytes `hanzoai/vfs chunker gear table` followed by
  `Head.Seed`, read as 256 little-endian 64-bit words.
* For a window `b` (the next `min(4 MiB, remaining)` bytes of the run): if `len(b)` is at
  most the minimum, the chunk is all of `b`. Otherwise start with `fp = 0` at index
  `i = 64 KiB`; for `i` below `min(512 KiB, len(b))` set `fp = (fp << 1) + g[b[i]]` and cut
  after byte `i` if `fp & maskS == 0`; then for the rest of the window the same with
  `maskL`. If no cut is found the chunk is the whole window. `maskS` is the top 21 bits
  set (`(2^21 - 1) << 43`) and `maskL` the top 17 bits (`(2^17 - 1) << 47`).
* The chunk id is the hash of the chunk's plaintext. Within a Volume equal ids mean
  equal bytes, which is the whole of dedupe. A Volume never shares chunks with another,
  and ids appear only inside sealed shards.

Because the gear table is per Volume, chunk lengths do not identify a known file
across Volumes.

### 7.3 Packs

A chunk is sealed on its own (4.2, chunk context) and the sealed frames are laid end to
end in a pack, with no header and no index: where each lies is the chunk table (record
`04`). A writer uploads the open pack when it reaches 16 MiB or at a commit. A reader
fetches one chunk with a ranged read of its frame, opens it, and MUST check that the
hash of the plaintext is the chunk id.

## 8. What is durable when a call returns

The reference implementation's behaviour today, which any change MUST preserve or
extend. "Commit" is the successful return of the Head's conditional write (5.3).

| Call | Durable when it returns |
|---|---|
| write, truncate, create, make directory, remove | nothing. Held in memory; lost if the process dies. Past a bound (`Config.Dirty`, 64 MiB by default) the data is chunked and uploaded, still unreferenced and not durable |
| close of a file handle | nothing. It releases the handle |
| flush | everything written before the call, committed. It returns only after the Head advanced |
| fsync | the same as flush: a full commit. There is no cheaper durable point |
| flush on a mount (every `close(2)`) | the same as flush |
| release on a mount | nothing |
| end of a mount | one flush |
| close of the Volume | nothing; it does not flush |
| attach (takeover) | every commit the previous owner saw acknowledged is in the new owner's tree, and no commit by an older Round lands afterwards |

A later change (a node-local journal for fast fsync, group commit) MUST keep: flush
and the detach sequence return only after the Head advanced in the store; a takeover
loses no acknowledged commit; objects are written once under names that make
repeating a write harmless; and the Head's compare-and-swap is the only mutation. A
journal may make a call cheaper only by making it durable somewhere else first, and
then flush MUST still be a commit in the store.

On a mounted file system, a signal that reaches a waiting caller interrupts the
request. `fsync` then stops waiting and is answered "interrupted" while its commit
goes on by itself; read, write, close and the calls that change the tree are not
answered "interrupted" and return their own result. No interrupt cancels a commit or
a fetch. (informative; the reasons are in the implementation's notes.)

## 9. Limits

| What | Limit |
|---|---|
| file size | 2^62 bytes (4 EiB) |
| inline file | at most 16 KiB |
| chunk | 64 KiB to 4 MiB (a short final chunk of a run is allowed) |
| pack | uploaded at 16 MiB; a pack may exceed that by one frame |
| node (shard plaintext) | ends at 512 KiB; sealed shard read bound 1 MiB |
| Head read bound | 64 KiB |
| name | 1 to 255 bytes, no `/`, no NUL, not `.` or `..` |
| prefix | at most 512 bytes, segments as in section 2 |
| round | 1 to 2^64-2 for a writer |
| fence retries | at most 8 compare-and-set attempts per Put or Claim |
| key id | 1 to 255 bytes |

## 10. Conformance

A reader or writer is conformant when it passes the equivalent of these tests of the
reference implementation (all in the repository root unless a package is named):

| Property | Tests |
|---|---|
| objects are written once; crash at any point leaves the last commit | `TestSigkill` (six kill points), `TestFailureAtEveryWrite`, `TestLostAcknowledgementOfHead`, `TestKillOverRealServer` |
| fence: one winner, older Round refused, takeover loses nothing, lost acknowledgements | `pkg/fence` tests, `TestFenceRefusesOlderRound`, `TestAttachNeedsAHigherRound`, `TestTakeoverLosesNoAcknowledgedCommit`, `TestRacingClaims`, `TestSameRoundRace`, `TestAttachLostAckIsNotStale`, `TestRoundMaxIsRefused`, `TestHeadSeqNeverRepeats` |
| store contract | `internal/storetest` conformance suite, run on every backend; `TestRealServerCAS`, `TestRealServerRaceStress` |
| sealing context and the Cipher check | `TestSealedToItsContext`, `TestIdentityCipherIsRefused`, `TestWrongKeyFailsClosed`, `TestTamperedObjectsAreRefused`, `TestSwappedChunkIsRefused`, `TestChunkHashIsChecked` |
| rotation | `TestRotation` |
| records and the tagged inode | `TestInodeRecordIsExtensible`, `TestHeadRejectsGarbage` |
| canonical tree | `TestTreeMatchesModel`, `TestTreeForcedCuts` |
| chunking | `TestCutBounds`, `TestCutStableUnderEdit`, `TestGearTableIsFixed`, `TestGearDiffersPerVolume`, `TestChunkListIsTheWholeFileCut` |
| holes and truncation, byte for byte against a plain model | `TestSparseExact`, `TestSparseRandom`, `TestSparsePetabyte`, `TestModelFuzz` |
| bounded reads | `TestHeadBound`, `TestShardBound`, `TestBackendBodyIsBounded`, `TestReadIsBounded` |
| prefixes | `TestValidPrefix`, `TestPrefixConfinement` |
| durability and interrupts on a mount | `TestFUSEFsyncSurvivesKill`, `TestFUSEInterruptsAreNotErrors`, `TestCommitSurvivesItsCaller` (`pkg/mount`) |

## 11. Not specified

Rename, links, symbolic links, owner and mode changes, extended attributes, locks and
file-system statistics have no records yet; a mount answers them "not supported". There
is no garbage collection of unreferenced objects, no snapshot or clone, and no way to
seal a Head for a new place. Deadlines, retries and back-off are the caller's. Sealed
sizes depend on the Cipher.
