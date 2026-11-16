# Specification notes

What follows is the behaviour of the on-disk layout and of the commit, fence and
sealing protocols, stated so that a third party can write a compatible reader or
writer. It describes what the reference implementation does today and what its tests
hold it to. "MUST", "SHOULD" and "MAY" are used as in RFC 2119. Sections marked
(informative) explain and bind no one. There is one layout: it carries no version
number, and nothing here describes any other. Every byte this text fixes is pinned by a
known-answer test that rebuilds it from the text alone, with each constant spelled out
(`spec_test.go` and `pin_test.go`, section 10), or by a fixture of what an outside library
produced (the Cipher): a change that moves a byte of the layout fails there, whatever the
other tests read back.

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
  for a directory). A reader and a writer MUST use the same string. The string is
  canonical: the prefix has no leading or trailing `/`; the directory is absolute,
  cleaned (no `.`, `..`, doubled or trailing `/`) and has its symbolic links resolved,
  so every spelling of one place gives one string. It never contains a credential, an
  endpoint or a query.

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
| `List(prefix)` | every object whose key begins with `prefix` as a string (`ext/y/a` lists `ext/y/ab` and `ext/y/a/c`), each with its key, its size and the time the store last modified it, by the store's own clock; none is hidden |
| `Stat(key)` | the size of the object at `key` without its body; absent is `NotFound` |

A Volume reads and writes with the first six. `List` and `Stat` are for tools: a
collector (11.1), and the check of a copied Volume (5.4).

A key is 1 to 511 bytes of `a-z`, `0-9` and `-`, in segments of 1 to 255 bytes separated
by `/`, and every operation MUST refuse another (`InvalidKey`) without effect. That is
the alphabet that every store keeps as written: a case-insensitive file system, one that
normalises names, one that gives `.` and `..` a meaning, or one that reads `%2e` as a dot
would each keep a wider one differently, and a file backend would need a name it cannot
make. The longest key leaves a 512-byte prefix room in the 1,024 bytes that an object
store keeps. The keys of this text are all of the grammar, and all of one kind have the
same number of segments, so no key is a prefix of another at a `/`, which a store that
keeps keys as files could not hold; a key written that is not of that shape (an
extension's, by mistake) MAY be refused by such a store, and a read of one that is only
the directory of other keys answers as absent or empty, whichever the store does.

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
bytes. A prefix that fails this MUST be refused, not cleaned. A Volume's prefix MUST NOT
nest inside another's, in either direction (`vols/a` and `vols/a/b`): a listing of one
would return the other's objects, and a collector of the first would find them unknown,
or worse, take them for its own. Two Volumes share a bucket by prefixes that name different
segments (`vols/a`, `vols/b`).

## 3. Objects

A Volume is three kinds of object, and may hold others (3.1). Every object is a sealed
frame (section 4).

| Key | Object | Mutable | Name |
|---|---|---|---|
| `head` | the Head, inside a fence record (section 5) | yes, only by conditional write | fixed |
| `meta/<2 hex>/<64 hex>` | a metadata shard (section 6) | no | the hash of the shard's sealed bytes, hex; the 2 hex digits are its first byte |
| `pack/<2 hex>/<32 hex>` | a pack of chunks (section 7) | no | 128 random bits, hex; the 2 hex digits are its first byte |

Of these three, everything but `head` is written once, under a name that is a hash of
its bytes or a random identifier, and never changes. A writer MAY write an immutable
object again, or after its caller gave up; the second write puts the same bytes at the
same name. A reader MUST check a shard against its name (the hash of the sealed bytes)
and a chunk against its id (section 7), and MUST treat a mismatch as corruption, not
retry it.

### 3.1 Objects this text does not define

Something that is not the tree of the Volume, such as a hint that makes the next
attachment faster, can be stored beside it. Such an object is an extension object, and
the rules for one are these.

* **Unknown objects are left alone.** A reader MUST ignore every key it does not know
  and MUST NOT fail because one exists. A collector, or any tool that removes objects,
  MUST NOT delete an object it does not know: it deletes only an object under `meta/`
  or `pack/` whose key has exactly the shape in the table above and that no Head it
  must keep reaches. A tool that finds a key of another shape, under `meta/` or `pack/`
  or anywhere else, reports it and leaves it.
* **Naming.** An extension object lives at `ext/<name>/<rest>`. `<name>` is 1 to 32
  bytes of `a-z`, `0-9` and `-` and names the extension. `<rest>` is the BLAKE3 hash of
  the caller's key, in 64 lower case hex digits, whatever the key is: the extension's own
  name for an object (a workflow, a snapshot) is bytes of its choosing and is never part of
  the key in the store. So every extension key is one segment of one length: none is a
  prefix of another, the alphabet is the grammar of section 2 and no store holds it
  differently from another, and the whole key is at most 101 bytes. An extension that must
  enumerate its objects keeps an index object of its own, as it keeps any other state;
  `List` shows hashes. Nothing but an extension defined this way uses `ext/`.
* **Sealing.** An extension object is a sealed frame (4.2) whose context has the kind
  `ext` and the fields: the 16-byte Volume id, the extension's `<name>` as bytes, and
  `<rest>` as bytes. It is bound to the Volume (it moves with it, as shards do, and
  needs no reseal) and to its own key, so an object of one Volume, of another
  extension or of another key does not open.
* **Size.** An extension object holds at most 32 MiB, and a reader MUST bound its read of
  one at that and the room the seal takes (the reference implementation reads 64 KiB
  more).
* **Fencing.** The Head's Round orders the commits of the tree; it does not cover
  extension objects. A writer that has been fenced out can still write one. An
  extension MUST therefore be safe against that: whatever version of one a reader finds,
  an old one, a stale writer's or none, the contents of the Volume are what its Head
  says, and the extension is only a hint, or says what a stale write costs and why it
  is bearable. State the tree depends on is not an extension: it is a change to this
  text.
* **Replacement.** A mutable extension object is created with `PutIfAbsent` and
  replaced with `PutIfVersion`, so a reader finds the old object or the new one, whole,
  and two writers do not overwrite each other's replacement unseen. An immutable one
  is named by a hash of its sealed bytes or by a random identifier, as in the table (an
  extension whose objects are immutable keeps its own index of them).
* **Unreachable is not garbage.** The rule that an object no Head reaches is garbage
  (section 11.1) applies to the three kinds of the table and to no extension object.
  An extension that wants reclamation defines it, and a collector applies it only if it
  implements that extension.

The reference implementation has `GetExt` and `PutExt` for them. The first user is the
trace store of `docs/prefetch.md` (S6), one object per workflow key at
`ext/trace/<hash of the workflow key>`.

## 4. Sealing

### 4.1 The Cipher

All object bytes pass through a Cipher, the only holder of key material. A Cipher
provides:

* `Seal(plaintext) -> (sealed, keyID)`: authenticated encryption. `keyID` names the key
  used, 1 to 255 bytes, and reveals nothing secret.
* `Open(sealed, keyID) -> plaintext`: fails if `sealed` was not produced by `Seal`
  under the key `keyID` names, or was altered, or if the Cipher does not hold that
  key (a distinct error, "unknown key").

Authenticated means authenticated against the holder of the key material that opens:
`Open` MUST refuse an object that a party without that material could have made. A
public-key construction does not give that by itself, since whoever holds the public
recipient can encrypt to the Volume; the Cipher MUST add something only the holders of
the secret can produce. A Cipher that does not is a Cipher under which anyone who
learns the recipient can forge a Head or a shard.

A Cipher MAY hold several keys, sealing under the current one and opening any it
holds; that is key rotation, and needs no re-encryption. A Cipher MUST be safe for
concurrent use. When a Volume is configured it MUST check its Cipher: it seals a
random 64-byte probe and refuses the Cipher unless the sealed bytes contain neither
the probe nor its first 16 bytes, `Open` returns the probe, and `Open` fails when one
bit in the middle of the sealed bytes is flipped. This refuses a Cipher wired to
nothing; it does not prove a Cipher strong. Because a Cipher can stop sealing after it
was checked (a fallback to the identity when its key service is gone), the Volume also
checks every `Seal`: the sealed bytes MUST NOT contain the context (4.2), and a Seal
whose output does is an error, not a write.

The reference implementation's default Cipher is a public-key hybrid of X25519 and
ML-KEM-768, producing one self-contained encrypted file per sealed object; its key id
is the first 8 bytes of the hash of the public recipient, in hex. Its integrity rests
on the secrecy of the identity, not of the recipient: the plaintext carries a final
32-byte tag, the BLAKE3 keyed hash of everything before it under a key derived from
the secret identity (`DeriveKey("hanzoai/vfs seal tag", identity string)`), and `Open`
refuses an object whose tag does not verify. A party that holds only the public
recipient can encrypt to the Volume but cannot make an object the Volume accepts.
Everything that can open an object can also seal one. (informative)

### 4.2 The frame and its context

A sealed object is `[len(keyID): 1 byte][keyID][sealed]`, where `sealed` is
`Seal(ctx || plaintext)`. The context `ctx` names what the object is and is not
stored. It is always exactly 32 bytes: the BLAKE3 digest of

    "hanzoai/vfs seal context" || frame(kind) || frame(field 1) || frame(field 2) ...

where `frame(x)` is the length of `x` as an unsigned varint followed by `x`, and the
kind and fields are:

| Object | kind | fields |
|---|---|---|
| Head | `head` | the canonical location string of the place |
| shard | `shard` | the 16-byte Volume id |
| chunk | `chunk` | the 16-byte Volume id, the 32-byte chunk id |

A reader supplies the context it expects, and after `Open` MUST check that the first
32 bytes of the plaintext are equal to it, byte for byte, and refuse the object
otherwise; the plaintext proper is what follows. Because the context has a fixed
length and its preimage is length-framed, no context is a prefix of another and two
different (kind, fields) never share one, so an object of another Volume, of another
kind or of another place fails closed even under a Cipher both Volumes share. A
variable-length context compared as a prefix would not: a Head sealed for
`.../vol10` would open at `.../vol1` and at the bucket root.

A Head is bound to its place: a Volume copied to another place does not open there
until its Head is sealed again for that place. Shards and chunks are bound to the
Volume, so they move with it, and moving a Volume is copying its objects and then
resealing the Head (5.4).

## 5. The Head, the fence and the commit

### 5.1 The Head

The Head is the root of everything a reader may see: it names the metadata tree, so
the tree it names is exactly the Volume at the instant it was written. Its plaintext
is, in order: `Seq` (uvarint), `Root` (32 bytes), `Next` (uvarint), `ID` (16 bytes), `Seed` (32
bytes), `Journal` (uvarint), `Needs` (uvarint), and then the fields a later build appends
(none today). It is sealed (4.2) and placed in a fence record.

* `Seq` numbers commit attempts. A writer takes the next number for every attempt,
  including one whose reply was lost, so no two Heads a Volume writes share a `Seq`; it
  can skip.
* `Root` is the ref (section 6) of the root shard, or 32 zero bytes for the empty tree.
* `Next` is the next inode number to assign.
* `ID` names the Volume. A new Volume draws it at random.
* `Seed` draws the Volume's chunking table (section 7.2) and keys the boundaries of
  the metadata tree (section 6.3). A new Volume draws it at random. It is as secret as
  the Volume: it is what a party who chooses file names does not know.
* `Journal` is the highest sequence of the node journal (`docs/journal.md`) whose effects
  the Head includes: what the store holds of it, as the journal's `Published`. It is 0
  when there is no journal. A writer that rewrites the Head and does not publish carries it
  unchanged.
* `Needs` is a set of feature bits, 0 today. A reader MUST refuse a Head whose `Needs` has a
  bit it does not know, and change nothing (`ErrNeeds`). A writer sets a bit only for a
  feature it implements. This is how a change that every writer, or every collector, must
  honour reaches the Volume without a flag day: it sets a bit, and appends the fields it
  needs after `Needs`, and a build that lacks the feature refuses instead of damaging
  the Volume.
* What follows `Needs` is the fields of later builds. A reader that knows every bit set in
  `Needs` MUST ignore those bytes, and a writer that rewrites the Head MUST write them back
  unchanged. A field that can be ignored (a hint, a counter nothing depends on) is
  appended without a bit and never forces a flag day.

The bits below are decided and RESERVED: no build sets one yet, and each is named so that
two changes do not take the same bit. Each, when set, appends the fields its section says
after `Needs`.

| Bit | Name | Set when |
|---|---|---|
| 0 | `snapshots` | the Volume has retained roots, stored as extension objects (3.1), that a collector must keep: a collector that does not know the bit would delete what they reach, so a build that does not know it refuses the Volume |
| 1 | `links` | inodes carry link counts that every unlink must adjust (6.1): a writer that does not keep them would leave them wrong |
| 2 | `base` | chunks are read through a base layer shared with other Volumes (11.1): a field after `Needs` names the layer, and a shared read key opens its chunks |

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
opened without a Round is read-only and claims nothing. After any error from an attach
other than a refusal for a Round that is too low, the claim may or may not have
landed (a reply can be lost), and a caller that tries again MUST use a higher Round: a
claim that landed with its reply lost is recognised as the claimant's own only by a
retry inside the same call, which finds its own bytes, and a later call at the same
Round is told "stale round". After a successor has claimed,
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

### 5.4 Moving a Volume

Only the Head is bound to the place (4.2). To move a Volume to another place, copy
every object under its prefix to the new place and then reseal the Head there: read
`head` through the fence, open its sealed payload (below), check that it decodes as a
Head, seal it again under the context of the new place, and write it with the Claim
procedure (5.2) at a Round strictly greater than the one recorded (neither 0 nor
2^64-1). The reseal fences the copy only: a writer still attached at the old place is
not told, so it must be stopped first. A writer that commits without pause can win the
race for the Head eight times, and the reseal then fails with a lost conditional
write. A failed reseal is retried at a higher Round.

Whichever way the payload was opened, the copy is checked before anything is written: the
root shard that the Head names MUST open at the new place under the Volume id the Head
carries and MUST hold the root directory (inode 1). That refuses a copy that lacks its
objects, a Head of another Volume, and an object of another kind that a writer of the store
put where the Head goes. A reseal MAY also be asked for the full check (`--verify`): every
shard the Head reaches opens, and every pack that a chunk record names is present and long
enough to hold it. The check reads no chunk; a chunk is checked against its hash when a
file is read.

The payload is opened in one of three ways, tried in this order:

1. under the context of the old place, given as its canonical location string (1);
2. if that fails because the context differs, under the context of the new place: the
   Head is already sealed there, a reseal whose reply was lost for instance, and it is
   left as it is and only the Round is claimed. A reseal can therefore be repeated at
   a higher Round whatever it is told the old place was;
3. if the old place is not given, with the key alone, ignoring the context. This
   trusts the key (only a holder of it can have written a Head that opens, because of
   the tag in 4.1) and trusts that the Head found is the newest of the Volume the
   operator means. It checks one thing more: the root shard that the Head names MUST
   open at the new place under the Volume id the Head carries, which refuses a copy
   that lacks its objects and a Head of another Volume. It cannot tell a Head from
   before the newest commit from the newest, as no one can without the place.

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

An inode's fields: tag 1 mode (uvarint, a 32-bit value, below), 2 size in bytes
(uvarint), 3 mtime, 4 atime, 5 ctime (varint, nanoseconds since the Unix epoch). Tags 1 to 5 MUST be present. A reader MUST check the fields it
knows, MUST reject tags that are not strictly ascending, MUST keep the bytes of fields
it does not know and write them back unchanged when it rewrites the inode, and MUST
NOT fail on them. A new attribute is a new tag. A new kind of record is a new first
byte of the key; a reader MUST ignore records of a table it does not know.

The mode is a 32-bit value whose bits are fixed here, so that a later record never has
to reinterpret one:

| Bits | Meaning |
|---|---|
| 0 to 8 | permissions `rwxrwxrwx` (octal 0777) |
| 20 | sticky |
| 22, 23 | setgid, setuid |
| 31 | directory |
| 27 | symbolic link |
| 26, 25, 24, 21 | device, named pipe, socket, character device |
| 19 | irregular file |
| 28, 29, 30 | reserved |

A regular file has none of the type bits (19, 21, 24 to 27, 31). The type bits of the
mode are the ones listed (19, 21, 24, 25, 26, 27, 31), and a directory entry (6.1)
records those only. A writer MUST NOT set a bit this table does not list; a reader
MUST keep bits it does not understand when it rewrites an inode and MUST NOT act on
them. The reference implementation sets bit 31 and the permissions today and no other.

Fields are optional by default: a reader that does not know one keeps it and ignores
its meaning. A field that some writer must maintain for the Volume to stay consistent
is critical: a link count that every unlink must adjust is one (the reserved `links` bit),
an owner is not. A critical field MUST be announced by a bit in `Head.Needs` (5.1). A
reader that does not know the bit refuses the Volume, instead of rewriting inodes behind
the field's back.

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

A tree is canonical: its shape depends on its records and the Volume's seed alone, so the tree built
incrementally by a sequence of changes is identical, shard for shard, to the tree
built from scratch from the same records. A writer MUST produce the canonical shape.
Entries of one level are taken in key order and cut into nodes: an entry ends its node
if either

* `size = len(key) + len(value) + 4` and `u32le(H[0:4]) * 65536 < size * 2^32`, where
  `H` is the BLAKE3 keyed hash, under the key `K` below, of `level_byte || key`,
  `level_byte` is the level as one byte, and `u32le(H[0:4])` is the first four bytes of
  `H` as a little-endian unsigned integer (65536 is the average node size, 64 KiB); or
* the node's accumulated size (the sum of `size` over its entries) has reached
  512 KiB.

The key is `K = DeriveKey("hanzoai/vfs tree boundaries", Head.Seed)` (32 bytes). It is
secret to the Volume, and that is its purpose: with an unkeyed hash a party who chooses
file names (an archive being unpacked) can choose names that each end their own node,
and 2,000 such names cost 2,007 shards instead of 4. With the key the boundaries fall
where they fall, and the tree is a pure function of the records and the seed: two
writers that hold the Head build the same tree, and a writer that does not hold it
cannot predict where a node will end.

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

## 8. What is durable when a call returns (today)

The reference implementation's behaviour today, which any change MUST preserve or
extend. "Commit" is the successful return of the Head's conditional write (5.3).

| Call | Durable when it returns |
|---|---|
| write, truncate, create, make directory, remove | nothing. Held in memory; lost if the process dies. Past a bound (`Config.Dirty`, 64 MiB by default) the data is chunked and uploaded, still unreferenced and not durable |
| close of a file handle | nothing. It releases the handle |
| flush | everything written before the call, committed. It returns only after the Head advanced |
| fsync | the same as flush: a full commit. There is no cheaper durable point |
| flush on a mount (every `close(2)`) | the same as flush: a full commit, also for a handle that wrote nothing |
| release on a mount | nothing |
| end of a mount | one flush |
| close of the Volume | nothing; it does not flush |
| attach (takeover) | every commit the previous owner saw acknowledged is in the new owner's tree, and no commit by an older Round lands afterwards |

This table is what the code does now, and it is not the target. The draft HIP for this
layout (HIP-1342, section 8) states a different behaviour for `close`: a close does not
commit by itself, and a read-only handle's close does not publish another handle's
writes. Today every close on a mount is a flush, and a flush commits everything any
handle wrote, so a read-only close can publish another handle's writes. (A flush with
nothing to commit already makes no store request: `TestCleanFlushMakesNoStoreRequest`.)
The change that adds the journal changes the rows for `close` and for a flush on a
mount, and this section with them; until then the table above binds.

A later change (a node-local journal for fast fsync, group commit) MUST keep: flush
and the detach sequence return only after the Head advanced in the store; a takeover
loses no acknowledged commit; objects are written once under names that make
repeating a write harmless; and the Head's compare-and-swap is the only mutation of
the tree. A journal may make a call cheaper only by making it durable somewhere else
first, and then flush MUST still be a commit in the store.

On a mounted file system, a signal that reaches a waiting caller makes the kernel
interrupt the request, and no request is answered "interrupted": every call waits for
its own result, and no interrupt cancels a commit or a fetch. (informative) The reason
is what callers do with the answer, measured on Linux: a C caller does not retry
`fsync(2)`, so SQLite reported an I/O error from it (a database over a mount failed
7 runs of 7 on one kernel, and a C caller under a timer saw 246,912 interrupted
fsyncs in 5,000); `stat(2)` and directory reads answered "interrupted" reached C callers
as errors 1,586 times in 60,000 calls without `SA_RESTART` and 2,884 with it; `read(2)`
reached its caller as `EIO`; `close(2)` is never restarted; a restarted create with
`O_EXCL` meets its own file. The cost is in section 11.2.

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
| sealing context | exactly 32 bytes |
| default Cipher tag | 32 bytes at the end of the plaintext |

## 10. Conformance

A reader or writer is conformant when it passes the equivalent of these tests of the
reference implementation (all in the repository root unless a package is named):

| Property | Tests |
|---|---|
| every byte of the layout | `TestSpecContexts`, `TestSpecFrameAndTag`, `TestSpecHead`, `TestSpecFenceRecord`, `TestSpecRecords`, `TestSpecNodeAndBoundary`, `TestSpecChunker`, `TestSpecExtension` (each rebuilds the bytes from this text and compares), `TestObjectKeysAreTheSpecShapes`, `TestFenceTokenIsSixteenBytes`, `TestTheSizesOfTheText`, and `TestTheCipherOpensAGoldenObject`, which holds the default Cipher's format to an object that the library produced once |
| the Head: its fields, the journal sequence, the bits, what a later build appends | `TestHeadRejectsGarbage`, `TestHeadNeeds`, `TestTheHeadHasRoomForTheJournalSequence`, `TestHeadFieldsSurviveACommit`, `TestHeadOfALaterBuildIsRefusedByEveryDoor` |
| objects are written once; crash at any point leaves the last commit | `TestSigkill` (six kill points, every run), `TestFailureAtEveryWrite`, `TestLostAcknowledgementOfHead`, `TestKillOverRealServer` |
| fence: one winner, older Round refused, takeover loses nothing, lost acknowledgements | `pkg/fence` tests, `TestFenceRefusesOlderRound`, `TestAttachNeedsAHigherRound`, `TestTakeoverLosesNoAcknowledgedCommit`, `TestRacingClaims`, `TestSameRoundRace`, `TestAttachLostAckIsNotStale`, `TestAttachAfterALostClaimNeedsAHigherRound`, `TestRoundMaxIsRefused`, `TestHeadSeqNeverRepeats` |
| store contract, the key grammar, listing by string prefix with sizes and times | `internal/storetest` conformance suite, run on every backend (the memory store, the directory, an S3 protocol server and the real server); `TestRealServerCAS`, `TestRealServerRaceStress`, `TestValidKey` |
| extension objects: one behaviour on every store, sealed to Volume, name and key, left alone | `TestExtensionObjectsOnEveryStore`, `TestExtensionObjectsAndTheFence`, `TestExtensionObjectsRaced`, `TestExtensionObjectIsSealedToItsVolumeNameAndKey`, `TestStrayKeysAreIgnoredAndKept` |
| sealing context, exact and injective | `TestSealedToItsContext`, `TestHeadContextIsExact`, `TestContextsDoNotCollide`, `TestSwappedHeadIsRefused`, `TestChunkHashIsChecked` |
| the Cipher: checked at configuration and at every Seal; unforgeable with the recipient alone | `TestIdentityCipherIsRefused`, `TestCipherThatStopsSealingIsCaughtAtEverySeal`, `TestSealCatchesAHeaderedFallback`, `TestNoPublicTagKeyVerifies`, `TestShortForgedPlaintextIsRefusedNotPanicked`, `TestContextIsComparedWhole`, `TestRecipientAloneCannotForge`, `TestWrongKeyFailsClosed`, `TestTamperedObjectsAreRefused`, `TestSwappedChunkIsRefused` |
| places and moving a Volume | `TestPlaceSpellings`, `TestPlaceStringsAreOneSpelling`, `TestPlacesAreRefusedNotCleaned`, `TestRedact`, `TestRedactionCases`, `TestRedactionLeaks`, `TestRedactionOfEverySpelling`, `TestErrorsDoNotEchoACredentialInAPrefix`, `TestResealMovesAVolume`, `TestResealWrongFromChangesNothing`, `TestResealCrashAtTheWrite`, `TestResealIsRepeatable`, `TestResealWithoutThePlace`, `TestResealOfALiveVolume`, `TestResealRacesAWriter`, `TestResealRefusesTheLastRound`, `TestResealRefusesAGarbageHead`, `TestResealChecksTheCopy`, `TestResealWrongKeyChangesNothing`, `TestResealWithoutAPlaceRefusesAnotherKindOfObject`, `TestPartialCopyFailsClosed`, `TestCLIReseal` (`cmd/vfs`), `TestFileOpenCreatesNothing` (`pkg/backend/file`) |
| keyed tree boundaries | `TestGroundNamesDoNotBloatTheTree`, `TestBoundariesDependOnTheSeed` |
| rotation | `TestRotation` |
| records and the tagged inode | `TestInodeRecordIsExtensible`, `TestHeadRejectsGarbage` |
| canonical tree | `TestTreeMatchesModel`, `TestTreeForcedCuts` |
| chunking | `TestCutBounds`, `TestCutStableUnderEdit`, `TestGearTableIsFixed`, `TestGearDiffersPerVolume`, `TestChunkListIsTheWholeFileCut` |
| holes and truncation, byte for byte against a plain model | `TestSparseExact`, `TestSparseRandom`, `TestSparsePetabyte`, `TestModelFuzz` |
| bounded reads | `TestHeadBound`, `TestShardBound`, `TestBackendBodyIsBounded`, `TestReadIsBounded` |
| prefixes | `TestValidPrefix`, `TestPrefixConfinement` |
| durability and interrupts on a mount | `TestFUSEFsyncSurvivesKill`, `TestFUSEInterruptsAreNotErrors`, `TestFsyncWaitsForItsCommit`, `TestCloseWaitsForItsCommit`, `TestReadWaitsForItsData`, `TestLookupIsNotAnsweredEINTR`, `TestAttrIsNotAnsweredEINTR`, `TestReaddirIsNotAnsweredEINTR`, `TestMkdirIsNotAnsweredEINTR`, `TestRemoveIsNotAnsweredEINTR`, `TestCreateIsNotAnsweredEINTR`, `TestEveryHandlerOutlivesItsRequest`, `TestFsyncCallersShareTwoCommits`, `TestFsyncJoinsAFlightThatCoversIt`, `TestKilledColdLookupIsFreedAfterTheGrace`, `TestErrnoMapping`, `TestFinishCommitsWhatTheMountWrote` (the flush at the end of a mount), `TestFUSETermStopsAMountInUse`, `TestFUSEReadsExistingFilesOverS3`, and `TestFUSESQLite` (`internal/sqlitecheck`) on a Linux kernel |
| full-size randomised runs | `TestSparseExact`, `TestSparseRandom`, `TestSparseCanonicalRuns`, `TestSparseTinyBounds` (the `sparse` gate, not `-short`) |

## 11. Not specified, and known limits

Rename, links, symbolic links, owner and mode changes, extended attributes, locks and
file-system statistics have no records yet; a mount answers them "not supported". There
is no garbage collection of unreferenced objects (11.1 says how it will be done), no
quota or usage accounting, and no snapshot or clone. Retries and back-off are the
caller's. Sealed sizes depend on the Cipher.

### 11.1 Collection (decided, not built)

A collector runs online, against live writers, and is mark and sweep. It is built on these
rules, and a change to a rule is a change to this text.

* **Mark.** From the Head's `Root` and from every retained root. A Volume with the
  `snapshots` bit (5.1) has retained roots as extension objects (3.1), and a collector that
  does not know the bit refuses the Volume, which is why the bit exists. The mark follows
  every shard to its children and every chunk record to its pack.
* **Sweep.** It deletes an object only if it is under `meta/` or `pack/` with exactly the
  shape of section 3, it is not marked, and it is older than the grace below. It deletes
  nothing under a key it does not know, whatever it is (3.1).
* **Age is the store's.** An unmarked object may be one a writer has uploaded for a commit
  that has not landed, and pack names are random, so nothing but time tells it from garbage.
  Age is measured from the object's modification time as the store reports it in `List`
  (`Object.Modified`), against the store's own time, which a collector learns by writing an
  object of its own and listing it: never against a writer's clock or the collector's. The
  reference interface lists keys, sizes and modification times.
* **Grace.** Longer than the longest a commit can take, which is the longest time between
  the first object of a commit being written and the Head naming it. That is bounded only
  when every store request has a deadline (11.2), so no collector is built before them: a
  writer ends a commit within the deadlines of its requests, and the grace is more than
  twice their sum. A collector marks from the Head it reads after the listing, not before,
  so a commit that landed during the listing is marked.
* **Volumes never share chunks**, and stay so until a build that sets the `base` bit (5.1)
  exists: a base layer shared across Volumes needs a field after `Needs` that names it and
  a read key that opens its chunks for every Volume that uses it, and a collector of one
  Volume never deletes what another can reach. Until then every object of a Volume is
  reachable from that Volume's roots alone, and a prefix does not nest in another (2).

### 11.2 Known limits

Known limits of the reference implementation. None changes the layout.

* **A stalled store stalls its callers, and a kill frees few of them.** A store request has
  no deadline, and no call is answered "interrupted" (section 8). The kernel keeps a request
  that reached the daemon until the daemon answers it, so with a store that has stopped
  answering, a caller that is killed stays in the kernel (state D) until the store
  resumes, except where noted. Measured with the store stopped:
  * a cold lookup (the store is asked, no commit is running), stat and readdir of a name
    not yet read: freed in about 3 s, the grace after which an interrupted request that
    changes nothing is answered "interrupted" (a mount that answered it at once freed the
    caller in 0.03 s, and C callers could not take the answer);
  * anything behind a stalled commit (stat, a lookup of an absent name, readdir, mkdir,
    unlink, truncate by path, fsync on a read-only descriptor): not freed, because the
    Volume's lock is held across the commit and the lock does not honour a request's
    context. This needs a lock that does, in the deadlines change;
  * read, create, truncate and write, and fsync on a descriptor: not freed. The
    caller's exit closes its descriptor, which sends the kernel's flush, which waits for
    the same commit; close and exit are not freed behind a stalled commit either;
  * killing the daemon frees every caller at once (the connection ends, 20 ms) and loses
    what was not committed.
  Memory: the FUSE library holds about 134 KiB for each abandoned request until its
  handler returns. 1,600 reads at distinct offsets, 1,600 more through the page cache and
  3,000 distinct lookups took the daemon from 16.8 MB to 831 MB; nothing panicked and the
  data was intact when the store resumed. Bounding what a handler allocates does not change
  it. Only answering the abandoned requests frees them, which is what a deadline on the
  store does, and it is the next change.
* **Before a tenant is given a mount**, the supervisor must: watch the store's latency and
  kill the daemon when it does not answer (callers are freed in 20 ms; what the daemon had
  not committed is lost); set `pids.max` and a memory limit for the mount's callers and for
  the daemon (about 134 KiB for each stuck request); and unmount lazily at teardown
  (`fusermount3 -u -z`), because a mount in use refuses a plain unmount.
* **SIGTERM** commits what the daemon holds, asks for the unmount, and if something holds the
  mount open says so, asks again for 5 s and then detaches it lazily and ends within 3 s
  more. What a holder writes after the commit is lost with the connection. A supervisor that
  escalates to SIGKILL, or kills at once, loses what was not committed.
* **Every close is a full commit** (section 8): about 143 files per second and 5.7 objects
  per file in `cp -r` and in a checkout through the mount, against about 0.04 objects per
  file for the same files written and committed once.
* **Not built:** rename, hard links, symbolic links, chmod, chown, utimens, extended
  attributes, statfs, and a read of a file that was unlinked while open. `git`, editors that
  save by renaming, `tar` and `npm` fail on them. A read of an unlinked open file answers
  ENOENT on a 6.8 kernel and EIO on a 7.0 one, as each kernel reads the answer to its
  attribute request that the inode is gone.
* **No quota, no usage accounting, no collection.** A Volume only grows (11.1).
* **Tenants must not hold the store's write credentials.** The Round in the fence header
  is not authenticated: it is checked against the recorded Round by conditional writes, not
  by the Cipher, and whoever can write the store can set it, as they can delete the Head.
  The store is trusted with availability and with ordering, not with confidentiality.
* **Metadata grows with the number of chunks** of a file: a chunk table is a record of
  the tree, so the shards a file costs grow in proportion to its chunks.
* **A chunk is read whole to be authenticated.** Its tag covers all of it, so a read of
  a few bytes fetches the chunk; sub-chunk reads cannot be added without a different
  sealing.
* **A retired key stays trusted** for as long as it is in the Cipher: it opens, and
  its holder can seal. Dropping it makes what only it sealed unreadable (rotation).
* **Keyed boundaries stop a guest, not an observer.** The boundaries are keyed from the
  Volume's seed so that a guest who picks names cannot grind them to bloat the tree
  (6.3). They do not hide from an observer of the store how many shards each commit
  writes.
* **A reseal can lose the race** against a writer that commits without pause (5.4);
  stop the writer first.
* The `replica/` package keeps a SQLite replica current by a ticker; it is not part
  of this layout and is not specified.
