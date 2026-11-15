# Journal

`github.com/hanzoai/vfs/journal` is an append-only log on the node's local disk.
It lets a write acknowledgement and an fsync run at the speed of local NVMe while
the object store catches up behind it. It is a standalone package: it knows
nothing of volumes, chunks or heads. A later change connects it to the Volume
(see [Integration](#integration-with-the-volume)).

Refs hanzoai/vfs#9 (journal and group fsync), #12 (backpressure), #26 (crash
exploration), and the ordering contract of #10 (watermarks).

## The contract

Every record has a sequence number: 1, 2, 3, ... assigned in the order Append
admits them, with no gaps. A record moves through three watermarks.

| Watermark | Means | Reached by |
|---|---|---|
| `Visible` | the record exists; Replay in this run returns it | `Append` returning |
| `Durable` | the record is on the local disk's media, or in the store | `Sync(seq)` returning |
| `Published` | the store holds the record | `Publish(seq)` |

`Published <= Durable <= Visible`, and each only moves forward. `Durable` is the
larger of what a completed disk sync covered and what is published: a record the
store holds needs no local sync to be safe.

| Call | Guarantees |
|---|---|
| `Open(dir, Options)` | Recovers the longest valid prefix of the log (see [Crash outcomes](#crash-outcomes)), makes it durable, discards everything after the first damaged byte, and starts a fresh segment for new records. `Options.Published` is the highest sequence the store holds (the Head's journal sequence, 0 for a store with none). It is the truth: records at or below it are not replayed, numbering continues after it even if the disk lost the tail, and a log that does not begin at or before `Published + 1` is missing the records in between and is discarded. |
| `Append(ctx, p) (seq, err)` | The record is **visible** when it returns, in order, with `seq = previous + 1`. It is **not durable**. Near capacity it slows; at capacity it returns `ErrFull` and changes nothing. A payload larger than a segment gives `ErrTooLarge`. |
| `Sync(ctx, seq)` | Returns `nil` only when record `seq` and every record before it are durable. Concurrent calls share one fsync. A call is released only by an fsync that **began after** its record was written; an fsync already in flight never releases it. After a failed write or sync it returns the error for anything not yet durable, forever. |
| `Marks()` | The three watermarks. |
| `Publish(seq)` | Records that the store holds `seq` and everything before it. Monotonic (a lower value is ignored); `seq` must have been appended. Releases `Sync` calls it satisfies and moves the `Due` deadline to the next unpublished record. Does no I/O. |
| `Replay(fn)` | Calls `fn` for every record after `Published`, in order, with no gaps, then returns. It changes nothing: replaying twice yields the same records. Records of the current run are included. It blocks `Reclaim` while it runs. |
| `Reclaim()` | Makes the space of fully published segments available again. Does file I/O, so the publisher calls it after `Publish`, off the path of `Append` and `Sync`. Never touches an unpublished record or the tail segment. |
| `State()` | `Backlog` (bytes acknowledged and unpublished), `Age` (how long the oldest unpublished record has been acknowledged; later writes never reset it), `Degraded`, and the `Err` that stopped the journal, if any. |
| `Due(age)` | A channel closed when the oldest unpublished record has been acknowledged for `age`. One timer set for the exact deadline; no polling. See below. |
| `Close()` | Makes everything appended durable, then stops. |

### Generation ordering

The rule that a `Sync(N)` is never satisfied by a sync that started before record
`N` was written is what an fsync caller relies on. The syncer takes a snapshot of
"written so far" and the list of files holding it, syncs those files, and only
then releases the waiters at or below the snapshot. Waiters that arrive during a
sync form the next group, so the group size follows the load with no timer. The
pipeline has a writer goroutine and a syncer goroutine: the next batch is written
while the previous one is being synced.

### The 5 s bound is per record

`Due(age)` fires when the oldest unpublished record, measured from **its own**
acknowledgement, is `age` old. It is never reset by later writes. When `Publish`
retires the oldest record the deadline moves to the next one; with nothing
unpublished there is no timer, and the deadline starts with the next `Append`.
The publisher loop is

```go
for {
    <-j.Due(bound - margin)        // or earlier: dirty bytes, Flush, Detach
    seq := <the generation the publisher is about to commit>
    commit the Head containing seq
    j.Publish(seq)
    j.Reclaim()
}
```

Acknowledgement times come from the injected `Clock`, so tests drive the bound
with a clock they control. Times are kept to a millisecond: records acknowledged
within a millisecond of each other share a deadline, which can make `Due` fire up
to a millisecond early, never late.

### Backpressure and the degraded state

The journal has a fixed `Capacity`, an explicit parameter set from the node's
NVMe budget. Disk use never exceeds it.

* Below 75 % full, `Append` is not slowed.
* From 75 % to 100 %, `Append` is held to a rate that falls linearly from
  unrestricted to 64 MiB/s, so the writers settle where their rate matches the
  drain. The delay is a timer wait; it ends early if the caller's context does.
* At 100 % `Append` returns `ErrFull`. Nothing was appended and nothing is
  dropped. The caller maps it to `ENOSPC`/`EAGAIN`.
* Recovery drains in order: once the publisher publishes and reclaims, `Append`
  succeeds again and `Replay` returns the old records first.

`State().Degraded` is true when the publication bound cannot be trusted: the
oldest unpublished record is older than `Options.Bound` (default 5 s), the
journal is slowing appends, or it has failed. During an object-store outage the
journal keeps accepting writes within its capacity, `Degraded` is true, and
`Age` and `Backlog` say how far behind it is.

## Crash outcomes

| What happens | What `Replay` returns after the next `Open` |
|---|---|
| Process crash (SIGKILL), disk and OS intact | Every record the writer had written to the file, which includes every record whose `Sync` returned. Records still in memory, appended but not yet written, are lost; none of them was durable. |
| OS crash or power loss | A **prefix** of the appended records that contains every record whose `Sync` returned. Which unsynced records survive is up to the disk; they survive as a prefix, never with a gap. |
| Torn write (partial sector) | The valid prefix before the damaged record. The damaged record and everything after it are discarded, for good. |
| Permanent disk loss | Nothing. The unpublished tail is lost. The journal cannot know what it lost; the caller knows the last published sequence and the acknowledgement time it had recorded, and must report that an unpublished tail may be missing. A takeover must not report those writes as durable. |
| Published ahead of the disk | The store holds records the disk lost (the publisher published records that had not been synced). `Open` with `Options.Published` set to the store's sequence continues numbering after it and replays nothing it already holds. |

Replay is idempotent: it is a read, and a record is identified by its sequence.
A consumer that applies records must tolerate seeing a record again after a crash
that happened between applying it and publishing it.

### Recovery

A segment file is *live* if its header checksum is valid. Live segments are ordered
by the sequence of their first record and kept while each begins exactly where the
one before ended and holds at least one valid record. The valid records of a
segment are the run, from its first, in which each record's checksum (seeded by
the segment's life) is valid and its sequence is the previous plus one. The first
byte that fails ends the log: after a torn or lost write nothing past it can be
trusted, and nothing past it was ever reported durable.

The segments after the end of the log are wiped (their header zeroed) and the wipe
is made durable **before** any new record is written. New records always go to a
fresh segment, never behind a torn tail: otherwise a record that reappeared from
the lost tail, carrying the sequence the log expects next, would be taken for a
new one. A segment file that is reused gets a new random *life*, which seeds the
checksum of every record in it, so records of its earlier uses never validate.

`Reclaim` wipes a segment's header durably, oldest first, one at a time: a crash
may lose the newest wipes but never an older one while keeping a newer, which would
leave a gap.

The store's sequence is what ties the log to the object store. If every segment
has been reclaimed and the machine then loses the newest unsynced records,
`Open(Published: h)` numbers on from `h`. If the store is *behind* the log's first
record (a rolled-back Head, or a start of the log lost in a power cut), the
records between are missing; the log is discarded rather than replayed across the
gap.

## How it writes

How should a write be made durable? `bench -what strategy` times the options on a
file of 1 GiB: `pwrite` then `fdatasync`, `O_DSYNC`, `O_DIRECT` with `fdatasync`,
`O_DIRECT|O_DSYNC`, each on a file whose every byte is written and on one that was
only `fallocate`d. Median of the p50 latency of five interleaved 1.2 s runs, ext4,
shared machines (load average beside each table):

NVMe, load average 39 (a shared development node; the numbers carry that noise):

| Strategy | 4 KiB | 64 KiB | 1 MiB |
|---|---|---|---|
| buffered write + `fdatasync`, fully written file | 1.09 ms | 1.10 ms | 2.37 ms |
| buffered write + `fdatasync`, `fallocate`d file | 1.41 ms | 1.46 ms | 4.59 ms |
| `O_DSYNC`, fully written | 1.08 ms | 1.07 ms | 2.62 ms |
| `O_DIRECT` + `fdatasync`, fully written | 1.05 ms | 1.03 ms | 2.27 ms |
| `O_DIRECT` with `O_DSYNC`, fully written | 1.66 ms | 0.61 ms | 3.39 ms |

SATA SSD, load average 11:

| Strategy | 4 KiB | 64 KiB | 1 MiB |
|---|---|---|---|
| buffered write + `fdatasync`, fully written file | 0.78 ms | 0.98 ms | 12.8 ms |
| buffered write + `fdatasync`, `fallocate`d file | 0.81 ms | 1.01 ms | 12.5 ms |
| `O_DSYNC`, fully written | 0.77 ms | 0.99 ms | 12.5 ms |
| `O_DIRECT` + `fdatasync`, fully written | 0.79 ms | 0.93 ms | 11.7 ms |
| `O_DIRECT` with `O_DSYNC`, fully written | 0.77 ms | 0.95 ms | 11.7 ms |

What decides the design:

1. **Overwriting blocks that are already written is as cheap as anything and
   sometimes much cheaper than writing into `fallocate`d space** (1.3 times at
   4 KiB and 64 KiB, 1.9 times at 1 MiB on the NVMe; no difference on the SATA SSD).
   An `fdatasync` into unwritten extents must commit the extent conversion through the
   filesystem journal; an overwrite needs only the data and one device flush. So
   segments are *fully written* files, and they are **recycled** instead of deleted and
   recreated: writing zeros again for every segment would double the write traffic.
   A new file is written once, off the append path and ahead of need; after that the
   same files are overwritten for good.
2. **`O_DIRECT` and `O_DSYNC` are not consistently better than buffered writes plus
   one `fdatasync`.** `O_DIRECT|O_DSYNC` wins at 64 KiB and loses at 4 KiB; `O_DIRECT`
   with `fdatasync` is within noise of the buffered path. They cost alignment and padding
   in the record format and cannot share a page cache. The journal uses buffered
   `pwrite` and `fdatasync`; the group-sync queue makes one `fdatasync` serve every
   waiter.

Two more rules keep the cost of a sync bounded:

* Data is handed to the system for writeback (`sync_file_range`, a hint) as soon as
  1 MiB has been written to a segment, so a later `fdatasync` finds it already on its
  way. Without the hint, two bulk writers beside eight fsync writers raised the fsync
  writers' median from 1.6 ms to 37 ms; with it, to 6.2 ms (the plain loop: 4.9 ms).
* An `fdatasync` is started every 8 MiB with nobody waiting, and the writer waits
  when 16 MiB are written and not yet synced. A sync is therefore never asked to
  flush more than a few megabytes, and the next `Sync` call never waits behind
  gigabytes.

## Measurements

`bench` (in `journal/bench`) runs the journal and a plain loop on the same disk, same
sizes, same concurrency. Plain means each writer does `pwrite` then `fdatasync` on its
own fully written, preallocated file (a variant with one shared file is in the
output and is no better). The journal runs with a publisher that publishes and
reclaims as fast as `Due(5 ms)` says. A journal run first cycles every segment once, so
the measured run finds recycled files. The fdatasync count is taken **inside the file
layer**, per 1,000 `Sync` calls; it includes the syncs of `Reclaim` and the background
syncs, which is why one writer shows 915 to 983 and not 1,000.

Both machines were shared with other work, so every line carries its load average and
single runs vary a great deal (on the NVMe node, one sequential run measured at load 91
gave 126 MB/s, the same run at load 17 gave 4,162 MB/s). Only comparisons made
back to back mean anything.

### 4 KiB Append + Sync, 5 s per run

NVMe node (arm64, 20 cores, PCIe NVMe, ext4). Rows 1, 8 and 32 ran at load 6 to 7;
64 and 128 at 26 to 50.

| Writers | Journal ops/s | Journal p50 | Journal fsyncs per 1,000 Sync | Plain ops/s | Plain p50 | Journal p50 / plain p50 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 279 | 1.44 ms | 915 | 232 | 1.16 ms | 1.25 |
| 8 | 2,044 | 1.55 ms | 125 | 1,133 | 4.31 ms | 0.36 |
| 32 | 7,676 | 1.67 ms | 32 | 1,884 | 8.52 ms | 0.20 |
| 64 | 14,035 | 1.73 ms | 17 | 2,829 | 12.1 ms | 0.14 |
| 128 | 23,258 | 2.14 ms | 8.8 | 3,559 | 17.9 ms | 0.12 |

An earlier full matrix on the same node (load 33 to 40) gave 389, 3,121, 8,442, 18,819
and 39,653 ops/s for the journal against 403, 1,209, 1,397, 3,521 and 6,742 for plain.
At 128 writers the journal released about 128 waiters per `fdatasync`; the group-size
histogram is in the raw output. The p99 of both is 40 to 60 ms at that load: a periodic
stall of the shared disk that the plain loop shows too.

SATA SSD node (amd64, 56 threads, ext4 on LVM; load 11 to 30):

| Writers | Journal ops/s | Journal p50 | Journal fsyncs per 1,000 Sync | Plain ops/s | Plain p50 | Journal p50 / plain p50 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 905 | 0.82 ms | 946 | 1,080 | 0.77 ms | 1.06 |
| 8 | 5,374 | 1.02 ms | 143 | 1,156 | 5.64 ms | 0.18 |
| 32 | 10,262 | 2.62 ms | 32 | 1,175 | 26.3 ms | 0.10 |
| 64 | 13,939 | 5.29 ms | 13 | 1,083 | 55.1 ms | 0.10 |
| 128 | 18,599 | 5.56 ms | 0.7 | 1,193 | 102 ms | 0.05 |

tmpfs, where `fdatasync` costs nothing (so this is the pipeline's own cost): one writer
3.6 us p50 against 1.0 us plain, about 190,000 Append+Sync per second; 238,000 to
282,000 per second with 8 to 128 writers.

**Target: 4 KiB Sync latency at most 1.5 times plain `fdatasync` at matched load.**
Met on both disks at one writer (1.25 and 1.06 times), and from eight writers up the
journal is 3 to 20 times faster, because one `fdatasync` serves the whole group. The
0.2 to 0.3 ms the journal adds at one writer is the hops between the appender, the
writer, the syncer and back (CPU profile: 10 % busy, nothing else on the path).

### 1 MiB sequential Append

Back-to-back comparisons on the NVMe node, 4 GiB per run, one writer, journal
capacity 2 GiB with the publisher publishing instantly, against a plain loop of 1 MiB
`pwrite` calls with `fdatasync` every 8 MiB (the same sync granularity the journal
uses). Durable MB/s, three rounds, load 15 to 25:

| Round | Journal | Plain, sync every 8 MiB | Ratio |
|---:|---:|---:|---:|
| 1 | 2,870 | 3,461 | 0.83 |
| 2 | 4,162 | 2,996 | 1.39 |
| 3 (disturbed) | 398 | 405 | 0.98 |

The median is 0.98 of plain. The strict plain loop, `fdatasync` after every 1 MiB, ran at
280 to 340 MB/s. Without the writeback hint the journal ran at 2,348, 3,610 and 301.
A journal run of 1.5 GiB, which fits the capacity (the burst), appended and was durable
at 1,905 to 2,575 MB/s when the node was not saturated; at load 91 it measured 126 MB/s.
tmpfs: 3,000 to 4,600 MB/s against 5,000 to 7,600 plain, the cost of the extra copy
and the checksum.

SATA SSD node: the disk sustains about 75 MB/s here and the journal is at its limit.
Sustained, three runs: 73.5, 68.6 and 73.1 MB/s (median 73.1), and 69.5, 82.7 and
69.1 with four writers; the plain loop syncing every 8 MiB ran at 77.2, 77.7 and
69.6 (median 77.2), every 32 MiB at 74.7, 77.5 and 71.0, and after every MiB at 75.7,
93.2 and 65.3. The 1.5 GiB burst measured 87, 51 and 48 MB/s, bound by the disk because
the final `Sync` waits for it.

**Target: sequential at least 90 % of plain NVMe write throughput.** Met at the
median of the back-to-back runs (0.98) and on the SATA disk (0.95 against the plain loop that syncs every 8 MiB); it ranged from
0.83 to 1.39 across the three rounds on the shared node. The burst figure of 1.1 GB/s
needs a disk that is not shared with a dozen compile jobs: it was exceeded at 1.9 to
2.6 GB/s when the node was quiet.

### Where the gap is

* **Sequential**: CPU profile of a 4 GiB run, 50 % of one core: `pwrite` 40 %,
  `fdatasync` 22 %, CRC-32C of the payload 17 %, `memmove` into the batch buffer 14 %.
  The journal costs one extra copy and one checksum over the plain loop; on a disk that
  keeps up that is the 0.83 of round 1, and on tmpfs the 0.6 to 0.8.
* **Small syncs**: at 32 writers 5.9 s of mutex wait in a 6 s run, 90 % of it in `Sync`
  registering a waiter while the syncer releases a group, about 100 us a call. Splitting
  the waiter list from the append lock is the next step; at the measured latencies it is
  5 % of the p50.
* **A bulk writer beside fsync writers**: one ordered log means a `Sync` covers all the
  bulk data before it. With two bulk writers beside eight fsync writers the journal's
  p50 was 6.2 ms against 4.9 ms for the plain loop on the NVMe node (1.3 times), and
  33.7 ms against 5.6 ms on the SATA SSD, where 16 MiB takes 200 ms to flush. The
  bounds above (writeback hints, 8 MiB syncs, 16 MiB exposed) keep it from being worse;
  keeping bulk chunk data out of the journal is the cure and belongs to the Volume.

## File layout

A directory of segment files `NNNNNN.seg`, all of one size (`Options.Segment`,
default 64 MiB). Integers are little endian; checksums are CRC-32C (Castagnoli).

```
segment  header | record | record | ... | (unused)
header   checksum u32 | first u64 | life u64                    20 bytes
record   checksum u32 | length u32 | sequence u64 | acknowledged i64 | payload checksum u32 | payload
```

* The header checksum covers `first` (the sequence of the segment's first record,
  at least 1) and `life` (random, new each time the file is reused). A zero header
  is a free segment.
* The record checksum covers the record header after itself and is seeded by the
  segment's life. The payload checksum is computed before the journal's lock is
  taken; the record checksum binds it to the sequence and the time.
* `acknowledged` is the wall-clock time of the `Append`, in nanoseconds since the
  Unix epoch. It survives a restart, so the age of the oldest unpublished record
  does too.
* Records do not span segments. Many records are written by one `pwrite` (group
  commit); the first write into a segment starts at its header.

## The file layer

The journal writes only through the `Disk` and `File` interfaces. The operating
system's files are the default. `journal/crash` is a `Disk` in memory that logs every
write, sync, create and remove, and rebuilds the disk a crash would leave after any
prefix of that log: nothing persisted, everything persisted, each prefix, every
subset of a few writes, torn writes inside and across sectors, random scatters. `crash.Disk.Explore` checks every such
state, in the manner of CrashMonkey and ACE. It is the harness for issue #26 and is
meant to be reused by the Volume's tests.

## Integration with the Volume

This change does not touch the Volume. The records are opaque bytes: the Volume
chooses the encoding of a mutation, the journal only orders, stores and numbers it.
The surface the integration change calls:

```go
j, err := journal.Open(dir, journal.Options{Capacity: budget, Published: head})
err = j.Replay(func(r journal.Record) error { ... })   // at mount, before serving
seq, err := j.Append(ctx, mutation)                    // before applying it
err = j.Sync(ctx, seq)                                 // fsync
<-j.Due(bound - margin)                                // the publisher's deadline
err = j.Publish(g); err = j.Reclaim()                  // after the Head carrying g is committed
st := j.State(); m := j.Marks()                        // what an agent reads
err = j.Close()                                        // Detach, suspend, end
```

The integration change does the following.

1. **Mount.** Read the Head's journal sequence `h` (the last sequence committed
   into the Head). `journal.Open(dir, Options{Capacity: <NVMe budget>, Published: h})`.
   `Replay` and apply each record to the Volume's in-memory state, in order; then
   start the publisher. The Head is the truth: pass it every time, including 0 for a
   Disk that has never published.
2. **Mutation.** Every Volume mutation (write, truncate, rename, create, unlink,
   setattr) is encoded as one record and `Append`ed before it is applied. A write's
   journal sequence is the Volume's write sequence.
3. **fsync.** `Sync(ctx, seq)` with the sequence of the last record the file
   depends on. A local fsync is then durable in the node's journal, not in the
   store.
4. **Publisher.** One serialized publisher per Volume. It takes an immutable
   generation under a short lock (everything up to sequence `g`), releases the
   lock, builds and uploads the objects, commits the Head carrying `g`, and calls
   `Publish(g)` then `Reclaim()`. It loops on `Due(bound - margin)` and also acts
   early on dirty bytes, an explicit Flush, Detach, suspend or end. A fsync waits
   for the journal, never for a generation.
5. **Bound.** The 5 s bound is `Due`'s argument. `State()` is what the caller or an
   agent reads to learn that it cannot be trusted. `ErrFull` is `ENOSPC`/`EAGAIN`.
6. **Detach, suspend, end.** Force the kernel to flush its dirty pages for the
   mount first (the journal sees only what reaches the daemon), then publish
   everything, then `Close`.
7. **Takeover.** The new owner opens its own journal with `Published` set to the
   Head's sequence. The old owner's unpublished tail is on its disk and is not
   visible to the new owner; if that disk is gone it is lost, and the takeover
   must say so rather than report those writes durable.

The trap to test in that change: with a writeback page cache the kernel can
acknowledge a `write(2)` before the daemon sees it, so the 5 s bound is measured
from the record's entry into the journal, and `fsync` or `syncfs` are the only
durability promises for buffered writes.

## Specification notes

This section is normative. The words MUST, MUST NOT and MAY are used as in RFC 2119.
A conforming implementation need not be this package; each criterion can be tested
from outside, with a file layer that can hold, fail and crash.

### Model

A *journal* is a sequence of *records*. A record has a *payload* of 0 to `S - 48`
bytes (`S` is the segment size), a *sequence* and an *acknowledgement time*. The
journal has a fixed *capacity* of at least two segments. It is used by one process
at a time. The caller supplies a clock and a file layer; the file layer has
`create` (file of a given size, every byte written, durable on return), `open`,
`remove` (durable on return), `list`, and per file `read`, `write`, `sync` and
`size`. A write is volatile until a `sync` that began after it completes; a crash may
leave any subset of the volatile writes, each possibly torn at any byte.

### Requirements

1. **Sequences.** Sequences are positive integers. The journal MUST assign
   `previous + 1` to each appended record, in the order the appends are admitted,
   and MUST NOT return a sequence twice in one history of a store. After an `open`
   that is told the store holds sequence `h`, the next sequence MUST be greater
   than `h` and greater than every sequence the journal recovers.
2. **Watermarks.** The journal MUST maintain `published <= durable <= visible`,
   each non-decreasing. `visible` is the highest appended sequence. `published` is the
   highest sequence passed to `publish` (monotonic; a lower value is ignored).
   `durable` is the larger of the highest sequence covered by a completed sync and
   `published`.
3. **Append.** On return the record MUST be visible: a replay in the same run
   includes it. A record is not durable on return. When the journal cannot accept the
   record because its capacity is used by unpublished records, `append` MUST fail
   with a distinguishable "full" error and MUST leave all state unchanged. A payload
   that cannot fit in a segment MUST be rejected with a distinguishable error.
4. **Sync.** `sync(s)` MUST return success only when record `s` and every record
   before it are durable, and MUST NOT be satisfied by a sync that began before
   record `s` was written. Concurrent `sync` calls MUST share syncs: calls that
   arrive while a sync is in flight are released by the next one. After a failed
   write or sync, `sync(s)` MUST fail for every `s` not already durable, and every
   later `append` MUST fail; the journal MUST NOT retry. `sync` of a sequence not
   yet appended MUST fail with a distinguishable error.
5. **Publish.** `publish(s)` requires `s <= visible`. It does no file I/O. It MUST
   release `sync` calls for sequences up to `s`.
6. **Replay.** `replay` MUST yield, in order and without gaps, every record with a
   sequence greater than `published`, up to `visible`, each with the payload and
   acknowledgement time it was appended with. It MUST NOT change the journal: two
   replays with no operation between them yield the same records. It MUST fail rather
   than skip a record that should be present.
7. **Reclaim.** `reclaim` MUST NOT make any record with a sequence above `published`
   unavailable, and MUST NOT free the segment new records are being added to. It MUST
   NOT run concurrently with a `replay` that is reading the segment.
8. **Deadline.** `due(a)` MUST complete when the oldest unpublished record has been
   acknowledged for `a`, measured from that record's own acknowledgement time. Later
   appends MUST NOT move the deadline. When `publish` retires the oldest record, the
   deadline MUST move to the next unpublished record. With nothing unpublished no timer
   MAY be pending; the deadline starts with the next append. The implementation MAY
   fire up to 1 ms early and MUST NOT fire late by more than the clock's resolution.
9. **State.** `state` MUST report the bytes of unpublished records, the age of the
   oldest, and the error that stopped the journal. It MUST report `degraded` when that
   age exceeds the configured bound, when the journal is slowing appends, or when it
   has failed.
10. **Backpressure.** Disk use MUST NOT exceed the capacity. Below 75 % of the
    capacity `append` MUST NOT be delayed. Between 75 % and 100 % it MUST be delayed so
    that the admitted rate falls toward 64 MiB/s as the fill approaches 100 %; a
    delayed append MUST return when its context ends. At 100 % it MUST fail with the
    "full" error. No record is ever dropped. When the publisher publishes and reclaims,
    appends MUST succeed again and replay MUST return the older records first.
11. **Format.** A segment is a file holding a 20-byte header and records back to back.
    The header is `checksum(4) first(8) life(8)`: the CRC-32C of the last 16 bytes,
    the sequence of the segment's first record (at least 1), and a random value fresh
    each time the file is used. A record is `checksum(4) length(4) sequence(8)
    acknowledged(8) payload checksum(4) payload`: the record checksum is the CRC-32C
    of the 24 bytes after it, seeded with a 32-bit value derived from the segment's
    life; the payload checksum is the CRC-32C of the payload; `acknowledged` is Unix
    nanoseconds. Integers are little endian. Records do not span segments. A header
    of zeros marks a free segment.
12. **Recovery.** `open` MUST keep the longest prefix of records in which every
    record is valid under its segment's life and has the sequence one greater than its
    predecessor, across segments ordered by `first` and linked only where `first`
    equals the previous segment's last sequence plus one. It MUST discard everything
    after the first byte that fails, MUST make the discarded segments free durably
    before any new record is written, MUST make the records it keeps durable before it
    returns, and MUST write new records to a segment that holds no kept record.
    `reclaim` MUST free segments oldest first, each durable before the next. A log
    that begins after the store's sequence plus one, or ends at or before the store's
    sequence, MUST be discarded and numbering MUST continue from the store's sequence.
13. **Bounded exposure.** The memory the journal queues for the file layer MUST be
    bounded: when the writer cannot keep up, `append` waits for it. The data written but
    not yet synced SHOULD be bounded too, and the journal SHOULD start syncs without a
    caller waiting and ask the system to write data out as it is written, so a `sync`
    never waits behind an unbounded amount of other callers' data. This implementation
    queues at most 16 MiB, starts a sync every 8 MiB, lets the writer run at most 16 MiB
    ahead of the last completed sync, and requests writeback for every 1 MiB.
14. **Crash outcomes.** After a crash of the process with the files intact, the
    recovered log MUST contain every record whose `sync` returned. After a crash of the
    machine, the recovered log MUST be a prefix of the appended records that contains
    every record whose `sync` returned and that is not published. After loss of the
    disk the journal is empty, the unpublished tail is lost, and the caller MUST NOT
    report those records durable.

### Acceptance criteria

Each criterion is a test that fails if the requirement is removed.

* **A1 order**: 16 goroutines append 150 records each; the returned sequences are
  exactly 1..2400, each once, and a replay returns them in order with their payloads.
* **A2 barrier**: hold the first sync in the file layer, append a record `b`, call
  `sync(b)`, release the sync: `sync(b)` is still blocked; it returns after a second
  sync that began after `b` was written.
* **A3 grouping**: hold a sync, start 32 `sync` callers, release: at most two further
  syncs are issued.
* **A4 crash exploration**: run a mixed workload (append, sync, publish, reclaim,
  several writers) on a file layer that logs writes and syncs. For every prefix of
  the log and for each of: no volatile write kept, all kept, each prefix kept, the last
  volatile writes torn at 1, 100, 512 and 612 bytes, at half the write rounded down to a
  sector, and one byte short, random per-sector subsets: recovery succeeds, told the
  store's sequence; the replay is contiguous after it; its payloads
  equal those appended; it reaches every record whose `sync` returned unless the store
  holds it; a second replay and a second `open` return the same.
* **A5 no resurrection**: in every state of A4, append four new records with different
  payloads and sync them, keep only synced data, recover: the replay is the earlier
  records followed by exactly the four, never a record of the lost tail. A direct case:
  reuse a segment so that the new records end where a stale record with the next
  expected sequence begins; the stale record is not replayed.
* **A6 reclaim window**: append 12 records (9 synced), publish 9 and reclaim; at every
  crash point, with the wipes persisted in any order, recovery with the store at 9
  yields a log that is a prefix, numbers on from 10, and loses no record above 9 that
  was synced.
* **A7 reclaim safety**: while a replay is paused inside its callback, `reclaim` does
  not modify any segment.
* **A8 deadline**: with a controlled clock, append at `t`, `due(5 s)` closes at `t + 5 s`
  and not 1 ms before; ten appends in between do not move it; with records at `t` and
  `t + 3 s`, publishing the first moves the deadline to `t + 8 s`; with nothing
  unpublished no timer is pending, and after 1,000 appends one timer is.
* **A9 bound**: a writer appending every 10 ms and a publisher that publishes when
  `due(5 s)` closes keep `state.age` at or below 5 s for a minute; stopping the
  publisher for 20 s makes `degraded` true with `age` above 20 s and a growing
  `backlog`; publishing everything clears both.
* **A10 backpressure**: with no publisher, appends are delayed before the first
  `full` error and no acknowledged record is lost; after publishing half and
  reclaiming, appends succeed, the next sequence continues, and replay starts after
  the published mark.
* **A11 failure**: after an injected write or sync error, `sync` of a non-durable
  sequence and every `append` fail with that error; `sync` of a durable sequence
  succeeds.
* **A12 damage**: flipping a byte inside record `k` leaves a replay of exactly
  `1..k-1`, and later appends continue at `k`; with the damage in the second of three
  segments the third is discarded and its sequences are reused.
* **A13 store ahead and behind**: after records 1..5 are synced and the journal is opened
  with the store at 9, the marks are 9/9/9, the replay is empty and the next sequence is
  10. With nine records synced, six published and the oldest segments reclaimed, opening
  with the store at 2 discards the log: marks 2/2/2, empty replay, next sequence 3, and
  the discarded records do not return.
* **A14 capacity**: the files never occupy more than the capacity.
* **A15 exposure**: with the first sync held in the file layer, a stream of 1 MiB
  appends is written no further than 28 MiB; with the writer held in its first write,
  at most 20 MiB are admitted. Appending 6 MiB in 1 MiB records requests writeback for
  5 to 7 MiB, and 3 KiB of small records requests none.

## References

* J. Mohan, A. Martinez, S. Ponnapalli, P. Raju, V. Chidambaram. Finding
  Crash-Consistency Bugs with Bounded Black-Box Crash Testing. OSDI 2018 (CrashMonkey
  and ACE).
* Wang et al. NVLog. FAST 2025,
  <https://www.usenix.org/conference/fast25/presentation/wang> (byte-granularity
  records, absorbing small synchronous writes, systematic crash testing).
