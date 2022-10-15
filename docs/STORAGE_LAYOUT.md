# Vortex On-Disk Storage Engine and Indexing Layout

This document specifies the physical disk representation, indexing strategies, and compaction semantics of the Vortex storage engine.

---

## 1. Directory Hierarchy

Partition commit logs are organized hierarchically on the local filesystem:

```
/var/vortex/data/
  +-- orders-0/
  |     +-- 00000000000000000000.log
  |     +-- 00000000000000000000.index
  |     +-- 00000000000000000000.timeindex
  |     +-- 00000000000000050000.log
  |     +-- 00000000000000050000.index
  |     +-- 00000000000000050000.timeindex
  |     +-- leader-epoch-checkpoint
  +-- orders-1/
  +-- __consumer_offsets-0/
```

- Segment files are named using their 20-digit zero-padded base offset (`00000000000000000000.log`).
- The base offset represents the first logical offset stored within that segment.

---

## 2. RecordBatch Binary Envelope Layout

The physical `.log` file is an append-only stream of `RecordBatch` structures:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Base Offset                          |
|                           (64-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Batch Length                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    Partition Leader Epoch                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (byte)  |                    CRC32C                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Attributes (int16)        |       Last Offset Delta       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       First Timestamp                         |
|                           (64-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Max Timestamp                          |
|                           (64-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Producer ID                           |
|                           (64-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Producer Epoch (int16)    |        Base Sequence          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Records Count                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Record Payload                        |
|                               ...                             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Record Encoding (Delta Encoded):
Each individual record within the batch stores deltas relative to the batch header to minimize disk footprint:
1. `Offset Delta` (int32): `rec.Offset - BaseOffset`
2. `Timestamp Delta` (int64): `rec.Timestamp - FirstTimestamp`
3. `Key Length` (int32) followed by `Key Bytes` (or 0 for null keys)
4. `Value Length` (int32) followed by `Value Bytes` (0 length represents a compaction tombstone)
5. `Headers Count` (int32) followed by key-value header tuples

---

## 3. Sparse Offset Index (`.index`)

To achieve rapid `O(log N)` point lookups without maintaining an in-memory index for every single record, Vortex writes an index entry only every `IndexIntervalBytes` (default: 4,096 bytes).

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Relative Offset                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Physical Position                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `Relative Offset` (int32): `TargetOffset - BaseOffset`. Fits within 4 bytes, supporting up to 2 billion records per individual segment.
- `Physical Position` (int32): Exact byte offset in the `.log` file where the corresponding `RecordBatch` begins.
- Binary Search Lookup:
  1. Binary search the memory-mapped `.index` buffer to locate the highest indexed offset `<= TargetOffset`.
  2. Seek the `.log` file directly to the indexed physical position.
  3. Sequentially scan only the small intervening window (maximum 4KB) to locate the exact record.

---

## 4. Time Index (`.timeindex`)

Maps millisecond timestamps to relative offsets:
```
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Timestamp                            |
|                           (64-bit)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Relative Offset                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```
Used for timestamp-based offsets lookups (`offsets_for_times`) and time-based retention pruning.

---

## 5. Log Compaction and SkimpyOffsetMap

For compacted topics (such as `__consumer_offsets`), Vortex runs an asynchronous background cleaner:
1. Phase 1 (Hash Index Build): The cleaner scans the dirty portion of the log, populating an in-memory `SkimpyOffsetMap` with 64-bit key hashes mapped to their highest observed offset.
2. Phase 2 (Clean Copy): The cleaner streams older segments, keeping only records whose offset equals the latest offset in the map. Older revisions are dropped.
3. Tombstones: Zero-length value records serve as deletion tombstones, which are retained until `DeleteRetentionMs` expires to ensure downstream consumers process the deletion.
