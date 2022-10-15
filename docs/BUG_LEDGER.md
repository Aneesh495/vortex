# Vortex Engineering Bug Ledger & Postmortem Analysis

This ledger records all functional defects identified during development, detailing the symptom, root cause analysis, reproduction test case, and verified fix.

---

### Bug 001: CRC32C Inversion on Empty Spans
- **Component**: `Vortex.Native.Crc32C`
- **Symptom**: Calling `Crc32C.Compute(ReadOnlySpan<byte>.Empty)` returned `0xFFFFFFFF` instead of standard `0`.
- **Root Cause**: The method `Compute(uint seed, ReadOnlySpan<byte> data)` performed `seed ^ 0xFFFFFFFF` at start and finish. When input data was empty, it inverted the default zero seed twice, resulting in `~0U` (`0xFFFFFFFF`).
- **Reproduction Test**: `tests/Vortex.Tests/NativeTests.cs: Crc32C_EmptySpan_ReturnsZero`
- **Permanent Fix**: Added early-exit check `if (data.IsEmpty) return seed;` prior to running final bitwise inversion.

---

### Bug 002: Big-Endian Int64 Sign Extension in High Values
- **Component**: `Vortex.Core.BinaryCodecs`
- **Symptom**: `ReadInt64BE` returned negative 64-bit longs when decoding positive numbers with the highest bit of the upper 32-bit word set.
- **Root Cause**: In G#, `(int64)ReadInt32BE(stream)` performed signed arithmetic sign-extension on 32-bit values before shifting left by 32 bits.
- **Reproduction Test**: `tests/Vortex.Tests/CoreTests.cs: BigEndianPrimitives_RoundTripCorrectly`
- **Permanent Fix**: Explicitly cast the high 32-bit read to `uint32` before widening to `int64`: `(int64(uint32(ReadInt32BE(stream))) << 32) | int64(uint32(ReadInt32BE(stream)))`.

---

### Bug 003: InSyncReplicas State Map KeyNotFoundException
- **Component**: `Vortex.Replication.InSyncReplicas`
- **Symptom**: Broker threw `KeyNotFoundException` during leader re-elections when evaluating ISR membership.
- **Root Cause**: Indexer access `Replicas[memberId]` assumed all cluster members were already in the dictionary. When a replica had not yet published its first fetch heartbeat, direct dictionary indexing threw an unhandled exception.
- **Reproduction Test**: `tests/Vortex.Tests/ReplicationTests.cs: InSyncReplicas_TracksLagsAndFailover`
- **Permanent Fix**: Switched all lookups to `Replicas.TryGetValue(id, out state)` and defaulted unrecorded nodes to replica lag 0.

---

### Bug 004: Record.Size() NullReferenceException on Uninitialized Headers
- **Component**: `Vortex.Core.Record`
- **Symptom**: Calling `rec.Size()` threw `NullReferenceException` when producing records without explicit header collections.
- **Root Cause**: The field `var Headers List[RecordHeader]` was declared without an initializer in `Record.gs`, leaving it null when constructed via parameterless constructors.
- **Reproduction Test**: `tests/Vortex.Tests/ClientTests.cs: VortexProducer_SendAndDrainBatches`
- **Permanent Fix**: Initialized `Headers` with `var Headers List[RecordHeader] = List[RecordHeader]()` and guarded the summation loop with `if (Headers != nil)`.

---

### Bug 005: VortexConsumer Poll Topic-Partition Key Mismatch
- **Component**: `Vortex.Client.VortexConsumer`
- **Symptom**: `VortexConsumer.Position(tp)` returned 0 after polling records, despite records being processed.
- **Root Cause**: `Poll()` formatted tracking keys using colon (`topic:partition`), whereas `Assign()`, `Position()`, `Seek()`, and `CommitSync()` used `TopicPartition.Format()` which formats with hyphen (`topic-partition`).
- **Reproduction Test**: `tests/Vortex.Tests/ClientTests.cs: VortexConsumer_AssignAndPoll_UpdatesPositions`
- **Permanent Fix**: Standardized `Poll()` to format partition keys using `rec.Topic + "-" + rec.Partition.ToString()`, maintaining consistency with `TopicPartition.Format()`.

---

### Bug 006: PartitionLog HighWatermark Un-Advanced on Append
- **Component**: `Vortex.Storage.PartitionLog`
- **Symptom**: `PartitionLog.Read(offset, maxBytes, highWatermark)` returned zero batches after successful append operations.
- **Root Cause**: In `PartitionLog.Append()`, `NextLogEndOffset` was updated to `baseOffset + records.Count`, but `HighWatermark` was left at initial value 0. Because reads are bounded by `maxOffset = HighWatermark`, all fetch attempts were rejected as past high watermark.
- **Reproduction Test**: `tests/Vortex.Tests/EndToEndTests.cs: Broker_MultiPartitionProduceAndFetch`
- **Permanent Fix**: Updated `HighWatermark = NextLogEndOffset` inside `PartitionLog.Append()` upon successful batch persistence.
