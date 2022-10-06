# Vortex Architecture Specification

Vortex is a distributed, high-throughput event streaming engine and stream processing runtime designed for ultra-low latency commit log operations. Built with G# (Gsharp) as its primary language and native SIMD hardware intrinsics for cryptographic data integrity, Vortex delivers sub-millisecond end-to-end append latencies and million-plus message-per-second ingestion rates on commodity hardware.

---

## 1. System Overview

```
                      +---------------------------------------+
                      |       Vortex Client Application       |
                      |   (VortexProducer / VortexConsumer)   |
                      +-------------------+-------------------+
                                          |  Binary Wire Protocol
                                          v  (TCP / Framing / CRC32C)
+-----------------------------------------+-----------------------------------------+
|                              Vortex Broker Node                                   |
|                                                                                   |
|   +-----------------------+   +-----------------------+   +-------------------+   |
|   | Socket Server Pipeline|   |  Raft Consensus Core  |   | Group Coordinator |   |
|   | (Channels / RingBuf)  |   | (Terms / Heartbeats)  |   | (Sticky Assignor) |   |
|   +-----------+-----------+   +-----------+-----------+   +---------+---------+   |
|               |                           |                         |             |
|               +-------------------+-------+                         |             |
|                                   |                                 v             |
|                                   v                       +-------------------+   |
|                       +-----------------------+           | __consumer_offsets|   |
|                       |   PartitionLog Engine |           | (Compacted Log)   |   |
|                       +-----------+-----------+           +-------------------+   |
|                                   |                                               |
|         +-------------------------+-------------------------+                     |
|         v                                                   v                     |
|   +-------------------+                               +-------------------+       |
|   |   Active Segment  |                               |   Sealed Segments |       |
|   | (.log / .index)   |                               | (Immutable Mmap)  |       |
|   +-------------------+                               +-------------------+       |
+-----------------------------------------------------------------------------------+
```

---

## 2. Core Architectural Pillars

### 2.1 Hardware-Accelerated SIMD and Cryptographic Validation
Network framing and disk storage envelopes are protected by 32-bit Castagnoli CRC (CRC32C) polynomials (`0x82F63B78`). Rather than relying on slow software bit-shifting tables, Vortex leverages CPU hardware intrinsics (`ARM64 CRC32` and `x86-64 SSE4.2`) with fallbacks to vector SIMD algorithms (`System.Numerics.Vector<byte>`). This allows zero-cost verification of gigabytes of streaming data per second.

### 2.2 Segmented Append-Only Log Storage
Data is partitioned into sequential, immutable append-only log segments. Each segment consists of:
- Physical Log File (`.log`): Raw binary sequence of structured `RecordBatch` envelopes.
- Sparse Offset Index (`.index`): Fixed-size 8-byte entries (relative offset to physical byte position) maintained every 4KB of appended data, enabling logarithmic `O(log N)` binary search queries.
- Time Index (`.timeindex`): Maps epoch timestamps to relative offset positions for time-travel queries and retention management.

### 2.3 Lock-Free CSP Concurrency and Pipelines
Vortex coordinates concurrent operations using Communicating Sequential Processes (CSP) channels and bounded ring buffers. Inter-thread communication between socket decoders, log appenders, and replication loops avoids kernel lock contention by utilizing single-producer single-consumer and multi-producer single-consumer lock-free ring buffers.

### 2.4 Raft and In-Sync Replicas (ISR)
High availability and fault tolerance are governed by two distinct consensus mechanisms:
- Raft Consensus Coordinator: Manages cluster metadata, epoch transitions, and membership state.
- In-Sync Replicas (ISR): Drives partition-level replication. Only replicas that acknowledge batches within the configurable lag timeout are kept in the ISR, guaranteeing zero data loss (`acks=all`) across broker failovers.

### 2.5 Cooperative Sticky Rebalancing
Consumer groups utilize a cooperative sticky partition assignment strategy. Unlike legacy eager stop-the-world rebalance protocols, cooperative rebalancing allows non-revoked partitions to continue consuming during cluster membership changes, eliminating pipeline stalls during rolling restarts.

### 2.6 Native Stream Processing Engine
Vortex embeds an asynchronous stream processing engine supporting:
- Event-time processing with bounded out-of-orderness watermark generation.
- Stateless transformations: `Map`, `Filter`, `FlatMap`, and `Branch`.
- Stateful windowing operators: Tumbling windows, sliding windows, and session windows with late event detection.
