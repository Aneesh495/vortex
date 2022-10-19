# Vortex

[![Build and Test](https://img.shields.io/badge/build-passing-brightgreen.svg)]()
[![Tests](https://img.shields.io/badge/tests-94%20passed-success.svg)]()
[![Platform](https://img.shields.io/badge/runtime-.NET%2010%20%7C%20G%23-blue.svg)]()
[![License](https://img.shields.io/badge/license-MIT-purple.svg)]()

Vortex is a distributed, high-throughput event streaming platform and stream processing runtime engineered for sub-millisecond append latency and linear horizontal scalability. Built with **G# (Gsharp)** as its primary language and accelerated by SIMD hardware intrinsics, Vortex combines an append-only commit log, memory-mapped sparse indexing, Raft consensus, and a CSP channel-based streaming runtime.

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

## Key Highlights

- **Hardware-Accelerated SIMD Integrity**: Castagnoli CRC32C (`0x82F63B78`) execution utilizes direct CPU intrinsics (`ARM64 CRC32` and `x86-64 SSE4.2`) with vectorized SIMD fallbacks, hashing gigabytes per second with near-zero CPU overhead.
- **Durable Append-Only Storage Engine**: Zero-copy write paths with sequential segment files, 4KB sparse relative offset indexing, and logarithmic `O(log N)` binary search seeking.
- **Log Cleaner and Compaction**: Two-phase deduplication engine using an in-memory `SkimpyOffsetMap` hash index for tombstone deletion and key-based log retention.
- **In-Sync Replicas (ISR) and Raft Consensus**: Automatic failover, leader elections, and monotonic epoch tracking guaranteeing zero data loss (`acks=all`).
- **Cooperative Sticky Rebalance**: Zero-downtime consumer group coordination that avoids global pipeline stalls during member churn.
- **CSP Channel-Based Stream Processing**: In-memory Directed Acyclic Graph (DAG) streaming engine with event-time watermarking, tumbling, sliding, and session windows.
- **Schema Registry and Evolution**: Built-in dynamic binary schema codecs enforcing `Backward`, `Forward`, and `Full` contract compatibility.
- **Interactive Telemetry Dashboard**: Embedded zero-dependency SVG web console and Prometheus exposition endpoint (`/metrics`) running on port `9100`.
- **Multi-Language SDKs**: Native G# client and Python SDK (`sdk/vortex-py`) with binary protocol encoders.

---

## Engine Benchmarks

Performance measured on Apple Silicon M-series hardware (.NET 10.0 runtime):

```
==================================================
  VORTEX HIGH-THROUGHPUT ENGINE BENCHMARK
==================================================
Iterations:   100,000 records
Payload size: 256 bytes
Elapsed time: 82 ms
Throughput:   1,219,512 msgs/sec
Data rate:    297.73 MB/sec
Latency p50:  12 us
Latency p90:  28 us
Latency p99:  42 us
Latency p99.9:78 us
==================================================
```

---

## Repository Structure

```
.
+-- src/
|   +-- Vortex.Native/         # C# hardware intrinsics (SIMD, ARM64/SSE4.2 CRC32C)
|   +-- Vortex.Core/           # G# primitives, RecordBatch, memory slabs, ring buffers
|   +-- Vortex.Storage/        # G# log segments, sparse offset index, compaction cleaner
|   +-- Vortex.Replication/    # G# Phi Accrual detector, ISR tracking, Raft consensus
|   +-- Vortex.Network/        # G# binary wire framing, request/response codecs, socket server
|   +-- Vortex.Coordination/   # G# group coordinator, cooperative sticky assignor, offsets
|   +-- Vortex.Processing/     # G# CSP streaming operators (Map/Filter/Window), watermarks
|   +-- Vortex.Schema/         # G# schema registry, dynamic records, compatibility checks
|   +-- Vortex.Server/         # G# broker server daemon, Prometheus telemetry, web dashboard
|   +-- Vortex.Cli/            # G# command-line interface and benchmarking suite
+-- tests/
|   +-- Vortex.Tests/          # 94 comprehensive xUnit unit and end-to-end integration tests
+-- sdk/
|   +-- vortex-py/             # Official Python client SDK with socket and protocol codecs
+-- docs/
|   +-- ARCHITECTURE.md        # Deep architectural design and distributed consensus spec
|   +-- WIRE_PROTOCOL.md       # Binary frame layout, opcodes, and request/response envelopes
|   +-- STORAGE_LAYOUT.md      # On-disk log segment binary representation and indexing
|   +-- BUG_LEDGER.md          # Engineering postmortem and bug reproduction ledger
+-- Makefile                   # Build, test, lint, and benchmark automation targets
```

---

## Quickstart

### Prerequisites
- [.NET 10.0 SDK](https://dotnet.microsoft.com/download)
- Python 3.10+ (for Python SDK)

### 1. Build and Run Tests
Clone the repository and run the test suite:

```bash
git clone https://github.com/Aneesh495/vortex.git
cd vortex

# Build solution and execute all 94 unit & integration tests
make all
```

### 2. Run High-Throughput Storage Benchmark
Execute the built-in storage benchmark:

```bash
make bench
```

### 3. Use the CLI Utility
Interact with the Vortex cluster via the command-line interface:

```bash
# Create a topic with 8 partitions
dotnet run --project src/Vortex.Cli/Vortex.Cli.gsproj -- topic create orders 8

# List cluster topics
dotnet run --project src/Vortex.Cli/Vortex.Cli.gsproj -- topic list

# Describe topic partitions
dotnet run --project src/Vortex.Cli/Vortex.Cli.gsproj -- topic describe orders

# Produce a message
dotnet run --project src/Vortex.Cli/Vortex.Cli.gsproj -- produce orders "payment_completed=100.00"
```

### 4. Embedded Web Dashboard
Launch the broker server to access the live SVG cluster console:

```bash
# Start broker (telemetry server listens on http://127.0.0.1:9100)
# Open http://127.0.0.1:9100/dashboard in your browser
```

---

## Engineering Postmortem and Quality Assurance

Every bug encountered during the architectural evolution of Vortex has been captured in [docs/BUG_LEDGER.md](docs/BUG_LEDGER.md) with failing reproduction tests committed to prevent future regressions. The codebase enforces strict nullability, zero compiler warnings, and SIMD hardware safety.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
