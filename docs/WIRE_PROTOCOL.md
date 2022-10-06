# Vortex Binary Wire Protocol Specification

Version: 1.0 (Draft Standard)  
Endianness: Big-Endian (Network Byte Order)

---

## 1. Frame Envelope Layout

Every message sent across TCP connections is framed with an envelope containing length prefixing and Castagnoli CRC32C integrity checksums:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Frame Length                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         CRC32C Checksum                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           API Key                             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         API Version                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Correlation ID                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       Client ID Length        |     Client ID Bytes ...       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload Bytes ...                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Field Definitions:
- `Frame Length` (int32): Total size in bytes of the following frame content (CRC32C checksum through end of payload).
- `CRC32C Checksum` (uint32): Castagnoli polynomial checksum over the payload starting from `API Key` to the end of `Payload Bytes`.
- `API Key` (int16): Identifier specifying the requested operation.
- `API Version` (int16): Schema version for backwards/forwards protocol compatibility.
- `Correlation ID` (int32): Client-generated integer echoed in the server response for multiplexed asynchronous tracking.
- `Client ID Length` (int16): Length of the ASCII/UTF-8 client identifier string.
- `Client ID Bytes`: Identifier string representing the client library or host.
- `Payload Bytes`: Request or response binary data.

---

## 2. API Key Catalog

| API Key | Name | Description |
|:-------:|:-----|:------------|
| 0 | Produce | Publish record batches to topic partitions |
| 1 | Fetch | Read record batches from topic partitions |
| 3 | Metadata | Discover cluster topology, partition leaders, and ISR |
| 8 | OffsetCommit | Commit consumer group partition progress |
| 9 | OffsetFetch | Retrieve latest committed consumer group offsets |
| 11 | JoinGroup | Register member and negotiate consumer group protocol |
| 12 | Heartbeat | Keep consumer group membership active |
| 13 | LeaveGroup | Explicitly leave a consumer group |
| 14 | SyncGroup | Distribute partition assignments across group members |

---

## 3. Protocol Request / Response Specifications

### 3.1 Produce Request (Key = 0)
```
ProduceRequest => Acks TimeoutMs [TopicData]
  Acks => int16
  TimeoutMs => int32
  TopicData => Topic [PartitionData]
    Topic => string (2-byte length + UTF-8 bytes)
    PartitionData => Partition RecordBytes
      Partition => int32
      RecordBytes => int32 length + RecordBatch binary bytes
```

### 3.2 Fetch Request (Key = 1)
```
FetchRequest => ReplicaId MaxWaitMs MinBytes [TopicFetchData]
  TopicFetchData => Topic [PartitionFetchData]
    Topic => string
    PartitionFetchData => Partition FetchOffset MaxBytes
      Partition => int32
      FetchOffset => int64
      MaxBytes => int32
```

### 3.3 Metadata Request (Key = 3)
```
MetadataRequest => [TopicNames]
  TopicNames => string (empty array requests all cluster topics)

MetadataResponse => [Brokers] ControllerId [TopicMetadata]
  Brokers => NodeId Host Port Rack
    NodeId => int32
    Host => string
    Port => int32
    Rack => string
  ControllerId => int32
  TopicMetadata => Topic IsInternal ErrorCode [PartitionMetadata]
    PartitionMetadata => PartitionId LeaderId LeaderEpoch [Replicas] [Isr] ErrorCode
```

---

## 4. Error Code Specification

| Code | Name | Description |
|:----:|:-----|:------------|
| 0 | None | Operation completed successfully |
| 1 | OffsetOutOfRange | Requested offset is before log start or beyond log end |
| 2 | CorruptMessage | CRC checksum validation failed |
| 3 | UnknownTopicOrPartition | Topic or partition does not exist in cluster |
| 4 | LeaderNotAvailable | Partition leader is currently electing |
| 5 | NotLeaderOrFollower | Node is not assigned as leader or follower |
| 6 | RequestTimedOut | Request exceeded specified timeout window |
| 11 | RebalanceInProgress | Consumer group is currently rebalancing |
| 14 | CommitFailed | Offset commit generation mismatch |
| 15 | TopicAlreadyExists | Topic name already registered |
| 18 | SchemaIncompatible | Registered schema breaks compatibility mode |
