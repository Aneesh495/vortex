// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Client

import System
import System.Collections.Generic
import Vortex.Core
import Vortex.Coordination

// Consumer configuration settings.
class ConsumerConfig {
    var BootstrapServers string = "localhost:9092"
    var GroupId string = "default-group"
    var MemberId string = ""
    var AutoOffsetReset string = "earliest" // "earliest" or "latest"
    var EnableAutoCommit bool = true
    var AutoCommitIntervalMs int32 = 5000
    var MaxPollRecords int32 = 500
}

// Emitted record yielded to consumer application loops.
class ConsumerRecord {
    var Topic string
    var Partition int32
    var Offset int64
    var Timestamp int64
    var Key []byte = [0]byte
    var Value []byte = [0]byte

    func KeyString() string {
        return System.Text.Encoding.UTF8.GetString(Key)
    }

    func ValueString() string {
        return System.Text.Encoding.UTF8.GetString(Value)
    }
}

// High-level consumer maintaining partition assignments, poll buffers, and offset commits.
class VortexConsumer {
    var Config ConsumerConfig = ConsumerConfig()
    var SubscribedTopics List[string] = List[string]()
    var AssignedPartitions List[TopicPartition] = List[TopicPartition]()
    var Positions Dictionary[string, int64] = Dictionary[string, int64]()
    var CommittedOffsets Dictionary[string, int64] = Dictionary[string, int64]()
    var FetchBuffer List[ConsumerRecord] = List[ConsumerRecord]()

    // Subscribes consumer to a list of topics.
    func Subscribe(topics List[string]) {
        SubscribedTopics.Clear()
        for i in 0 ... topics.Count {
            SubscribedTopics.Add(topics[i])
        }
    }

    // Directly assigns static partition assignments to this consumer.
    func Assign(partitions List[TopicPartition]) {
        AssignedPartitions.Clear()
        for i in 0 ... partitions.Count {
            var tp = partitions[i]
            AssignedPartitions.Add(tp)
            var key string = tp.Format()
            if !Positions.ContainsKey(key) {
                Positions[key] = 0
            }
        }
    }

    func Assignment() List[TopicPartition] {
        return AssignedPartitions
    }

    // Ingests fetched raw records into the internal fetch buffer.
    func BufferRecords(records List[ConsumerRecord]) {
        for i in 0 ... records.Count {
            FetchBuffer.Add(records[i])
        }
    }

    // Polls buffered records up to MaxPollRecords, updating current position.
    func Poll(timeoutMs int32) List[ConsumerRecord] {
        var batch = List[ConsumerRecord]()
        var takeCount int32 = Config.MaxPollRecords
        if takeCount > FetchBuffer.Count {
            takeCount = FetchBuffer.Count
        }

        for i in 0 ... takeCount {
            var rec = FetchBuffer[i]
            batch.Add(rec)

            var key string = rec.Topic + "-" + rec.Partition.ToString()
            Positions[key] = rec.Offset + 1
        }

        FetchBuffer.RemoveRange(0, takeCount)
        return batch
    }

    // Returns current fetch position for a topic partition.
    func Position(tp TopicPartition) int64 {
        var key string = tp.Format()
        var pos int64 = 0
        var found bool = Positions.TryGetValue(key, out pos)
        if found {
            return pos
        }
        return 0
    }

    // Updates fetch position for a topic partition.
    func Seek(tp TopicPartition, offset int64) {
        var key string = tp.Format()
        Positions[key] = offset
    }

    // Commits the current position offsets for all assigned partitions.
    func CommitSync() {
        for i in 0 ... AssignedPartitions.Count {
            var tp = AssignedPartitions[i]
            var key string = tp.Format()
            var pos int64 = 0
            if Positions.TryGetValue(key, out pos) {
                CommittedOffsets[key] = pos
            }
        }
    }

    // Fetches the latest committed offset for a partition. Returns -1 if not committed.
    func Committed(tp TopicPartition) int64 {
        var key string = tp.Format()
        var comm int64 = -1
        var found bool = CommittedOffsets.TryGetValue(key, out comm)
        if found {
            return comm
        }
        return -1
    }
}
