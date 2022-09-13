// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Client

import System
import System.IO
import System.Collections.Generic
import System.Threading
import Vortex.Core
import Vortex.Network

// Producer configuration parameters.
class ProducerConfig {
    var BootstrapServers string = "localhost:9092"
    var BatchSize int32 = 16384
    var LingerMs int32 = 5
    var Acks int16 = 1
    var TimeoutMs int32 = 30000
    var Compression int16 = 0
    var MaxRetries int32 = 3
}

// Result metadata returned after a record is appended to the cluster.
class RecordMetadata {
    var Topic string
    var Partition int32
    var Offset int64
    var Timestamp int64
    var ErrorCode int16
}

// Key-based and round-robin partition selector.
class Partitioner {
    var Counter int32 = 0

    // Computes target partition using 32-bit FNV-1a hash for keyed records or round-robin for unkeyed.
    func Partition(key ([]byte)?, numPartitions int32) int32 {
        if numPartitions <= 0 {
            return 0
        }

        if key != nil && key.Length > 0 {
            var hash uint32 = 2166136261
            for i in 0 ... key.Length {
                hash = (hash ^ uint32(key[i])) * 16777619
            }
            var p int32 = int32(hash % uint32(numPartitions))
            if p < 0 {
                return -p
            }
            return p
        }

        var next int32 = Interlocked.Increment(ref Counter)
        var res int32 = next % numPartitions
        if res < 0 {
            return -res
        }
        return res
    }
}

// High-throughput batching producer accumulating messages before wire transmission.
class VortexProducer {
    var Config ProducerConfig
    var Partitioner Partitioner = Partitioner()
    var Metadata Dictionary[string, int32] = Dictionary[string, int32]() // Topic -> NumPartitions
    var Accumulator Dictionary[string, List[Record]] = Dictionary[string, List[Record]]() // "topic:partition" -> records
    var AccumulatorBytes Dictionary[string, int32] = Dictionary[string, int32]()
    var BucketKeys List[string] = List[string]()
    var SentRecordsCount int64 = 0

    func RegisterTopicMetadata(topic string, numPartitions int32) {
        Metadata[topic] = numPartitions
    }

    func GetPartitionCount(topic string) int32 {
        var count int32 = 0
        var found bool = Metadata.TryGetValue(topic, out count)
        if found && count > 0 {
            return count
        }
        return 1
    }

    // Sends a record to the cluster, automatically selecting partition.
    func Send(topic string, key ([]byte)?, value []byte) RecordMetadata {
        var numPartitions int32 = GetPartitionCount(topic)
        var p int32 = Partitioner.Partition(key, numPartitions)
        return SendToPartition(topic, p, key, value)
    }

    // Sends a record directly to a designated partition.
    func SendToPartition(topic string, partition int32, key ([]byte)?, value []byte) RecordMetadata {
        var rec = Record()
        rec.Key = key != nil ? key : [0]byte
        rec.Value = value
        rec.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

        var bucketKey string = topic + ":" + partition.ToString()
        var list List[Record]? = nil
        var found bool = Accumulator.TryGetValue(bucketKey, out list)
        if !found || list == nil {
            list = List[Record]()
            Accumulator[bucketKey] = list
            AccumulatorBytes[bucketKey] = 0
            BucketKeys.Add(bucketKey)
        }

        list.Add(rec)
        AccumulatorBytes[bucketKey] = AccumulatorBytes[bucketKey] + rec.Size()
        SentRecordsCount = SentRecordsCount + 1

        var meta = RecordMetadata()
        meta.Topic = topic
        meta.Partition = partition
        meta.Offset = int64(list.Count - 1)
        meta.Timestamp = rec.Timestamp
        meta.ErrorCode = 0
        return meta
    }

    // Flushes accumulated batches into formatted RecordBatch instances ready for dispatch.
    func DrainBatches() List[PartitionProduceData] {
        var drained = List[PartitionProduceData]()

        for i in 0 ... BucketKeys.Count {
            var bKey = BucketKeys[i]
            var records = Accumulator[bKey]
            if records.Count > 0 {
                var parts = bKey.Split(':')
                var partition int32 = Convert.ToInt32(parts[1])

                var batch = RecordBatch()
                batch.BaseOffset = 0
                batch.Magic = 2
                batch.Attributes = Config.Compression
                batch.ProducerId = 1
                batch.ProducerEpoch = 0
                batch.Records = records

                using var ms = MemoryStream()
                batch.Serialize(ms)
                var batchBytes []byte = ms.ToArray()

                var pData = PartitionProduceData(partition, batchBytes)
                drained.Add(pData)
            }
        }

        Accumulator.Clear()
        AccumulatorBytes.Clear()
        BucketKeys.Clear()
        return drained
    }
}
