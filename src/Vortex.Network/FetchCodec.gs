// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Network

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

data class PartitionFetchData(Partition int32, FetchOffset int64, MaxBytes int32)
data class TopicFetchData(Topic string, Partitions List[PartitionFetchData])

class FetchRequest {
    var ReplicaId int32 = -1
    var MaxWaitMs int32 = 500
    var MinBytes int32 = 1
    var MaxBytes int32 = 10485760 // 10 MB
    var Topics List[TopicFetchData] = List[TopicFetchData]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteInt32BE(ms, ReplicaId)
        BinaryCodecs.WriteInt32BE(ms, MaxWaitMs)
        BinaryCodecs.WriteInt32BE(ms, MinBytes)
        BinaryCodecs.WriteInt32BE(ms, MaxBytes)
        BinaryCodecs.WriteInt32BE(ms, Topics.Count)

        for t in 0 ... Topics.Count {
            var tf = Topics[t]
            BinaryCodecs.WriteString(ms, tf.Topic)
            BinaryCodecs.WriteInt32BE(ms, tf.Partitions.Count)

            for p in 0 ... tf.Partitions.Count {
                var pf = tf.Partitions[p]
                BinaryCodecs.WriteInt32BE(ms, pf.Partition)
                BinaryCodecs.WriteInt64BE(ms, pf.FetchOffset)
                BinaryCodecs.WriteInt32BE(ms, pf.MaxBytes)
            }
        }

        return ms.ToArray()
    }
}

data class PartitionFetchResponse(Partition int32, ErrorCode int16, HighWatermark int64, RecordBytes []byte)
data class TopicFetchResponse(Topic string, Partitions List[PartitionFetchResponse])

class FetchResponse {
    var ThrottleTimeMs int32 = 0
    var ErrorCode int16 = 0
    var Responses List[TopicFetchResponse] = List[TopicFetchResponse]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteInt32BE(ms, ThrottleTimeMs)
        BinaryCodecs.WriteInt16BE(ms, ErrorCode)
        BinaryCodecs.WriteInt32BE(ms, Responses.Count)

        for t in 0 ... Responses.Count {
            var tr = Responses[t]
            BinaryCodecs.WriteString(ms, tr.Topic)
            BinaryCodecs.WriteInt32BE(ms, tr.Partitions.Count)

            for p in 0 ... tr.Partitions.Count {
                var pr = tr.Partitions[p]
                BinaryCodecs.WriteInt32BE(ms, pr.Partition)
                BinaryCodecs.WriteInt16BE(ms, pr.ErrorCode)
                BinaryCodecs.WriteInt64BE(ms, pr.HighWatermark)
                BinaryCodecs.WriteInt32BE(ms, pr.RecordBytes.Length)
                if pr.RecordBytes.Length > 0 {
                    ms.Write(pr.RecordBytes, 0, pr.RecordBytes.Length)
                }
            }
        }

        return ms.ToArray()
    }
}

shared class FetchCodec {
    func DecodeRequest(payload []byte) FetchRequest {
        var ms = MemoryStream(payload)
        var replicaId int32 = BinaryCodecs.ReadInt32BE(ms)
        var maxWait int32 = BinaryCodecs.ReadInt32BE(ms)
        var minBytes int32 = BinaryCodecs.ReadInt32BE(ms)
        var maxBytes int32 = BinaryCodecs.ReadInt32BE(ms)
        var topicCount int32 = BinaryCodecs.ReadInt32BE(ms)

        var topics = List[TopicFetchData]()
        for t in 0 ... topicCount {
            var topic string = BinaryCodecs.ReadString(ms)
            var partCount int32 = BinaryCodecs.ReadInt32BE(ms)
            var parts = List[PartitionFetchData]()

            for p in 0 ... partCount {
                var partition int32 = BinaryCodecs.ReadInt32BE(ms)
                var offset int64 = BinaryCodecs.ReadInt64BE(ms)
                var pMax int32 = BinaryCodecs.ReadInt32BE(ms)
                parts.Add(PartitionFetchData(partition, offset, pMax))
            }
            topics.Add(TopicFetchData(topic, parts))
        }

        var req = FetchRequest{}
        req.ReplicaId = replicaId
        req.MaxWaitMs = maxWait
        req.MinBytes = minBytes
        req.MaxBytes = maxBytes
        req.Topics = topics
        return req
    }

    func DecodeResponse(payload []byte) FetchResponse {
        var ms = MemoryStream(payload)
        var throttle int32 = BinaryCodecs.ReadInt32BE(ms)
        var err int16 = BinaryCodecs.ReadInt16BE(ms)
        var topicCount int32 = BinaryCodecs.ReadInt32BE(ms)

        var responses = List[TopicFetchResponse]()
        for t in 0 ... topicCount {
            var topic string = BinaryCodecs.ReadString(ms)
            var partCount int32 = BinaryCodecs.ReadInt32BE(ms)
            var parts = List[PartitionFetchResponse]()

            for p in 0 ... partCount {
                var partition int32 = BinaryCodecs.ReadInt32BE(ms)
                var pErr int16 = BinaryCodecs.ReadInt16BE(ms)
                var hw int64 = BinaryCodecs.ReadInt64BE(ms)
                var rLen int32 = BinaryCodecs.ReadInt32BE(ms)
                var rBytes []byte = [rLen]byte
                if rLen > 0 {
                    ms.Read(rBytes, 0, rLen)
                }
                parts.Add(PartitionFetchResponse(partition, pErr, hw, rBytes))
            }
            responses.Add(TopicFetchResponse(topic, parts))
        }

        var resp = FetchResponse{}
        resp.ThrottleTimeMs = throttle
        resp.ErrorCode = err
        resp.Responses = responses
        return resp
    }
}
