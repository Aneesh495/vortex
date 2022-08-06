// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Network

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

data class PartitionProduceData(Partition int32, RecordBytes []byte)
data class TopicProduceData(Topic string, Partitions List[PartitionProduceData])

class ProduceRequest {
    var Acks int16 = -1
    var TimeoutMs int32 = 30000
    var Topics List[TopicProduceData] = List[TopicProduceData]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteInt16BE(ms, Acks)
        BinaryCodecs.WriteInt32BE(ms, TimeoutMs)
        BinaryCodecs.WriteInt32BE(ms, Topics.Count)

        for t in 0 ... Topics.Count {
            var topicData = Topics[t]
            BinaryCodecs.WriteString(ms, topicData.Topic)
            BinaryCodecs.WriteInt32BE(ms, topicData.Partitions.Count)

            for p in 0 ... topicData.Partitions.Count {
                var partData = topicData.Partitions[p]
                BinaryCodecs.WriteInt32BE(ms, partData.Partition)
                BinaryCodecs.WriteInt32BE(ms, partData.RecordBytes.Length)
                if partData.RecordBytes.Length > 0 {
                    ms.Write(partData.RecordBytes, 0, partData.RecordBytes.Length)
                }
            }
        }

        return ms.ToArray()
    }
}

data class PartitionProduceResponse(Partition int32, ErrorCode int16, BaseOffset int64, LogAppendTimeMs int64)
data class TopicProduceResponse(Topic string, Partitions List[PartitionProduceResponse])

class ProduceResponse {
    var Responses List[TopicProduceResponse] = List[TopicProduceResponse]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteInt32BE(ms, Responses.Count)

        for t in 0 ... Responses.Count {
            var tr = Responses[t]
            BinaryCodecs.WriteString(ms, tr.Topic)
            BinaryCodecs.WriteInt32BE(ms, tr.Partitions.Count)

            for p in 0 ... tr.Partitions.Count {
                var pr = tr.Partitions[p]
                BinaryCodecs.WriteInt32BE(ms, pr.Partition)
                BinaryCodecs.WriteInt16BE(ms, pr.ErrorCode)
                BinaryCodecs.WriteInt64BE(ms, pr.BaseOffset)
                BinaryCodecs.WriteInt64BE(ms, pr.LogAppendTimeMs)
            }
        }

        return ms.ToArray()
    }
}

shared class ProduceCodec {
    func DecodeRequest(payload []byte) ProduceRequest {
        var ms = MemoryStream(payload)
        var acks int16 = BinaryCodecs.ReadInt16BE(ms)
        var timeoutMs int32 = BinaryCodecs.ReadInt32BE(ms)
        var topicCount int32 = BinaryCodecs.ReadInt32BE(ms)

        var topics = List[TopicProduceData]()
        for t in 0 ... topicCount {
            var topic string = BinaryCodecs.ReadString(ms)
            var partCount int32 = BinaryCodecs.ReadInt32BE(ms)
            var partitions = List[PartitionProduceData]()

            for p in 0 ... partCount {
                var partition int32 = BinaryCodecs.ReadInt32BE(ms)
                var bytesLen int32 = BinaryCodecs.ReadInt32BE(ms)
                var recBytes []byte = [bytesLen]byte
                if bytesLen > 0 {
                    ms.Read(recBytes, 0, bytesLen)
                }
                partitions.Add(PartitionProduceData(partition, recBytes))
            }
            topics.Add(TopicProduceData(topic, partitions))
        }

        var req = ProduceRequest{}
        req.Acks = acks
        req.TimeoutMs = timeoutMs
        req.Topics = topics
        return req
    }

    func DecodeResponse(payload []byte) ProduceResponse {
        var ms = MemoryStream(payload)
        var topicCount int32 = BinaryCodecs.ReadInt32BE(ms)

        var responses = List[TopicProduceResponse]()
        for t in 0 ... topicCount {
            var topic string = BinaryCodecs.ReadString(ms)
            var partCount int32 = BinaryCodecs.ReadInt32BE(ms)
            var partitions = List[PartitionProduceResponse]()

            for p in 0 ... partCount {
                var partition int32 = BinaryCodecs.ReadInt32BE(ms)
                var err int16 = BinaryCodecs.ReadInt16BE(ms)
                var baseOffset int64 = BinaryCodecs.ReadInt64BE(ms)
                var appendTime int64 = BinaryCodecs.ReadInt64BE(ms)
                partitions.Add(PartitionProduceResponse(partition, err, baseOffset, appendTime))
            }
            responses.Add(TopicProduceResponse(topic, partitions))
        }

        var resp = ProduceResponse{}
        resp.Responses = responses
        return resp
    }
}
