// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Network

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

data class BrokerMetadata(NodeId int32, Host string, Port int32, Rack string)
data class PartitionMetadata(PartitionId int32, LeaderId int32, LeaderEpoch int32, Replicas List[int32], Isr List[int32], ErrorCode int16)
data class TopicMetadata(Topic string, IsInternal bool, ErrorCode int16, Partitions List[PartitionMetadata])

class MetadataRequest {
    var Topics List[string] = List[string]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteInt32BE(ms, Topics.Count)
        for i in 0 ... Topics.Count {
            BinaryCodecs.WriteString(ms, Topics[i])
        }
        return ms.ToArray()
    }
}

class MetadataResponse {
    var Brokers List[BrokerMetadata] = List[BrokerMetadata]()
    var ClusterId string = "vortex-cluster"
    var ControllerId int32 = 1
    var Topics List[TopicMetadata] = List[TopicMetadata]()

    func Encode() []byte {
        var ms = MemoryStream()
        BinaryCodecs.WriteString(ms, ClusterId)
        BinaryCodecs.WriteInt32BE(ms, ControllerId)

        // Write brokers
        BinaryCodecs.WriteInt32BE(ms, Brokers.Count)
        for b in 0 ... Brokers.Count {
            var broker = Brokers[b]
            BinaryCodecs.WriteInt32BE(ms, broker.NodeId)
            BinaryCodecs.WriteString(ms, broker.Host)
            BinaryCodecs.WriteInt32BE(ms, broker.Port)
            BinaryCodecs.WriteString(ms, broker.Rack)
        }

        // Write topics
        BinaryCodecs.WriteInt32BE(ms, Topics.Count)
        for t in 0 ... Topics.Count {
            var tm = Topics[t]
            BinaryCodecs.WriteString(ms, tm.Topic)
            var internalByte byte = byte(0)
            if tm.IsInternal { internalByte = byte(1) }
            ms.WriteByte(internalByte)
            BinaryCodecs.WriteInt16BE(ms, tm.ErrorCode)

            BinaryCodecs.WriteInt32BE(ms, tm.Partitions.Count)
            for p in 0 ... tm.Partitions.Count {
                var pm = tm.Partitions[p]
                BinaryCodecs.WriteInt32BE(ms, pm.PartitionId)
                BinaryCodecs.WriteInt32BE(ms, pm.LeaderId)
                BinaryCodecs.WriteInt32BE(ms, pm.LeaderEpoch)
                BinaryCodecs.WriteInt16BE(ms, pm.ErrorCode)

                BinaryCodecs.WriteInt32BE(ms, pm.Replicas.Count)
                for r in 0 ... pm.Replicas.Count {
                    BinaryCodecs.WriteInt32BE(ms, pm.Replicas[r])
                }

                BinaryCodecs.WriteInt32BE(ms, pm.Isr.Count)
                for i in 0 ... pm.Isr.Count {
                    BinaryCodecs.WriteInt32BE(ms, pm.Isr[i])
                }
            }
        }

        return ms.ToArray()
    }
}

shared class MetadataCodec {
    func DecodeRequest(payload []byte) MetadataRequest {
        var ms = MemoryStream(payload)
        var count int32 = BinaryCodecs.ReadInt32BE(ms)
        var topics = List[string]()
        for i in 0 ... count {
            topics.Add(BinaryCodecs.ReadString(ms))
        }

        var req = MetadataRequest{}
        req.Topics = topics
        return req
    }

    func DecodeResponse(payload []byte) MetadataResponse {
        var ms = MemoryStream(payload)
        var clusterId string = BinaryCodecs.ReadString(ms)
        var controllerId int32 = BinaryCodecs.ReadInt32BE(ms)

        var brokerCount int32 = BinaryCodecs.ReadInt32BE(ms)
        var brokers = List[BrokerMetadata]()
        for b in 0 ... brokerCount {
            var nodeId int32 = BinaryCodecs.ReadInt32BE(ms)
            var host string = BinaryCodecs.ReadString(ms)
            var port int32 = BinaryCodecs.ReadInt32BE(ms)
            var rack string = BinaryCodecs.ReadString(ms)
            brokers.Add(BrokerMetadata(nodeId, host, port, rack))
        }

        var topicCount int32 = BinaryCodecs.ReadInt32BE(ms)
        var topics = List[TopicMetadata]()
        for t in 0 ... topicCount {
            var topic string = BinaryCodecs.ReadString(ms)
            var isInternal bool = ms.ReadByte() == 1
            var err int16 = BinaryCodecs.ReadInt16BE(ms)

            var partCount int32 = BinaryCodecs.ReadInt32BE(ms)
            var parts = List[PartitionMetadata]()
            for p in 0 ... partCount {
                var pId int32 = BinaryCodecs.ReadInt32BE(ms)
                var leaderId int32 = BinaryCodecs.ReadInt32BE(ms)
                var epoch int32 = BinaryCodecs.ReadInt32BE(ms)
                var pErr int16 = BinaryCodecs.ReadInt16BE(ms)

                var repCount int32 = BinaryCodecs.ReadInt32BE(ms)
                var reps = List[int32]()
                for r in 0 ... repCount {
                    reps.Add(BinaryCodecs.ReadInt32BE(ms))
                }

                var isrCount int32 = BinaryCodecs.ReadInt32BE(ms)
                var isr = List[int32]()
                for i in 0 ... isrCount {
                    isr.Add(BinaryCodecs.ReadInt32BE(ms))
                }

                parts.Add(PartitionMetadata(pId, leaderId, epoch, reps, isr, pErr))
            }
            topics.Add(TopicMetadata(topic, isInternal, err, parts))
        }

        var resp = MetadataResponse{}
        resp.ClusterId = clusterId
        resp.ControllerId = controllerId
        resp.Brokers = brokers
        resp.Topics = topics
        return resp
    }
}
