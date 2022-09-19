// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Server

import System
import System.IO
import System.Collections.Generic
import Vortex.Core
import Vortex.Storage
import Vortex.Replication
import Vortex.Network
import Vortex.Coordination
import Vortex.Schema

// Configuration settings for a standalone or clustered Vortex broker.
class BrokerConfig {
    var BrokerId int32 = 1
    var Host string = "127.0.0.1"
    var Port int32 = 9092
    var LogDir string = "/tmp/vortex-data"
    var HeartbeatIntervalMs int32 = 3000
    var SessionTimeoutMs int32 = 10000
}

// Core broker daemon combining storage, network handling, consensus, and coordination.
class VortexBroker {
    var Config BrokerConfig = BrokerConfig()
    var IsRunning bool = false
    var PartitionLogs Dictionary[string, PartitionLog] = Dictionary[string, PartitionLog]()
    var Coordinator GroupCoordinator = GroupCoordinator()
    var Offsets OffsetManager = OffsetManager()
    var RaftNode RaftCoordinator = RaftCoordinator()
    var Schemas SchemaRegistry = SchemaRegistry()
    var Topics Dictionary[string, int32] = Dictionary[string, int32]()
    var TopicList List[string] = List[string]()

    // Starts the broker daemon and prepares log directories.
    func Start() {
        if !Directory.Exists(Config.LogDir) {
            Directory.CreateDirectory(Config.LogDir)
        }
        RaftNode.NodeId = Config.BrokerId
        IsRunning = true
    }

    // Stops the broker daemon and flushes partition logs to disk.
    func Stop() {
        IsRunning = false
        var keys = List[string]()
        for i in 0 ... TopicList.Count {
            var t = TopicList[i]
            var count int32 = Topics[t]
            for p in 0 ... count {
                var k string = t + "-" + p.ToString()
                keys.Add(k)
            }
        }

        for i in 0 ... keys.Count {
            var log PartitionLog? = nil
            if PartitionLogs.TryGetValue(keys[i], out log) && log != nil {
                log.Flush()
            }
        }
    }

    // Creates a new topic with the designated partition count.
    func CreateTopic(topic string, partitionCount int32) int16 {
        if Topics.ContainsKey(topic) {
            return VortexError.TopicAlreadyExists
        }
        if partitionCount <= 0 {
            return VortexError.CorruptMessage
        }

        Topics[topic] = partitionCount
        TopicList.Add(topic)

        for p in 0 ... partitionCount {
            var pl = PartitionLogFactory.OpenOrCreate(topic, p, Config.LogDir)
            var key string = topic + "-" + p.ToString()
            PartitionLogs[key] = pl
        }

        return VortexError.None
    }

    // Obtains the PartitionLog instance for a given topic and partition.
    func GetPartitionLog(topic string, partition int32) PartitionLog? {
        var key string = topic + "-" + partition.ToString()
        var pl PartitionLog? = nil
        var found bool = PartitionLogs.TryGetValue(key, out pl)
        if found {
            return pl
        }
        return nil
    }

    // Handles an incoming ProduceRequest packet and appends records to local logs.
    func HandleProduce(req ProduceRequest) ProduceResponse {
        var resp = ProduceResponse()

        for i in 0 ... req.Topics.Count {
            var tData = req.Topics[i]
            var partitionResponses = List[PartitionProduceResponse]()

            for j in 0 ... tData.Partitions.Count {
                var pData = tData.Partitions[j]
                var log = GetPartitionLog(tData.Topic, pData.Partition)

                if log == nil {
                    var pResp = PartitionProduceResponse(pData.Partition, VortexError.UnknownTopicOrPartition, -1, 0)
                    partitionResponses.Add(pResp)
                } else {
                    using var ms = MemoryStream(pData.RecordBytes)
                    var batch = RecordBatchCodec.Deserialize(ms)
                    var appendRes = log.Append(batch.Records, batch.PartitionLeaderEpoch, batch.ProducerId, batch.ProducerEpoch)
                    var nowMs int64 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

                    var pResp = PartitionProduceResponse(pData.Partition, VortexError.None, appendRes.BaseOffset, nowMs)
                    partitionResponses.Add(pResp)
                }
            }

            var tResp = TopicProduceResponse(tData.Topic, partitionResponses)
            resp.Responses.Add(tResp)
        }

        return resp
    }

    // Handles an incoming FetchRequest packet and reads records from local logs.
    func HandleFetch(req FetchRequest) FetchResponse {
        var resp = FetchResponse()

        for i in 0 ... req.Topics.Count {
            var tReq = req.Topics[i]
            var partitionResponses = List[PartitionFetchResponse]()

            for j in 0 ... tReq.Partitions.Count {
                var pReq = tReq.Partitions[j]
                var log = GetPartitionLog(tReq.Topic, pReq.Partition)

                if log == nil {
                    var pResp = PartitionFetchResponse(pReq.Partition, VortexError.UnknownTopicOrPartition, -1, [0]byte)
                    partitionResponses.Add(pResp)
                } else {
                    var batches = log.Read(pReq.FetchOffset, pReq.MaxBytes, log.HighWatermark)
                    var recBytes []byte = [0]byte
                    if batches.Count > 0 {
                        using var ms = MemoryStream()
                        for b in 0 ... batches.Count {
                            batches[b].Serialize(ms)
                        }
                        recBytes = ms.ToArray()
                    }

                    var pResp = PartitionFetchResponse(pReq.Partition, VortexError.None, log.HighWatermark, recBytes)
                    partitionResponses.Add(pResp)
                }
            }

            var tResp = TopicFetchResponse(tReq.Topic, partitionResponses)
            resp.Responses.Add(tResp)
        }

        return resp
    }

    // Handles a cluster metadata request, advertising broker topology and topics.
    func HandleMetadata(req MetadataRequest) MetadataResponse {
        var resp = MetadataResponse()

        var bMeta = BrokerMetadata(Config.BrokerId, Config.Host, Config.Port, "rack-1")
        resp.Brokers.Add(bMeta)
        resp.ClusterId = "vortex-cluster-1"
        resp.ControllerId = Config.BrokerId

        var requestedTopics = req.Topics
        var queryAll bool = requestedTopics.Count == 0

        for i in 0 ... TopicList.Count {
            var topic = TopicList[i]
            if queryAll || requestedTopics.Contains(topic) {
                var partitions = List[PartitionMetadata]()
                var count int32 = Topics[topic]

                for p in 0 ... count {
                    var replicas = List[int32]()
                    replicas.Add(Config.BrokerId)
                    var isr = List[int32]()
                    isr.Add(Config.BrokerId)

                    var pMeta = PartitionMetadata(p, Config.BrokerId, 0, replicas, isr, VortexError.None)
                    partitions.Add(pMeta)
                }

                var tMeta = TopicMetadata(topic, false, VortexError.None, partitions)
                resp.Topics.Add(tMeta)
            }
        }

        return resp
    }
}
