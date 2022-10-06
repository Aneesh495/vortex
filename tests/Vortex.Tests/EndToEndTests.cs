// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Vortex.Client;
using Vortex.Coordination;
using Vortex.Core;
using Vortex.Network;
using Vortex.Server;
using Xunit;

namespace Vortex.Tests;

public class EndToEndTests : IDisposable
{
    private readonly string _testDir;
    private readonly VortexBroker _broker;

    public EndToEndTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "vortex-e2e-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);

        _broker = new VortexBroker();
        _broker.Config.BrokerId = 1;
        _broker.Config.Host = "127.0.0.1";
        _broker.Config.Port = 9092;
        _broker.Config.LogDir = _testDir;
        _broker.Start();
    }

    public void Dispose()
    {
        _broker.Stop();
        try
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
            }
        }
        catch
        {
            // Suppress cleanup exceptions in temp directory
        }
    }

    [Fact]
    public void Broker_MultiPartitionProduceAndFetch()
    {
        // 1. Create topic with 4 partitions
        short createErr = _broker.CreateTopic("e2e-orders", 4);
        Assert.Equal(VortexError.None, createErr);

        // 2. Prepare producer and accumulate records across partitions
        var producer = new VortexProducer();
        producer.RegisterTopicMetadata("e2e-orders", 4);

        for (int i = 0; i < 20; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"customer-{i % 4}");
            byte[] val = Encoding.UTF8.GetBytes($"order-payload-{i}");
            producer.Send("e2e-orders", key, val);
        }

        var drainedBatches = producer.DrainBatches();
        Assert.NotEmpty(drainedBatches);

        // 3. Assemble ProduceRequest and dispatch to broker
        var produceReq = new ProduceRequest();
        produceReq.Acks = 1;
        produceReq.TimeoutMs = 5000;

        var tProduce = new TopicProduceData("e2e-orders", drainedBatches);
        produceReq.Topics.Add(tProduce);

        var produceResp = _broker.HandleProduce(produceReq);
        Assert.Single(produceResp.Responses);

        var topicResp = produceResp.Responses[0];
        Assert.Equal("e2e-orders", topicResp.Topic);
        Assert.NotEmpty(topicResp.Partitions);

        foreach (var pResp in topicResp.Partitions)
        {
            Assert.Equal(VortexError.None, pResp.ErrorCode);
            Assert.True(pResp.BaseOffset >= 0);
        }

        // 4. Fetch records from partition 0
        var fetchReq = new FetchRequest();
        var pFetch = new PartitionFetchData(0, 0L, 1024 * 1024);
        var tFetch = new TopicFetchData("e2e-orders", new List<PartitionFetchData> { pFetch });
        fetchReq.Topics.Add(tFetch);

        var fetchResp = _broker.HandleFetch(fetchReq);
        Assert.Single(fetchResp.Responses);

        var fetchedTopic = fetchResp.Responses[0];
        Assert.Single(fetchedTopic.Partitions);

        var fetchedP0 = fetchedTopic.Partitions[0];
        Assert.Equal(0, fetchedP0.Partition);
        Assert.Equal(VortexError.None, fetchedP0.ErrorCode);
        Assert.NotEmpty(fetchedP0.RecordBytes);

        // 5. Deserialize RecordBatch from fetched bytes and verify contents
        using var ms = new MemoryStream(fetchedP0.RecordBytes);
        var batch = RecordBatchCodec.Deserialize(ms);

        Assert.NotNull(batch);
        Assert.True(batch.RecordCount() > 0);
        var firstRecord = batch.Records[0];
        Assert.StartsWith("order-payload-", firstRecord.ValueString());
    }

    [Fact]
    public void Broker_MetadataTopicDiscovery()
    {
        _broker.CreateTopic("telemetry", 8);
        _broker.CreateTopic("alerts", 2);

        var metaReq = new MetadataRequest();
        var metaResp = _broker.HandleMetadata(metaReq);

        Assert.Single(metaResp.Brokers);
        Assert.Equal(1, metaResp.Brokers[0].NodeId);
        Assert.Equal("127.0.0.1", metaResp.Brokers[0].Host);

        var topics = metaResp.Topics;
        Assert.Equal(2, topics.Count);

        var telemetryMeta = topics.Find(t => t.Topic == "telemetry");
        Assert.NotNull(telemetryMeta);
        Assert.Equal(8, telemetryMeta.Partitions.Count);

        var alertsMeta = topics.Find(t => t.Topic == "alerts");
        Assert.NotNull(alertsMeta);
        Assert.Equal(2, alertsMeta.Partitions.Count);
    }

    [Fact]
    public void Broker_ConsumerGroupAndOffsetManager()
    {
        // 1. Commit offset for a consumer group
        _broker.Offsets.CommitOffset("consumer-group-alpha", "e2e-orders", 0, 42L, "alpha-meta", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // 2. Fetch committed offset
        var (offset, metadata, err) = _broker.Offsets.FetchOffset("consumer-group-alpha", "e2e-orders", 0);
        Assert.Equal(42L, offset);
        Assert.Equal("alpha-meta", metadata);
        Assert.Equal(VortexError.None, err);

        // 3. Uncommitted partition returns -1
        var (uncommitted, _, _) = _broker.Offsets.FetchOffset("consumer-group-alpha", "e2e-orders", 1);
        Assert.Equal(-1L, uncommitted);
    }
}
