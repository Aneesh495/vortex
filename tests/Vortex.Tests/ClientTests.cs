// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.IO;
using System.Text;
using Vortex.Client;
using Vortex.Core;
using Xunit;

namespace Vortex.Tests;

public class ClientTests
{
    [Fact]
    public void Partitioner_KeyHashingIsDeterministic()
    {
        var partitioner = new Partitioner();
        byte[] key = Encoding.UTF8.GetBytes("user-42819");

        int p1 = partitioner.Partition(key, 8);
        int p2 = partitioner.Partition(key, 8);
        int p3 = partitioner.Partition(key, 8);

        Assert.Equal(p1, p2);
        Assert.Equal(p2, p3);
        Assert.InRange(p1, 0, 7);
    }

    [Fact]
    public void Partitioner_UnkeyedRecords_DistributeRoundRobin()
    {
        var partitioner = new Partitioner();
        int numPartitions = 3;

        int p1 = partitioner.Partition(null, numPartitions);
        int p2 = partitioner.Partition(null, numPartitions);
        int p3 = partitioner.Partition(null, numPartitions);

        Assert.True(p1 != p2 || p2 != p3);
        Assert.InRange(p1, 0, numPartitions - 1);
        Assert.InRange(p2, 0, numPartitions - 1);
        Assert.InRange(p3, 0, numPartitions - 1);
    }

    [Fact]
    public void VortexProducer_SendAndDrainBatches()
    {
        var producer = new VortexProducer();
        producer.RegisterTopicMetadata("orders", 4);

        var key1 = Encoding.UTF8.GetBytes("k1");
        var val1 = Encoding.UTF8.GetBytes("v1");
        var val2 = Encoding.UTF8.GetBytes("v2");

        var meta1 = producer.Send("orders", key1, val1);
        var meta2 = producer.SendToPartition("orders", 2, null, val2);

        Assert.Equal("orders", meta1.Topic);
        Assert.InRange(meta1.Partition, 0, 3);
        Assert.Equal(0L, meta1.Offset);

        Assert.Equal("orders", meta2.Topic);
        Assert.Equal(2, meta2.Partition);

        var drained = producer.DrainBatches();
        Assert.NotEmpty(drained);

        // Verify payload can be deserialized back into a RecordBatch
        var firstBatchData = drained[0];
        using var ms = new MemoryStream(firstBatchData.RecordBytes);
        var batch = RecordBatchCodec.Deserialize(ms);

        Assert.NotNull(batch);
        Assert.True(batch.RecordCount() > 0);
    }

    [Fact]
    public void VortexConsumer_AssignAndPoll_UpdatesPositions()
    {
        var consumer = new VortexConsumer();
        var tp0 = new TopicPartition("telemetry", 0);
        var tp1 = new TopicPartition("telemetry", 1);

        var partitions = new List<TopicPartition> { tp0, tp1 };
        consumer.Assign(partitions);

        Assert.Equal(2, consumer.Assignment().Count);
        Assert.Equal(0L, consumer.Position(tp0));

        var r1 = new ConsumerRecord
        {
            Topic = "telemetry",
            Partition = 0,
            Offset = 0,
            Timestamp = 1000L,
            Key = Encoding.UTF8.GetBytes("k1"),
            Value = Encoding.UTF8.GetBytes("v1")
        };
        var r2 = new ConsumerRecord
        {
            Topic = "telemetry",
            Partition = 0,
            Offset = 1,
            Timestamp = 1010L,
            Key = Encoding.UTF8.GetBytes("k2"),
            Value = Encoding.UTF8.GetBytes("v2")
        };

        var records = new List<ConsumerRecord> { r1, r2 };
        consumer.BufferRecords(records);

        var polled = consumer.Poll(100);
        Assert.Equal(2, polled.Count);
        Assert.Equal("k1", polled[0].KeyString());
        Assert.Equal("v1", polled[0].ValueString());

        // Next fetch position should be offset + 1 = 2
        Assert.Equal(2L, consumer.Position(tp0));
    }

    [Fact]
    public void VortexConsumer_CommitSync_TracksOffsets()
    {
        var consumer = new VortexConsumer();
        var tp = new TopicPartition("events", 0);
        consumer.Assign(new List<TopicPartition> { tp });

        var r = new ConsumerRecord { Topic = "events", Partition = 0, Offset = 5L };
        consumer.BufferRecords(new List<ConsumerRecord> { r });
        consumer.Poll(100);

        Assert.Equal(-1L, consumer.Committed(tp));
        consumer.CommitSync();
        Assert.Equal(6L, consumer.Committed(tp));
    }

    [Fact]
    public void VortexConsumer_Seek_ModifiesPosition()
    {
        var consumer = new VortexConsumer();
        var tp = new TopicPartition("events", 0);
        consumer.Assign(new List<TopicPartition> { tp });

        consumer.Seek(tp, 42L);
        Assert.Equal(42L, consumer.Position(tp));
    }

    [Fact]
    public void VortexAdmin_CreateTopic_ListAndDescribe()
    {
        var admin = new VortexAdmin();

        short res1 = admin.CreateTopic("market-ticks", 16, 3);
        Assert.Equal(VortexError.None, res1);

        // Reject duplicate topic
        short resDuplicate = admin.CreateTopic("market-ticks", 16, 3);
        Assert.Equal(VortexError.TopicAlreadyExists, resDuplicate);

        var topics = admin.ListTopics();
        Assert.Contains("market-ticks", topics);

        var desc = admin.DescribeTopic("market-ticks");
        Assert.NotNull(desc);
        Assert.Equal("market-ticks", desc.Name);
        Assert.Equal(16, desc.PartitionCount);
        Assert.Equal(3, desc.ReplicationFactor);
    }

    [Fact]
    public void VortexAdmin_DeleteTopic_RemovesMetadata()
    {
        var admin = new VortexAdmin();
        admin.CreateTopic("temp-stream", 1, 1);

        short del1 = admin.DeleteTopic("temp-stream");
        Assert.Equal(VortexError.None, del1);

        short del2 = admin.DeleteTopic("temp-stream");
        Assert.Equal(VortexError.UnknownTopicOrPartition, del2);
        Assert.DoesNotContain("temp-stream", admin.ListTopics());
    }
}
