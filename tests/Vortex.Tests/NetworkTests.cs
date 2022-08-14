// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Vortex.Network;
using Xunit;

public class NetworkTests
{
    [Fact]
    public void WireFrame_SerializeAndDeserialize_RoundTripsExactly()
    {
        var frame = new WireFrame();
        frame.ApiKey = 0; // Produce
        frame.ApiVersion = 2;
        frame.CorrelationId = 42;
        frame.ClientId = "console-producer";
        frame.Payload = Encoding.UTF8.GetBytes("test-payload-bytes");

        using var ms = new MemoryStream();
        frame.Serialize(ms);

        ms.Position = 0;
        var recovered = WireFrameCodec.Deserialize(ms);

        Assert.Equal(frame.ApiKey, recovered.ApiKey);
        Assert.Equal(frame.ApiVersion, recovered.ApiVersion);
        Assert.Equal(frame.CorrelationId, recovered.CorrelationId);
        Assert.Equal(frame.ClientId, recovered.ClientId);
        Assert.Equal("test-payload-bytes", Encoding.UTF8.GetString(recovered.Payload));
    }

    [Fact]
    public void WireFrame_CorruptedChecksum_ThrowsInvalidDataException()
    {
        var frame = new WireFrame();
        frame.ApiKey = 1;
        frame.ApiVersion = 1;
        frame.CorrelationId = 100;
        frame.ClientId = "client-a";
        frame.Payload = Encoding.UTF8.GetBytes("important-payload");

        using var ms = new MemoryStream();
        frame.Serialize(ms);

        byte[] raw = ms.ToArray();
        // Tamper with one byte in the payload
        raw[raw.Length - 5] ^= 0x55;

        using var corruptedMs = new MemoryStream(raw);
        Assert.Throws<InvalidDataException>(() => WireFrameCodec.Deserialize(corruptedMs));
    }

    [Fact]
    public void ProduceCodec_RoundTripsPayload()
    {
        var req = new ProduceRequest();
        req.Acks = -1;
        req.TimeoutMs = 15000;

        var partData = new List<PartitionProduceData>
        {
            new PartitionProduceData(0, Encoding.UTF8.GetBytes("record-data-p0")),
            new PartitionProduceData(1, Encoding.UTF8.GetBytes("record-data-p1"))
        };
        req.Topics.Add(new TopicProduceData("telemetry", partData));

        byte[] bytes = req.Encode();
        var recovered = ProduceCodec.DecodeRequest(bytes);

        Assert.Equal(req.Acks, recovered.Acks);
        Assert.Equal(req.TimeoutMs, recovered.TimeoutMs);
        Assert.Single(recovered.Topics);
        Assert.Equal("telemetry", recovered.Topics[0].Topic);
        Assert.Equal(2, recovered.Topics[0].Partitions.Count);
        Assert.Equal("record-data-p0", Encoding.UTF8.GetString(recovered.Topics[0].Partitions[0].RecordBytes));
    }

    [Fact]
    public void MetadataCodec_RoundTripsClusterView()
    {
        var resp = new MetadataResponse();
        resp.ClusterId = "vortex-cluster-west";
        resp.ControllerId = 1;
        resp.Brokers.Add(new BrokerMetadata(1, "10.0.0.1", 9092, "rack-a"));
        resp.Brokers.Add(new BrokerMetadata(2, "10.0.0.2", 9092, "rack-b"));

        var pm = new PartitionMetadata(0, 1, 3, new List<int> { 1, 2 }, new List<int> { 1, 2 }, 0);
        resp.Topics.Add(new TopicMetadata("orders", false, 0, new List<PartitionMetadata> { pm }));

        byte[] encoded = resp.Encode();
        var decoded = MetadataCodec.DecodeResponse(encoded);

        Assert.Equal("vortex-cluster-west", decoded.ClusterId);
        Assert.Equal(1, decoded.ControllerId);
        Assert.Equal(2, decoded.Brokers.Count);
        Assert.Single(decoded.Topics);
        Assert.Equal("orders", decoded.Topics[0].Topic);
        Assert.Equal(1, decoded.Topics[0].Partitions[0].LeaderId);
    }

    [Fact]
    public void GroupCodec_JoinAndSync_RoundTrips()
    {
        var joinReq = new JoinGroupRequest();
        joinReq.GroupId = "payment-processors";
        joinReq.SessionTimeoutMs = 10000;
        joinReq.MemberId = "member-alpha";
        joinReq.GroupProtocols.Add(new GroupProtocolMetadata("roundrobin", Encoding.UTF8.GetBytes("proto-data")));

        byte[] encodedJoin = joinReq.Encode();
        var decodedJoin = GroupCodec.DecodeJoinRequest(encodedJoin);

        Assert.Equal("payment-processors", decodedJoin.GroupId);
        Assert.Equal("member-alpha", decodedJoin.MemberId);
        Assert.Single(decodedJoin.GroupProtocols);
        Assert.Equal("roundrobin", decodedJoin.GroupProtocols[0].Name);

        var syncReq = new SyncGroupRequest();
        syncReq.GroupId = "payment-processors";
        syncReq.GenerationId = 1;
        syncReq.MemberId = "member-alpha";
        syncReq.GroupAssignments.Add(new GroupAssignment("member-alpha", Encoding.UTF8.GetBytes("orders-0,orders-1")));

        byte[] encodedSync = syncReq.Encode();
        var decodedSync = GroupCodec.DecodeSyncRequest(encodedSync);

        Assert.Equal("payment-processors", decodedSync.GroupId);
        Assert.Single(decodedSync.GroupAssignments);
        Assert.Equal("orders-0,orders-1", Encoding.UTF8.GetString(decodedSync.GroupAssignments[0].Assignment));
    }

    [Fact]
    public void ConnectionManager_RateLimiter_EnforcesLimit()
    {
        var mgr = new ConnectionManager();
        var ctx = mgr.Register(101, "127.0.0.1:54321", 1000);
        ctx.Tokens = 2.0; // Limit to 2 tokens
        ctx.MaxTokens = 2.0;
        ctx.RefillRatePerSecond = 0.0; // No refill for instant test

        bool r1 = mgr.RecordActivity(101, 100, 1000);
        bool r2 = mgr.RecordActivity(101, 100, 1000);
        bool r3 = mgr.RecordActivity(101, 100, 1000); // Exceeds token capacity

        Assert.True(r1);
        Assert.True(r2);
        Assert.False(r3); // Rejected by rate limiter
    }
}
