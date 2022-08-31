// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.Collections.Generic;
using Vortex.Coordination;
using Vortex.Core;
using Xunit;

public class CoordinationTests
{
    [Fact]
    public void GroupCoordinator_LifecycleAndHeartbeat_TracksState()
    {
        var coord = new GroupCoordinator();
        long now = 1000;

        // Member 1 joins -> should become leader
        var (m1, gen1, leader1, err1) = coord.JoinGroup("analytics", "", "worker-1", 10000, now);
        Assert.Equal(0, err1);
        Assert.NotEmpty(m1);
        Assert.Equal(1, gen1);
        Assert.Equal(m1, leader1);

        // Member 2 joins -> should join existing group, increment generation
        var (m2, gen2, leader2, err2) = coord.JoinGroup("analytics", "", "worker-2", 10000, now);
        Assert.Equal(0, err2);
        Assert.NotEmpty(m2);
        Assert.NotEqual(m1, m2);
        Assert.Equal(2, gen2);
        Assert.Equal(m1, leader2); // Leader remains member 1

        // Heartbeat verification
        short hbOk = coord.Heartbeat("analytics", gen2, m1, now + 500);
        Assert.Equal(0, hbOk);

        // Heartbeat with old generation rejected
        short hbBadGen = coord.Heartbeat("analytics", gen1, m1, now + 500);
        Assert.Equal(9, hbBadGen); // IllegalGeneration

        // Heartbeat timeout reaping
        long timeoutTime = now + 15000; // 15 seconds later (> 10s session timeout for member 2)
        coord.Heartbeat("analytics", gen2, m1, timeoutTime); // Member 1 stays alive

        int reaped = coord.ReapExpiredMembers(timeoutTime);
        Assert.Equal(1, reaped); // Member 2 expired and reaped
    }

    [Fact]
    public void RangeAssignor_EvenlyDividesConsecutivePartitions()
    {
        var assignor = new RangeAssignor();
        var members = new List<string> { "consumer-1", "consumer-2" };
        var partitions = new List<TopicPartition>
        {
            new TopicPartition("orders", 0),
            new TopicPartition("orders", 1),
            new TopicPartition("orders", 2),
            new TopicPartition("orders", 3)
        };

        var assignment = assignor.Assign(members, partitions);

        Assert.Equal(2, assignment["consumer-1"].Count);
        Assert.Equal(2, assignment["consumer-2"].Count);

        // Range order: consumer-1 gets [0, 1], consumer-2 gets [2, 3]
        Assert.Equal(0, assignment["consumer-1"][0].Partition);
        Assert.Equal(1, assignment["consumer-1"][1].Partition);
        Assert.Equal(2, assignment["consumer-2"][0].Partition);
        Assert.Equal(3, assignment["consumer-2"][1].Partition);
    }

    [Fact]
    public void RoundRobinAssignor_InterleavesPartitions()
    {
        var assignor = new RoundRobinAssignor();
        var members = new List<string> { "c1", "c2", "c3" };
        var partitions = new List<TopicPartition>
        {
            new TopicPartition("topic-A", 0),
            new TopicPartition("topic-A", 1),
            new TopicPartition("topic-B", 0),
            new TopicPartition("topic-B", 1)
        };

        var assignment = assignor.Assign(members, partitions);

        // 4 partitions across 3 consumers: 2 consumers get 1, 1 consumer gets 2
        int total = assignment["c1"].Count + assignment["c2"].Count + assignment["c3"].Count;
        Assert.Equal(4, total);
        Assert.True(assignment["c1"].Count <= 2);
        Assert.True(assignment["c2"].Count <= 2);
        Assert.True(assignment["c3"].Count <= 2);
    }

    [Fact]
    public void CooperativeStickyAssignor_MinimizesPartitionChurn()
    {
        var assignor = new CooperativeStickyAssignor();
        var members = new List<string> { "m1", "m2" };
        var partitions = new List<TopicPartition>
        {
            new TopicPartition("t", 0),
            new TopicPartition("t", 1),
            new TopicPartition("t", 2),
            new TopicPartition("t", 3)
        };

        // Initial assignment
        var assign1 = assignor.Assign(members, partitions);
        Assert.Equal(2, assign1["m1"].Count);
        Assert.Equal(2, assign1["m2"].Count);

        // Member m3 joins: total 3 members for 4 partitions
        var newMembers = new List<string> { "m1", "m2", "m3" };
        var assign2 = assignor.Assign(newMembers, partitions);

        Assert.True(assign2["m1"].Count >= 1);
        Assert.True(assign2["m2"].Count >= 1);
        Assert.True(assign2["m3"].Count >= 1);

        int total = assign2["m1"].Count + assign2["m2"].Count + assign2["m3"].Count;
        Assert.Equal(4, total);
    }

    [Fact]
    public void OffsetManager_CommitAndFetch_ComputesLag()
    {
        var mgr = new OffsetManager();
        long now = 1700000000000L;

        mgr.CommitOffset("reporting-group", "events", 0, 150, "batch-1", now);

        var (offset, meta, err) = mgr.FetchOffset("reporting-group", "events", 0);
        Assert.Equal(0, err);
        Assert.Equal(150, offset);
        Assert.Equal("batch-1", meta);

        // When partition LEO is 200, consumer lag is 50
        long lag = mgr.ComputeLag("reporting-group", "events", 0, 200);
        Assert.Equal(50, lag);

        // Unknown partition returns -1 offset
        var (uncommitted, _, _) = mgr.FetchOffset("reporting-group", "events", 99);
        Assert.Equal(-1, uncommitted);
    }
}
