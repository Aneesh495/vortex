// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.Collections.Generic;
using System.Text;
using Vortex.Replication;
using Xunit;

public class ReplicationTests
{
    [Fact]
    public void FailureDetector_RegularHeartbeats_YieldsLowPhi()
    {
        var fd = new PhiAccrualFailureDetector();
        long now = 1000;

        for (int i = 0; i < 20; i++)
        {
            fd.Heartbeat(now);
            now += 100; // Exact 100ms heartbeat interval
        }

        double phiAt50ms = fd.Phi(now + 50);
        Assert.True(phiAt50ms < 2.0);
        Assert.True(fd.IsAvailable(now + 50, 8.0));

        // When elapsed time is 15x normal interval
        double phiAt1500ms = fd.Phi(now + 1500);
        Assert.True(phiAt1500ms > 8.0);
        Assert.False(fd.IsAvailable(now + 1500, 8.0));
    }

    [Fact]
    public void ISR_HighWatermark_AdvancesWithFollowerProgress()
    {
        var coord = new PartitionReplicaCoordinator();
        coord.Topic = "events";
        coord.Partition = 0;
        coord.LeaderId = 1;
        coord.LeaderEpoch = 1;
        coord.AssignedReplicas.Add(1);
        coord.AssignedReplicas.Add(2);
        coord.Isr.Add(1);
        coord.Isr.Add(2);

        long time = 1000;

        // Leader writes up to LEO = 100
        coord.RecordLeaderLEO(100, time);
        // High watermark should still be 0 since follower 2 is at 0
        Assert.Equal(0, coord.HighWatermarkOffset());

        // Follower 2 fetches up to offset 60
        coord.RecordFollowerProgress(2, 60, time);
        Assert.Equal(60, coord.HighWatermarkOffset());

        // Follower 2 catches up to offset 100
        coord.RecordFollowerProgress(2, 100, time);
        Assert.Equal(100, coord.HighWatermarkOffset());
    }

    [Fact]
    public void ISR_LaggingFollower_EvictedPastDeadline()
    {
        var coord = new PartitionReplicaCoordinator();
        coord.Topic = "events";
        coord.Partition = 0;
        coord.LeaderId = 1;
        coord.MaxLagMs = 5000;
        coord.AssignedReplicas.Add(1);
        coord.AssignedReplicas.Add(2);
        coord.Isr.Add(1);
        coord.Isr.Add(2);

        long time = 1000;
        coord.RecordLeaderLEO(50, time);
        coord.RecordFollowerProgress(2, 50, time);
        Assert.Equal(2, coord.IsrCount());

        // Advance time by 6000ms without follower updates
        time += 6000;
        var evicted = coord.CheckShrinkIsr(time);

        Assert.Single(evicted);
        Assert.Equal(2, evicted[0]);
        Assert.Equal(1, coord.IsrCount()); // Only leader remains in ISR
    }

    [Fact]
    public void Raft_ElectionAndReplication_ProgressesCommitIndex()
    {
        var nodes = new List<int> { 1, 2, 3 };

        var node1 = new RaftCoordinator { NodeId = 1, ClusterNodes = nodes };
        var node2 = new RaftCoordinator { NodeId = 2, ClusterNodes = nodes };
        var node3 = new RaftCoordinator { NodeId = 3, ClusterNodes = nodes };

        // Node 1 starts election
        var voteArgs = node1.StartElection();
        Assert.Equal(1, node1.CurrentTerm);

        var r2 = node2.HandleRequestVote(voteArgs);
        var r3 = node3.HandleRequestVote(voteArgs);

        Assert.True(r2.VoteGranted);
        Assert.True(r3.VoteGranted);

        // Node 1 receives 3 votes (including self)
        bool becameLeader = node1.EvaluateVotes(3);
        Assert.True(becameLeader);
        Assert.Equal(2, node1.Role); // Leader

        // Leader proposes command
        byte[] cmd = Encoding.UTF8.GetBytes("create-topic:orders");
        var entry = node1.ProposeCommand(cmd);
        Assert.Equal(1, entry.Index);

        // Replicate to Node 2
        var appendArgs = new AppendEntriesArgs
        {
            Term = node1.CurrentTerm,
            LeaderId = node1.NodeId,
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            Entries = new List<RaftLogEntry> { entry },
            LeaderCommit = node1.CommitIndex
        };

        var appResult = node2.HandleAppendEntries(appendArgs);
        Assert.True(appResult.Success);

        // Leader marks Node 2 match index
        node1.MatchIndex[2] = appResult.MatchIndex;

        // Advance commit index with quorum (Node 1 and Node 2)
        long newCommit = node1.AdvanceCommitIndex();
        Assert.Equal(1, newCommit);
    }
}
