// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Replication

import System
import System.Collections.Generic

// Role of a node within the Raft consensus cluster.
shared class RaftRole {
    let Follower int32 = 0
    let Candidate int32 = 1
    let Leader int32 = 2

    func Describe(role int32) string {
        if role == 0 { return "Follower" }
        if role == 1 { return "Candidate" }
        if role == 2 { return "Leader" }
        return "Unknown"
    }
}

// An individual command entry stored within the replicated Raft consensus log.
data class RaftLogEntry(Term int64, Index int64, Command []byte)

// Invocation arguments for the Raft RequestVote RPC.
class RequestVoteArgs {
    var Term int64
    var CandidateId int32
    var LastLogIndex int64
    var LastLogTerm int64
}

// Result returned by a node responding to a RequestVote RPC.
data class RequestVoteResult(Term int64, VoteGranted bool)

// Invocation arguments for the Raft AppendEntries (heartbeat / log replication) RPC.
class AppendEntriesArgs {
    var Term int64
    var LeaderId int32
    var PrevLogIndex int64
    var PrevLogTerm int64
    var Entries List[RaftLogEntry] = List[RaftLogEntry]()
    var LeaderCommit int64
}

// Result returned by a node responding to an AppendEntries RPC.
data class AppendEntriesResult(Term int64, Success bool, MatchIndex int64)
