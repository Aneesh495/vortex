// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Replication

import System
import System.Collections.Generic

// Full Raft consensus state machine coordinating cluster metadata and partition leadership.
class RaftCoordinator {
    var NodeId int32
    var CurrentTerm int64 = 0
    var VotedFor int32 = -1
    var Role int32 = 0 // RaftRole.Follower
    var Log List[RaftLogEntry] = List[RaftLogEntry]()
    var CommitIndex int64 = 0
    var LastApplied int64 = 0
    var ClusterNodes List[int32] = List[int32]()
    var NextIndex Dictionary[int32, int64] = Dictionary[int32, int64]()
    var MatchIndex Dictionary[int32, int64] = Dictionary[int32, int64]()
    var SyncLock Object = Object()

    func LastLogIndex() int64 {
        if Log.Count == 0 {
            return 0
        }
        return Log[Log.Count - 1].Index
    }

    func LastLogTerm() int64 {
        if Log.Count == 0 {
            return 0
        }
        return Log[Log.Count - 1].Term
    }

    // Handles incoming RequestVote RPC from a candidate node.
    func HandleRequestVote(args RequestVoteArgs) RequestVoteResult {
        lock SyncLock {
            // 1. Reply false if term < currentTerm
            if args.Term < CurrentTerm {
                return RequestVoteResult(CurrentTerm, false)
            }

            // 2. If term > currentTerm, become follower and update term
            if args.Term > CurrentTerm {
                CurrentTerm = args.Term
                Role = 0 // Follower
                VotedFor = -1
            }

            // 3. Check if vote can be granted
            var canVote bool = (VotedFor == -1 || VotedFor == args.CandidateId)
            var logOk bool = false

            var localLastTerm int64 = LastLogTerm()
            var localLastIndex int64 = LastLogIndex()

            if args.LastLogTerm > localLastTerm {
                logOk = true
            } else if args.LastLogTerm == localLastTerm && args.LastLogIndex >= localLastIndex {
                logOk = true
            }

            if canVote && logOk {
                VotedFor = args.CandidateId
                return RequestVoteResult(CurrentTerm, true)
            }

            return RequestVoteResult(CurrentTerm, false)
        }
    }

    // Handles incoming AppendEntries RPC (heartbeat or log replication) from the leader.
    func HandleAppendEntries(args AppendEntriesArgs) AppendEntriesResult {
        lock SyncLock {
            // 1. Reply false if term < currentTerm
            if args.Term < CurrentTerm {
                return AppendEntriesResult(CurrentTerm, false, 0)
            }

            // 2. If term >= currentTerm, recognize leader and revert to follower
            if args.Term > CurrentTerm || Role != 0 {
                CurrentTerm = args.Term
                Role = 0 // Follower
                VotedFor = args.LeaderId
            }

            // 3. Verify log matching at prevLogIndex
            if args.PrevLogIndex > 0 {
                if args.PrevLogIndex > LastLogIndex() {
                    return AppendEntriesResult(CurrentTerm, false, LastLogIndex())
                }
                var existingTerm int64 = Log[int32(args.PrevLogIndex - 1)].Term
                if existingTerm != args.PrevLogTerm {
                    // Truncate conflicting log entries
                    var trimCount int32 = int32(Log.Count) - int32(args.PrevLogIndex - 1)
                    while trimCount > 0 {
                        Log.RemoveAt(Log.Count - 1)
                        trimCount = trimCount - 1
                    }
                    return AppendEntriesResult(CurrentTerm, false, LastLogIndex())
                }
            }

            // 4. Append any new entries not already in the log
            for i in 0 ... args.Entries.Count {
                var entry = args.Entries[i]
                var entryIdx int32 = int32(entry.Index - 1)
                if entryIdx < Log.Count {
                    if Log[entryIdx].Term != entry.Term {
                        // Conflict found: trim rest of log and add
                        while Log.Count > entryIdx {
                            Log.RemoveAt(Log.Count - 1)
                        }
                        Log.Add(entry)
                    }
                } else {
                    Log.Add(entry)
                }
            }

            // 5. Update commit index if leaderCommit > commitIndex
            if args.LeaderCommit > CommitIndex {
                var newCommit int64 = args.LeaderCommit
                var lastIdx int64 = LastLogIndex()
                if lastIdx < newCommit {
                    newCommit = lastIdx
                }
                CommitIndex = newCommit
            }

            return AppendEntriesResult(CurrentTerm, true, LastLogIndex())
        }
    }

    // Initiates a new election round to become cluster leader.
    func StartElection() RequestVoteArgs {
        lock SyncLock {
            Role = 1 // Candidate
            CurrentTerm = CurrentTerm + 1
            VotedFor = NodeId

            var args = RequestVoteArgs{}
            args.Term = CurrentTerm
            args.CandidateId = NodeId
            args.LastLogIndex = LastLogIndex()
            args.LastLogTerm = LastLogTerm()
            return args
        }
    }

    // Evaluates collected votes from peers. Transitions to leader if quorum attained.
    func EvaluateVotes(votesGranted int32) bool {
        lock SyncLock {
            if Role != 1 {
                return false
            }

            var quorum int32 = (ClusterNodes.Count / 2) + 1
            if votesGranted >= quorum {
                Role = 2 // Leader

                // Initialize leader replication state for all peers
                var lastIdx int64 = LastLogIndex()
                for i in 0 ... ClusterNodes.Count {
                    var peerId int32 = ClusterNodes[i]
                    if peerId != NodeId {
                        NextIndex[peerId] = lastIdx + 1
                        MatchIndex[peerId] = 0
                    }
                }
                return true
            }

            return false
        }
    }

    // Appends a new metadata command to the local leader log.
    func ProposeCommand(command []byte) RaftLogEntry {
        lock SyncLock {
            if Role != 2 {
                throw InvalidOperationException("Cannot propose command: node is not the leader")
            }

            var newIndex int64 = LastLogIndex() + 1
            var entry = RaftLogEntry(CurrentTerm, newIndex, command)
            Log.Add(entry)
            return entry
        }
    }

    // Advances the commit index based on acknowledged matchIndex from quorum of peers.
    func AdvanceCommitIndex() int64 {
        lock SyncLock {
            if Role != 2 {
                return CommitIndex
            }

            var quorum int32 = (ClusterNodes.Count / 2) + 1
            var lastIdx int64 = LastLogIndex()

            var startN int32 = int32(CommitIndex + 1)
            var endN int32 = int32(lastIdx + 1)

            for n in startN ... endN {
                var n64 int64 = int64(n)
                var count int32 = 1 // Count leader's own match
                for i in 0 ... ClusterNodes.Count {
                    var peerId int32 = ClusterNodes[i]
                    if peerId != NodeId {
                        var mIdx int64 = 0
                        MatchIndex.TryGetValue(peerId, out mIdx)
                        if mIdx >= n64 {
                            count = count + 1
                        }
                    }
                }

                if count >= quorum && Log[n - 1].Term == CurrentTerm {
                    CommitIndex = n64
                }
            }

            return CommitIndex
        }
    }
}
