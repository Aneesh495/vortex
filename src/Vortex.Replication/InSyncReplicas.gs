// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Replication

import System
import System.Collections.Generic

// Runtime replication metrics for a single replica tracking the partition leader.
class ReplicaState {
    var ReplicaId int32
    var LogEndOffset int64 = 0
    var LastFetchTimestampMs int64 = 0
    var LastCaughtUpTimestampMs int64 = 0
}

// In-Sync Replicas (ISR) state coordinator managing High Watermark calculation
// and dynamic ISR expansion and contraction according to replica lag deadlines.
class PartitionReplicaCoordinator {
    var Topic string
    var Partition int32
    var LeaderId int32
    var LeaderEpoch int32
    var MaxLagMs int64 = 10000 // 10 second maximum lag threshold
    var AssignedReplicas List[int32] = List[int32]()
    var Isr List[int32] = List[int32]()
    var Replicas Dictionary[int32, ReplicaState] = Dictionary[int32, ReplicaState]()
    var HighWatermark int64 = 0
    var SyncLock Object = Object()

    // Records the progress of a follower replica fetching records from the leader.
    func RecordFollowerProgress(replicaId int32, followerLEO int64, currentTimeMs int64) int64 {
        lock SyncLock {
            var state ReplicaState? = nil
            var found bool = Replicas.TryGetValue(replicaId, out state)
            if !found || state == nil {
                state = ReplicaState{}
                state.ReplicaId = replicaId
                Replicas[replicaId] = state
            }

            state.LogEndOffset = followerLEO
            state.LastFetchTimestampMs = currentTimeMs

            var leaderState = Replicas[LeaderId]
            var leaderLEO int64 = 0
            if leaderState != nil {
                leaderLEO = leaderState.LogEndOffset
            }

            // Follower caught up: update caught up timestamp and expand ISR if needed
            if followerLEO >= leaderLEO {
                state.LastCaughtUpTimestampMs = currentTimeMs
                if !Isr.Contains(replicaId) {
                    Isr.Add(replicaId)
                }
            }

            // Recalculate High Watermark across all ISR members
            var newHW int64 = leaderLEO
            for i in 0 ... Isr.Count {
                var memberId int32 = Isr[i]
                var mState = Replicas[memberId]
                if mState != nil && mState.LogEndOffset < newHW {
                    newHW = mState.LogEndOffset
                }
            }

            if newHW > HighWatermark {
                HighWatermark = newHW
            }

            return HighWatermark
        }
    }

    // Updates the leader's own Log End Offset when a local batch append occurs.
    func RecordLeaderLEO(newLeaderLEO int64, currentTimeMs int64) int64 {
        lock SyncLock {
            var leaderState = Replicas[LeaderId]
            if leaderState == nil {
                leaderState = ReplicaState{}
                leaderState.ReplicaId = LeaderId
                Replicas[LeaderId] = leaderState
            }

            leaderState.LogEndOffset = newLeaderLEO
            leaderState.LastFetchTimestampMs = currentTimeMs
            leaderState.LastCaughtUpTimestampMs = currentTimeMs

            // Single node cluster or 1-replica partition: HW advances immediately with leader LEO
            if Isr.Count == 1 && Isr[0] == LeaderId {
                HighWatermark = newLeaderLEO
                return HighWatermark
            }

            // Multi-replica: compute minimum LEO of all ISR members
            var newHW int64 = newLeaderLEO
            for i in 0 ... Isr.Count {
                var memberId int32 = Isr[i]
                var mState = Replicas[memberId]
                if mState != nil && mState.LogEndOffset < newHW {
                    newHW = mState.LogEndOffset
                }
            }

            if newHW > HighWatermark {
                HighWatermark = newHW
            }

            return HighWatermark
        }
    }

    // Identifies and evicts lagging replicas from the ISR whose lag exceeds MaxLagMs.
    func CheckShrinkIsr(currentTimeMs int64) List[int32] {
        lock SyncLock {
            var evicted = List[int32]()
            var retained = List[int32]()

            for i in 0 ... Isr.Count {
                var replicaId int32 = Isr[i]
                if replicaId == LeaderId {
                    retained.Add(replicaId)
                    continue
                }

                var state = Replicas[replicaId]
                if state != nil {
                    var lag int64 = currentTimeMs - state.LastCaughtUpTimestampMs
                    if lag > MaxLagMs {
                        evicted.Add(replicaId)
                    } else {
                        retained.Add(replicaId)
                    }
                } else {
                    evicted.Add(replicaId)
                }
            }

            if evicted.Count > 0 {
                Isr = retained
            }

            return evicted
        }
    }

    func IsrCount() int32 {
        lock SyncLock {
            return Isr.Count
        }
    }

    func HighWatermarkOffset() int64 {
        lock SyncLock {
            return HighWatermark
        }
    }
}
