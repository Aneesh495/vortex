// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Coordination

import System
import System.Collections.Concurrent
import System.Collections.Generic
import Vortex.Core

// State phases of a distributed consumer group lifecycle.
shared class GroupState {
    let Empty int32 = 0
    let PreparingRebalance int32 = 1
    let CompletingRebalance int32 = 2
    let Stable int32 = 3
    let Dead int32 = 4
}

// Metadata for a registered consumer group member.
class GroupMember {
    var MemberId string
    var ClientId string = ""
    var ClientHost string = ""
    var SessionTimeoutMs int32 = 10000
    var RebalanceTimeoutMs int32 = 30000
    var ProtocolType string = "consumer"
    var SupportedProtocols List[string] = List[string]()
    var Assignment []byte = [0]byte
    var LastHeartbeatTimestampMs int64 = 0
}

// Managed consumer group tracking members, generation state, and partition sync handoffs.
class ConsumerGroup {
    var GroupId string
    var State int32 = 0 // GroupState.Empty
    var GenerationId int32 = 0
    var LeaderId string = ""
    var Protocol string = "range"
    var Members Dictionary[string, GroupMember] = Dictionary[string, GroupMember]()
    var SyncAssignments Dictionary[string, []byte] = Dictionary[string, []byte]()
    var SyncLock Object = Object()

    func MemberCount() int32 {
        lock SyncLock {
            return Members.Count
        }
    }

    func AddOrUpdateMember(member GroupMember) {
        lock SyncLock {
            Members[member.MemberId] = member
            if LeaderId == "" || !Members.ContainsKey(LeaderId) {
                LeaderId = member.MemberId
            }
        }
    }

    func RemoveMember(memberId string) bool {
        lock SyncLock {
            var removed bool = Members.Remove(memberId)
            SyncAssignments.Remove(memberId)
            if LeaderId == memberId {
                LeaderId = ""
                var enumerator = Members.Keys.GetEnumerator()
                if enumerator.MoveNext() {
                    LeaderId = enumerator.Current
                }
            }
            if Members.Count == 0 {
                State = 0 // Empty
            }
            return removed
        }
    }
}

// Coordinates consumer groups, driving rebalancing rounds and member heartbeats.
class GroupCoordinator {
    var Groups ConcurrentDictionary[string, ConsumerGroup] = ConcurrentDictionary[string, ConsumerGroup]()

    func GetOrCreateGroup(groupId string) ConsumerGroup {
        var group ConsumerGroup? = nil
        var found bool = Groups.TryGetValue(groupId, out group)
        if !found || group == nil {
            group = ConsumerGroup{}
            group.GroupId = groupId
            group.SyncLock = Object()
            Groups[groupId] = group
        }
        return group
    }

    // Handles member join request, generating memberId if empty and updating group state.
    func JoinGroup(groupId string, memberId string, clientId string, sessionTimeoutMs int32, currentTimeMs int64) (string, int32, string, int16) {
        var group = GetOrCreateGroup(groupId)

        lock group.SyncLock {
            var assignedId string = memberId
            if assignedId == "" {
                assignedId = clientId + "-" + Guid.NewGuid().ToString("N").Substring(0, 8)
            }

            var member = GroupMember{}
            member.MemberId = assignedId
            member.ClientId = clientId
            member.SessionTimeoutMs = sessionTimeoutMs
            member.LastHeartbeatTimestampMs = currentTimeMs

            group.AddOrUpdateMember(member)
            group.GenerationId = group.GenerationId + 1
            group.State = 2 // CompletingRebalance

            return (assignedId, group.GenerationId, group.LeaderId, 0)
        }
    }

    // Handles partition assignment synchronization between leader and followers.
    func SyncGroup(groupId string, generationId int32, memberId string, assignments Dictionary[string, []byte]) ([]byte, int16) {
        var group ConsumerGroup? = nil
        var found bool = Groups.TryGetValue(groupId, out group)
        if !found || group == nil {
            return ([0]byte, 3) // UnknownTopicOrPartition / group
        }

        lock group.SyncLock {
            if generationId != group.GenerationId {
                return ([0]byte, 9) // IllegalGeneration
            }

            if memberId == group.LeaderId && assignments != nil {
                var enumerator = assignments.GetEnumerator()
                while enumerator.MoveNext() {
                    var pair = enumerator.Current
                    group.SyncAssignments[pair.Key] = pair.Value
                }
                group.State = 3 // Stable
            }

            var myAssignment []byte = [0]byte
            if group.SyncAssignments.ContainsKey(memberId) {
                myAssignment = group.SyncAssignments[memberId]
            }

            return (myAssignment, 0)
        }
    }

    // Handles periodic heartbeat to maintain member liveness.
    func Heartbeat(groupId string, generationId int32, memberId string, currentTimeMs int64) int16 {
        var group ConsumerGroup? = nil
        var found bool = Groups.TryGetValue(groupId, out group)
        if !found || group == nil {
            return 3 // Unknown group
        }

        lock group.SyncLock {
            if generationId != group.GenerationId {
                return 9 // IllegalGeneration
            }

            var member GroupMember? = nil
            var mFound bool = group.Members.TryGetValue(memberId, out member)
            if !mFound || member == nil {
                return 10 // UnknownMemberId
            }

            member.LastHeartbeatTimestampMs = currentTimeMs
            return 0 // OK
        }
    }

    // Reaps expired members from consumer groups that failed to heartbeat within session timeout.
    func ReapExpiredMembers(currentTimeMs int64) int32 {
        var reapedCount int32 = 0
        var keysEnumerator = Groups.Keys.GetEnumerator()

        while keysEnumerator.MoveNext() {
            var gId string = keysEnumerator.Current
            var group = GetOrCreateGroup(gId)
            lock group.SyncLock {
                var expiredMembers = List[string]()
                var mEnumerator = group.Members.Values.GetEnumerator()

                while mEnumerator.MoveNext() {
                    var m = mEnumerator.Current
                    var elapsed int64 = currentTimeMs - m.LastHeartbeatTimestampMs
                    if elapsed > int64(m.SessionTimeoutMs) {
                        expiredMembers.Add(m.MemberId)
                    }
                }

                for i in 0 ... expiredMembers.Count {
                    group.RemoveMember(expiredMembers[i])
                    reapedCount = reapedCount + 1
                }

                if expiredMembers.Count > 0 && group.Members.Count > 0 {
                    group.GenerationId = group.GenerationId + 1
                    group.State = 1 // PreparingRebalance
                }
            }
        }

        return reapedCount
    }
}
