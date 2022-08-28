// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Coordination

import System
import System.Collections.Generic
import Vortex.Core

// Cooperative sticky assignor preserving existing partition mappings across rebalances
// to eliminate stop-the-world pauses and minimize partition migration churn.
class CooperativeStickyAssignor : IPartitionAssignor {
    var PreviousAssignments Dictionary[string, List[TopicPartition]] = Dictionary[string, List[TopicPartition]]()

    func Name() string {
        return "cooperative-sticky"
    }

    func Assign(memberIds List[string], partitions List[TopicPartition]) Dictionary[string, List[TopicPartition]] {
        var result = Dictionary[string, List[TopicPartition]]()
        if memberIds.Count == 0 || partitions.Count == 0 {
            return result
        }

        var sortedMembers = List[string](memberIds)
        sortedMembers.Sort()

        for m in 0 ... sortedMembers.Count {
            result[sortedMembers[m]] = List[TopicPartition]()
        }

        var unassigned = List[TopicPartition]()
        var assignedMap = Dictionary[string, string]() // Format(tp) -> memberId

        // 1. Retain previous valid assignments for surviving members
        for p in 0 ... partitions.Count {
            var tp = partitions[p]
            var formatStr string = tp.Format()
            var preserved bool = false

            var prevEnumerator = PreviousAssignments.GetEnumerator()
            while prevEnumerator.MoveNext() {
                var prevMember = prevEnumerator.Current.Key
                if sortedMembers.Contains(prevMember) {
                    var prevList = prevEnumerator.Current.Value
                    for k in 0 ... prevList.Count {
                        if prevList[k].Format() == formatStr {
                            result[prevMember].Add(tp)
                            assignedMap[formatStr] = prevMember
                            preserved = true
                            break
                        }
                    }
                }
                if preserved {
                    break
                }
            }

            if !preserved {
                unassigned.Add(tp)
            }
        }

        // 2. Determine target count per consumer
        var targetPerConsumer int32 = partitions.Count / sortedMembers.Count
        var consumersWithExtra int32 = partitions.Count % sortedMembers.Count

        // 3. Balance: revoke from consumers having strictly more than target
        for m in 0 ... sortedMembers.Count {
            var mId string = sortedMembers[m]
            var currentList = result[mId]
            var maxAllowed int32 = targetPerConsumer
            if m < consumersWithExtra {
                maxAllowed = targetPerConsumer + 1
            }

            while currentList.Count > maxAllowed {
                var revoked = currentList[currentList.Count - 1]
                currentList.RemoveAt(currentList.Count - 1)
                unassigned.Add(revoked)
            }
        }

        // 4. Allocate remaining unassigned partitions to members below target
        for u in 0 ... unassigned.Count {
            var tp = unassigned[u]

            // Find member with lowest partition count
            var bestMember string = sortedMembers[0]
            var lowestCount int32 = result[bestMember].Count

            for m in 1 ... sortedMembers.Count {
                var candidateId string = sortedMembers[m]
                if result[candidateId].Count < lowestCount {
                    bestMember = candidateId
                    lowestCount = result[candidateId].Count
                }
            }

            result[bestMember].Add(tp)
        }

        PreviousAssignments = result
        return result
    }
}
