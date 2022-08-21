// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Coordination

import System
import System.Collections.Generic
import Vortex.Core

// Contract for consumer group partition assignment strategies.
interface IPartitionAssignor {
    func Name() string;
    func Assign(memberIds List[string], partitions List[TopicPartition]) Dictionary[string, List[TopicPartition]];
}

// Assigns partitions on a per-topic basis, allocating consecutive partition ranges to each consumer.
class RangeAssignor : IPartitionAssignor {
    func Name() string {
        return "range"
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

        // Group partitions by topic
        var topicMap = Dictionary[string, List[TopicPartition]]()
        for p in 0 ... partitions.Count {
            var tp = partitions[p]
            var list List[TopicPartition]? = nil
            var found bool = topicMap.TryGetValue(tp.Topic, out list)
            if !found || list == nil {
                list = List[TopicPartition]()
                topicMap[tp.Topic] = list
            }
            list.Add(tp)
        }

        var topicEnumerator = topicMap.GetEnumerator()
        while topicEnumerator.MoveNext() {
            var topicPartitions = topicEnumerator.Current.Value
            var numParts int32 = topicPartitions.Count
            var numConsumers int32 = sortedMembers.Count

            var partsPerConsumer int32 = numParts / numConsumers
            var consumersWithExtra int32 = numParts % numConsumers

            for i in 0 ... numConsumers {
                var start int32 = partsPerConsumer * i + Math.Min(i, consumersWithExtra)
                var extra int32 = 0
                if i < consumersWithExtra {
                    extra = 1
                }
                var length int32 = partsPerConsumer + extra

                var memberId string = sortedMembers[i]
                for k in start ... (start + length) {
                    result[memberId].Add(topicPartitions[k])
                }
            }
        }

        return result
    }
}

// Assigns all partitions across all topics in a single interleaved round-robin sequence.
class RoundRobinAssignor : IPartitionAssignor {
    func Name() string {
        return "roundrobin"
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

        for i in 0 ... partitions.Count {
            var memberIndex int32 = i % sortedMembers.Count
            var assignedMember string = sortedMembers[memberIndex]
            result[assignedMember].Add(partitions[i])
        }

        return result
    }
}
