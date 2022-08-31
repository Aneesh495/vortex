// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Coordination

import System
import System.Collections.Concurrent
import System.Collections.Generic
import Vortex.Core

data class CommittedOffset(Offset int64, Metadata string, CommitTimestampMs int64)

// In-memory transactional offset cache backed by the internal __consumer_offsets commit log.
class OffsetManager {
    var Offsets ConcurrentDictionary[string, CommittedOffset] = ConcurrentDictionary[string, CommittedOffset]()

    func FormatKey(groupId string, topic string, partition int32) string {
        return groupId + "::" + topic + "::" + partition.ToString()
    }

    // Commits an offset position for a consumer group on a specific topic partition.
    func CommitOffset(groupId string, topic string, partition int32, offset int64, metadata string, commitTimestampMs int64) {
        var key string = FormatKey(groupId, topic, partition)
        Offsets[key] = CommittedOffset(offset, metadata, commitTimestampMs)
    }

    // Fetches the latest committed offset for the consumer group. Returns -1 if no offset exists.
    func FetchOffset(groupId string, topic string, partition int32) (int64, string, int16) {
        var key string = FormatKey(groupId, topic, partition)
        var commit CommittedOffset? = nil
        var found bool = Offsets.TryGetValue(key, out commit)
        if found && commit != nil {
            return (commit.Offset, commit.Metadata, 0)
        }
        return (-1, "", 0)
    }

    // Computes consumer lag given the current partition Log End Offset (LEO).
    func ComputeLag(groupId string, topic string, partition int32, partitionLEO int64) int64 {
        var key string = FormatKey(groupId, topic, partition)
        var commit CommittedOffset? = nil
        var found bool = Offsets.TryGetValue(key, out commit)
        if found && commit != nil {
            if partitionLEO >= commit.Offset {
                return partitionLEO - commit.Offset
            }
            return 0
        }
        return partitionLEO
    }
}
