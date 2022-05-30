// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.Collections.Generic

// Represents a unique partition identifier within a named topic.
data class TopicPartition(Topic string, Partition int32) {
    func Format() string {
        return Topic + "-" + Partition.ToString()
    }
}

// Logical range of offsets bounded by inclusive start and exclusive end.
data class OffsetRange(StartOffset int64, EndOffset int64) {
    func Length() int64 {
        return EndOffset - StartOffset
    }

    func Contains(offset int64) bool {
        return offset >= StartOffset && offset < EndOffset
    }
}

// Monotonic cluster election or partition leader epoch.
data class Epoch(Value int32) {
    func Next() Epoch {
        return Epoch(Value + 1)
    }

    func IsNewerThan(other Epoch) bool {
        return Value > other.Value
    }
}

// User-defined key-value header metadata attached to a stream record.
data class RecordHeader(Key string, Value []byte) {
    func Size() int32 {
        return Key.Length + Value.Length
    }
}

// Standardized error codes for the Vortex streaming engine.
shared class VortexError {
    let None int16 = 0
    let OffsetOutOfRange int16 = 1
    let CorruptMessage int16 = 2
    let UnknownTopicOrPartition int16 = 3
    let LeaderNotAvailable int16 = 4
    let NotLeaderOrFollower int16 = 5
    let RequestTimedOut int16 = 6
    let RecordTooLarge int16 = 7
    let NetworkException int16 = 8
    let IllegalGeneration int16 = 9
    let UnknownMemberId int16 = 10
    let RebalanceInProgress int16 = 11
    let CommitFailed int16 = 14
    let TopicAlreadyExists int16 = 15
    let SchemaIncompatible int16 = 18

    func Describe(code int16) string {
        if code == 0 { return "OK" }
        if code == 1 { return "Offset out of range" }
        if code == 2 { return "Corrupt message payload" }
        if code == 3 { return "Unknown topic or partition" }
        if code == 4 { return "Leader not available" }
        if code == 5 { return "Not leader or follower for partition" }
        if code == 6 { return "Request timed out" }
        if code == 7 { return "Record size exceeds allowed threshold" }
        if code == 8 { return "Network transmission error" }
        if code == 9 { return "Illegal consumer group generation" }
        if code == 10 { return "Unknown consumer group member id" }
        if code == 11 { return "Consumer group rebalance in progress" }
        if code == 14 { return "Offset commit failed" }
        if code == 15 { return "Topic already exists" }
        if code == 18 { return "Schema incompatibility detected" }
        return "Vortex error (" + code.ToString() + ")"
    }
}
