// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Processing

import System

// Encapsulates a streaming event or watermark punctuation moving through a pipeline.
class StreamEvent[K, V] {
    var Key K
    var Value V
    var Timestamp int64
    var Offset int64
    var IsWatermark bool = false
    var WatermarkTimestamp int64 = 0
}

// Factory helper for constructing StreamEvent instances.
shared class StreamEventFactory {
    func FromRecord[K, V](key K, value V, timestamp int64, offset int64) StreamEvent[K, V] {
        var ev = StreamEvent[K, V]{}
        ev.Key = key
        ev.Value = value
        ev.Timestamp = timestamp
        ev.Offset = offset
        ev.IsWatermark = false
        return ev
    }

    func CreateWatermark[K, V](watermarkTimestamp int64) StreamEvent[K, V] {
        var ev = StreamEvent[K, V]{}
        ev.IsWatermark = true
        ev.WatermarkTimestamp = watermarkTimestamp
        return ev
    }
}

// Bounded out-of-orderness watermark generator tracking stream event-time progression.
class BoundedOutOfOrdernessGenerator {
    var MaxOutOfOrdernessMs int64
    var CurrentMaxTimestamp int64 = 0
    var LastEmittedWatermark int64 = 0

    // Observes an incoming event timestamp and returns an updated watermark if it advances.
    func OnEvent(timestamp int64) (int64, bool) {
        if timestamp > CurrentMaxTimestamp {
            CurrentMaxTimestamp = timestamp
        }

        var candidate int64 = CurrentMaxTimestamp - MaxOutOfOrdernessMs
        if candidate > LastEmittedWatermark {
            LastEmittedWatermark = candidate
            return (candidate, true)
        }

        return (LastEmittedWatermark, false)
    }

    func CurrentWatermark() int64 {
        return LastEmittedWatermark
    }
}
