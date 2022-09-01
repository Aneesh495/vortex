// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Processing

import System
import System.Collections.Generic

// Represents a discrete time interval [Start, End).
class TimeWindow {
    var Start int64
    var End int64

    func Contains(timestamp int64) bool {
        return timestamp >= Start && timestamp < End
    }
}

// Emitted outcome of a window evaluation.
class WindowResult[K, TResult] {
    var Key K
    var WindowStart int64
    var WindowEnd int64
    var Result TResult
}

// Interface for incremental aggregations over a windowed collection.
interface IWindowAggregator[V, TAcc, TResult] {
    func Initial() TAcc;
    func Aggregate(accum TAcc, value V) TAcc;
    func Emit(accum TAcc) TResult;
}

// Fixed-size, non-overlapping tumbling window operator triggered by watermarks.
class TumblingWindowOperator[K, V, TAcc, TResult] {
    var WindowSizeMs int64
    var Aggregator IWindowAggregator[V, TAcc, TResult]
    var CurrentWatermark int64 = 0
    var LateEventCount int64 = 0

    var Windows Dictionary[int64, List[StreamEvent[K, V]]] = Dictionary[int64, List[StreamEvent[K, V]]]()
    var ActiveStarts List[int64] = List[int64]()

    // Assigns an event to its tumbling window bucket, or drops it if late.
    func ProcessEvent(ev StreamEvent[K, V]) bool {
        if ev.Timestamp < CurrentWatermark {
            LateEventCount = LateEventCount + 1
            return false
        }

        var winStart int64 = (ev.Timestamp / WindowSizeMs) * WindowSizeMs
        var bucket List[StreamEvent[K, V]]? = nil
        var found bool = Windows.TryGetValue(winStart, out bucket)
        if !found || bucket == nil {
            bucket = List[StreamEvent[K, V]]()
            Windows[winStart] = bucket
            ActiveStarts.Add(winStart)
        }

        bucket.Add(ev)
        return true
    }

    // Advances watermark and returns all triggered window results.
    func OnWatermark(watermarkTimestamp int64) List[WindowResult[K, TResult]] {
        CurrentWatermark = watermarkTimestamp
        var triggered = List[WindowResult[K, TResult]]()
        var remainingStarts = List[int64]()

        for i in 0 ... ActiveStarts.Count {
            var winStart int64 = ActiveStarts[i]
            var winEnd int64 = winStart + WindowSizeMs

            if winEnd <= CurrentWatermark {
                var events List[StreamEvent[K, V]]? = nil
                var found bool = Windows.TryGetValue(winStart, out events)
                if found && events != nil {
                    var keyList = List[K]()
                    var keyBuckets = Dictionary[K, List[V]]()

                    for eIdx in 0 ... events.Count {
                        var e = events[eIdx]
                        var valList List[V]? = nil
                        var kFound bool = keyBuckets.TryGetValue(e.Key, out valList)
                        if !kFound || valList == nil {
                            valList = List[V]()
                            keyBuckets[e.Key] = valList
                            keyList.Add(e.Key)
                        }
                        valList.Add(e.Value)
                    }

                    for kIdx in 0 ... keyList.Count {
                        var k = keyList[kIdx]
                        var vals = keyBuckets[k]
                        var acc = Aggregator.Initial()
                        for vIdx in 0 ... vals.Count {
                            acc = Aggregator.Aggregate(acc, vals[vIdx])
                        }

                        var res = WindowResult[K, TResult]()
                        res.Key = k
                        res.WindowStart = winStart
                        res.WindowEnd = winEnd
                        res.Result = Aggregator.Emit(acc)
                        triggered.Add(res)
                    }

                    Windows.Remove(winStart)
                }
            } else {
                remainingStarts.Add(winStart)
            }
        }

        ActiveStarts = remainingStarts
        return triggered
    }
}

// Sliding window operator supporting overlapping fixed-duration windows.
class SlidingWindowOperator[K, V, TAcc, TResult] {
    var WindowSizeMs int64
    var SlideMs int64
    var Aggregator IWindowAggregator[V, TAcc, TResult]
    var CurrentWatermark int64 = 0
    var LateEventCount int64 = 0

    var Windows Dictionary[int64, List[StreamEvent[K, V]]] = Dictionary[int64, List[StreamEvent[K, V]]]()
    var ActiveStarts List[int64] = List[int64]()

    // Assigns an event to all overlapping sliding window buckets.
    func ProcessEvent(ev StreamEvent[K, V]) bool {
        if ev.Timestamp < CurrentWatermark {
            LateEventCount = LateEventCount + 1
            return false
        }

        var lastStart int64 = (ev.Timestamp / SlideMs) * SlideMs
        var firstStart int64 = lastStart - WindowSizeMs + SlideMs
        if firstStart < 0 {
            firstStart = 0
        }

        var curStart int64 = firstStart
        while curStart <= lastStart {
            var winEnd int64 = curStart + WindowSizeMs
            if ev.Timestamp >= curStart && ev.Timestamp < winEnd {
                var bucket List[StreamEvent[K, V]]? = nil
                var found bool = Windows.TryGetValue(curStart, out bucket)
                if !found || bucket == nil {
                    bucket = List[StreamEvent[K, V]]()
                    Windows[curStart] = bucket
                    ActiveStarts.Add(curStart)
                }
                bucket.Add(ev)
            }
            curStart = curStart + SlideMs
        }

        return true
    }

    // Advances watermark and flushes ready sliding windows.
    func OnWatermark(watermarkTimestamp int64) List[WindowResult[K, TResult]] {
        CurrentWatermark = watermarkTimestamp
        var triggered = List[WindowResult[K, TResult]]()
        var remainingStarts = List[int64]()

        for i in 0 ... ActiveStarts.Count {
            var winStart int64 = ActiveStarts[i]
            var winEnd int64 = winStart + WindowSizeMs

            if winEnd <= CurrentWatermark {
                var events List[StreamEvent[K, V]]? = nil
                var found bool = Windows.TryGetValue(winStart, out events)
                if found && events != nil {
                    var keyList = List[K]()
                    var keyBuckets = Dictionary[K, List[V]]()

                    for eIdx in 0 ... events.Count {
                        var e = events[eIdx]
                        var valList List[V]? = nil
                        var kFound bool = keyBuckets.TryGetValue(e.Key, out valList)
                        if !kFound || valList == nil {
                            valList = List[V]()
                            keyBuckets[e.Key] = valList
                            keyList.Add(e.Key)
                        }
                        valList.Add(e.Value)
                    }

                    for kIdx in 0 ... keyList.Count {
                        var k = keyList[kIdx]
                        var vals = keyBuckets[k]
                        var acc = Aggregator.Initial()
                        for vIdx in 0 ... vals.Count {
                            acc = Aggregator.Aggregate(acc, vals[vIdx])
                        }

                        var res = WindowResult[K, TResult]()
                        res.Key = k
                        res.WindowStart = winStart
                        res.WindowEnd = winEnd
                        res.Result = Aggregator.Emit(acc)
                        triggered.Add(res)
                    }

                    Windows.Remove(winStart)
                }
            } else {
                remainingStarts.Add(winStart)
            }
        }

        ActiveStarts = remainingStarts
        return triggered
    }
}
