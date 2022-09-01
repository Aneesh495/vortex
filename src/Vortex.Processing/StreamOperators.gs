// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Processing

import System
import System.Collections.Generic

// Predicate interface for filtering streaming events.
interface IStreamPredicate[K, V] {
    func Test(ev StreamEvent[K, V]) bool;
}

// Mapper interface for transforming streaming events.
interface IStreamMapper[KIn, VIn, KOut, VOut] {
    func Map(ev StreamEvent[KIn, VIn]) StreamEvent[KOut, VOut];
}

// FlatMapper interface for one-to-many streaming transformations.
interface IStreamFlatMapper[KIn, VIn, KOut, VOut] {
    func FlatMap(ev StreamEvent[KIn, VIn]) List[StreamEvent[KOut, VOut]];
}

// Stateless filter operator dropping non-matching events while forwarding watermarks.
class FilterOperator[K, V] {
    var Predicate IStreamPredicate[K, V]

    // Evaluates an event. Returns true and the event if matched, or false if dropped.
    func Process(ev StreamEvent[K, V]) (StreamEvent[K, V]?, bool) {
        if ev.IsWatermark {
            return (ev, true)
        }

        if Predicate.Test(ev) {
            return (ev, true)
        }

        return (nil, false)
    }
}

// Stateless mapper operator projecting events from input schema to output schema.
class MapOperator[KIn, VIn, KOut, VOut] {
    var Mapper IStreamMapper[KIn, VIn, KOut, VOut]

    // Transforms an incoming event, preserving watermarks.
    func Process(ev StreamEvent[KIn, VIn]) StreamEvent[KOut, VOut] {
        if ev.IsWatermark {
            var wm = StreamEvent[KOut, VOut]{}
            wm.IsWatermark = true
            wm.WatermarkTimestamp = ev.WatermarkTimestamp
            return wm
        }

        return Mapper.Map(ev)
    }
}

// Stateless flat map operator producing zero or more output events per input event.
class FlatMapOperator[KIn, VIn, KOut, VOut] {
    var FlatMapper IStreamFlatMapper[KIn, VIn, KOut, VOut]

    // Expands an incoming event into a list of output events.
    func Process(ev StreamEvent[KIn, VIn]) List[StreamEvent[KOut, VOut]] {
        var results = List[StreamEvent[KOut, VOut]]{}
        if ev.IsWatermark {
            var wm = StreamEvent[KOut, VOut]{}
            wm.IsWatermark = true
            wm.WatermarkTimestamp = ev.WatermarkTimestamp
            results.Add(wm)
            return results
        }

        var mapped = FlatMapper.FlatMap(ev)
        for i in 0 ... mapped.Count {
            results.Add(mapped[i])
        }
        return results
    }
}

// Branch router interface directing events to one or more output branches.
interface IBranchSelector[K, V] {
    func SelectBranch(ev StreamEvent[K, V]) int32;
}

// Stateless branch operator splitting a single stream into multiple independent branches.
class BranchOperator[K, V] {
    var Selector IBranchSelector[K, V]
    var BranchCount int32

    // Determines which branch should receive the event. Returns -1 for broadcast watermarks.
    func Route(ev StreamEvent[K, V]) int32 {
        if ev.IsWatermark {
            return -1
        }
        return Selector.SelectBranch(ev)
    }
}
