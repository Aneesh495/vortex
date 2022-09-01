// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Processing

import System
import System.Collections.Generic
import System.Threading
import System.Threading.Tasks
import System.Threading.Channels

// Interface for a single execution step in a streaming topology DAG.
interface ITopologyStage[TIn, TOut] {
    func Process(input TIn) (TOut?, bool);
}

// Stage executing an IStreamPredicate filter.
class FilterStage[K, V] : ITopologyStage[StreamEvent[K, V], StreamEvent[K, V]] {
    var Operator FilterOperator[K, V]

    func Process(input StreamEvent[K, V]) (StreamEvent[K, V]?, bool) {
        return Operator.Process(input)
    }
}

// Stage executing an IStreamMapper transformation.
class MapStage[KIn, VIn, KOut, VOut] : ITopologyStage[StreamEvent[KIn, VIn], StreamEvent[KOut, VOut]] {
    var Operator MapOperator[KIn, VIn, KOut, VOut]

    func Process(input StreamEvent[KIn, VIn]) (StreamEvent[KOut, VOut]?, bool) {
        var res = Operator.Process(input)
        return (res, true)
    }
}

// Sink collector capturing terminal stream events.
interface IStreamSink[T] {
    func Accept(item T);
}

// In-memory sink recording all received events for verification and downstream feeding.
class InMemorySink[T] : IStreamSink[T] {
    var Items List[T] = List[T]()

    func Accept(item T) {
        Items.Add(item)
    }

    func Count() int32 {
        return Items.Count
    }

    func Clear() {
        Items.Clear()
    }
}

// Directed Acyclic Graph topology runner managing stream execution pipelines.
class StreamTopology {
    var Name string
    var IsRunning bool = false
    var ProcessedCount int64 = 0
    var DroppedCount int64 = 0

    // Executes a linear event through a filter and map pipeline to a sink.
    func ExecuteEvent[KIn, VIn, KOut, VOut](
        ev StreamEvent[KIn, VIn],
        filterOp FilterOperator[KIn, VIn]?,
        mapOp MapOperator[KIn, VIn, KOut, VOut],
        sink IStreamSink[StreamEvent[KOut, VOut]]) bool {

        ProcessedCount = ProcessedCount + 1

        if filterOp != nil {
            var (filtered, ok) = filterOp.Process(ev)
            if !ok || filtered == nil {
                DroppedCount = DroppedCount + 1
                return false
            }
            var mapped = mapOp.Process(filtered)
            sink.Accept(mapped)
            return true
        } else {
            var mapped = mapOp.Process(ev)
            sink.Accept(mapped)
            return true
        }
    }
}
