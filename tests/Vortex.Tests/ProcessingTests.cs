// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

using System.Collections.Generic;
using Vortex.Processing;
using Xunit;

namespace Vortex.Tests;

public class ProcessingTests
{
    private class EvenNumberPredicate : IStreamPredicate<string, int>
    {
        public bool Test(StreamEvent<string, int> ev)
        {
            return ev.Value % 2 == 0;
        }
    }

    private class IntToStringMapper : IStreamMapper<string, int, string, string>
    {
        public StreamEvent<string, string> Map(StreamEvent<string, int> ev)
        {
            var res = new StreamEvent<string, string>();
            res.Key = ev.Key;
            res.Value = $"val:{ev.Value}";
            res.Timestamp = ev.Timestamp;
            res.Offset = ev.Offset;
            return res;
        }
    }

    private class DuplicateFlatMapper : IStreamFlatMapper<string, int, string, int>
    {
        public List<StreamEvent<string, int>> FlatMap(StreamEvent<string, int> ev)
        {
            var list = new List<StreamEvent<string, int>>();
            var e1 = new StreamEvent<string, int> { Key = ev.Key, Value = ev.Value, Timestamp = ev.Timestamp };
            var e2 = new StreamEvent<string, int> { Key = ev.Key, Value = ev.Value * 10, Timestamp = ev.Timestamp };
            list.Add(e1);
            list.Add(e2);
            return list;
        }
    }

    private class OddEvenBranchSelector : IBranchSelector<string, int>
    {
        public int SelectBranch(StreamEvent<string, int> ev)
        {
            return ev.Value % 2 == 0 ? 0 : 1;
        }
    }

    private class SumAggregator : IWindowAggregator<int, int, int>
    {
        public int Initial() => 0;
        public int Aggregate(int accum, int value) => accum + value;
        public int Emit(int accum) => accum;
    }

    [Fact]
    public void StreamEvent_CreationAndWatermarks()
    {
        var ev = StreamEventFactory.FromRecord("sensor-1", 42, 1000L, 10L);
        Assert.Equal("sensor-1", ev.Key);
        Assert.Equal(42, ev.Value);
        Assert.Equal(1000L, ev.Timestamp);
        Assert.Equal(10L, ev.Offset);
        Assert.False(ev.IsWatermark);

        var wm = StreamEventFactory.CreateWatermark<string, int>(1500L);
        Assert.True(wm.IsWatermark);
        Assert.Equal(1500L, wm.WatermarkTimestamp);
    }

    [Fact]
    public void BoundedOutOfOrderness_AdvancesWatermarkCorrectly()
    {
        var gen = new BoundedOutOfOrdernessGenerator { MaxOutOfOrdernessMs = 200 };

        var (wm1, advanced1) = gen.OnEvent(1000);
        Assert.True(advanced1);
        Assert.Equal(800, wm1);

        // Out-of-order event earlier than max timestamp
        var (wm2, advanced2) = gen.OnEvent(950);
        Assert.False(advanced2);
        Assert.Equal(800, wm2);

        // Later event advancing max timestamp
        var (wm3, advanced3) = gen.OnEvent(1500);
        Assert.True(advanced3);
        Assert.Equal(1300, wm3);
        Assert.Equal(1300, gen.CurrentWatermark());
    }

    [Fact]
    public void FilterOperator_DropsMismatches_KeepsWatermarks()
    {
        var filter = new FilterOperator<string, int> { Predicate = new EvenNumberPredicate() };

        var ev1 = StreamEventFactory.FromRecord("k", 10, 100, 1);
        var (res1, ok1) = filter.Process(ev1);
        Assert.True(ok1);
        Assert.NotNull(res1);
        Assert.Equal(10, res1.Value);

        var ev2 = StreamEventFactory.FromRecord("k", 11, 101, 2);
        var (res2, ok2) = filter.Process(ev2);
        Assert.False(ok2);
        Assert.Null(res2);

        var wm = StreamEventFactory.CreateWatermark<string, int>(200);
        var (resWm, okWm) = filter.Process(wm);
        Assert.True(okWm);
        Assert.NotNull(resWm);
        Assert.True(resWm.IsWatermark);
    }

    [Fact]
    public void MapOperator_TransformsValues_PreservesWatermarks()
    {
        var mapper = new MapOperator<string, int, string, string> { Mapper = new IntToStringMapper() };

        var ev = StreamEventFactory.FromRecord("device", 77, 500, 1);
        var mapped = mapper.Process(ev);
        Assert.Equal("val:77", mapped.Value);
        Assert.Equal(500, mapped.Timestamp);

        var wm = StreamEventFactory.CreateWatermark<string, int>(600);
        var mappedWm = mapper.Process(wm);
        Assert.True(mappedWm.IsWatermark);
        Assert.Equal(600, mappedWm.WatermarkTimestamp);
    }

    [Fact]
    public void FlatMapOperator_ProducesMultipleOutputs()
    {
        var flatMapper = new FlatMapOperator<string, int, string, int> { FlatMapper = new DuplicateFlatMapper() };

        var ev = StreamEventFactory.FromRecord("k", 5, 100, 1);
        var results = flatMapper.Process(ev);
        Assert.Equal(2, results.Count);
        Assert.Equal(5, results[0].Value);
        Assert.Equal(50, results[1].Value);
    }

    [Fact]
    public void BranchOperator_RoutesCorrectly()
    {
        var branch = new BranchOperator<string, int> { Selector = new OddEvenBranchSelector(), BranchCount = 2 };

        var evEven = StreamEventFactory.FromRecord("k", 20, 100, 1);
        Assert.Equal(0, branch.Route(evEven));

        var evOdd = StreamEventFactory.FromRecord("k", 21, 100, 2);
        Assert.Equal(1, branch.Route(evOdd));

        var wm = StreamEventFactory.CreateWatermark<string, int>(200);
        Assert.Equal(-1, branch.Route(wm));
    }

    [Fact]
    public void TumblingWindow_AggregatesByKey_TriggersOnWatermark()
    {
        var window = new TumblingWindowOperator<string, int, int, int>
        {
            WindowSizeMs = 1000,
            Aggregator = new SumAggregator()
        };

        // Add events in window [0, 1000)
        Assert.True(window.ProcessEvent(StreamEventFactory.FromRecord("k1", 10, 100, 1)));
        Assert.True(window.ProcessEvent(StreamEventFactory.FromRecord("k1", 20, 200, 2)));
        Assert.True(window.ProcessEvent(StreamEventFactory.FromRecord("k2", 50, 300, 3)));

        // Watermark at 500 should NOT trigger window [0, 1000)
        var triggered1 = window.OnWatermark(500);
        Assert.Empty(triggered1);

        // Watermark at 1000 triggers window [0, 1000)
        var triggered2 = window.OnWatermark(1000);
        Assert.Equal(2, triggered2.Count);

        var k1Result = triggered2.Find(r => r.Key == "k1");
        var k2Result = triggered2.Find(r => r.Key == "k2");
        Assert.NotNull(k1Result);
        Assert.NotNull(k2Result);
        Assert.Equal(30, k1Result.Result);
        Assert.Equal(50, k2Result.Result);
        Assert.Equal(0, k1Result.WindowStart);
        Assert.Equal(1000, k1Result.WindowEnd);
    }

    [Fact]
    public void TumblingWindow_DropsLateEvents()
    {
        var window = new TumblingWindowOperator<string, int, int, int>
        {
            WindowSizeMs = 1000,
            Aggregator = new SumAggregator()
        };

        window.OnWatermark(1500);

        // Event with timestamp 1200 is late because watermark is 1500
        bool accepted = window.ProcessEvent(StreamEventFactory.FromRecord("k1", 10, 1200, 1));
        Assert.False(accepted);
        Assert.Equal(1, window.LateEventCount);
    }

    [Fact]
    public void SlidingWindow_CalculatesOverlappingWindows()
    {
        var window = new SlidingWindowOperator<string, int, int, int>
        {
            WindowSizeMs = 1000,
            SlideMs = 500,
            Aggregator = new SumAggregator()
        };

        // Event at 600 falls into window [0, 1000) and window [500, 1500)
        Assert.True(window.ProcessEvent(StreamEventFactory.FromRecord("k1", 15, 600, 1)));

        // Watermark at 1000 closes window [0, 1000)
        var triggered1 = window.OnWatermark(1000);
        Assert.Single(triggered1);
        Assert.Equal(0, triggered1[0].WindowStart);
        Assert.Equal(1000, triggered1[0].WindowEnd);
        Assert.Equal(15, triggered1[0].Result);

        // Watermark at 1500 closes window [500, 1500)
        var triggered2 = window.OnWatermark(1500);
        Assert.Single(triggered2);
        Assert.Equal(500, triggered2[0].WindowStart);
        Assert.Equal(1500, triggered2[0].WindowEnd);
        Assert.Equal(15, triggered2[0].Result);
    }

    [Fact]
    public void StreamTopology_ExecutesFullPipeline()
    {
        var topology = new StreamTopology { Name = "test-pipeline" };
        var filter = new FilterOperator<string, int> { Predicate = new EvenNumberPredicate() };
        var mapper = new MapOperator<string, int, string, string> { Mapper = new IntToStringMapper() };
        var sink = new InMemorySink<StreamEvent<string, string>>();

        var ev1 = StreamEventFactory.FromRecord("dev", 10, 100, 1);
        var ev2 = StreamEventFactory.FromRecord("dev", 11, 200, 2);
        var ev3 = StreamEventFactory.FromRecord("dev", 12, 300, 3);

        Assert.True(topology.ExecuteEvent(ev1, filter, mapper, sink));
        Assert.False(topology.ExecuteEvent(ev2, filter, mapper, sink));
        Assert.True(topology.ExecuteEvent(ev3, filter, mapper, sink));

        Assert.Equal(3, topology.ProcessedCount);
        Assert.Equal(1, topology.DroppedCount);
        Assert.Equal(2, sink.Count());
        Assert.Equal("val:10", sink.Items[0].Value);
        Assert.Equal("val:12", sink.Items[1].Value);
    }
}
