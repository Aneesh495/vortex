// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Vortex.Core;
using Vortex.Storage;
using VortexRecord = Vortex.Core.Record;
using Xunit;

public class StorageTests : IDisposable
{
    private readonly string _tempDir;

    public StorageTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "vortex_tests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, true); } catch { }
        }
    }

    [Fact]
    public void OffsetIndex_BinarySearch_LocatesFloorOffset()
    {
        string indexPath = Path.Combine(_tempDir, "00000000000000000000.index");
        var idx = OffsetIndexFactory.OpenOrCreate(indexPath, 100);

        idx.Append(100, 0);
        idx.Append(110, 500);
        idx.Append(120, 1024);
        idx.Append(130, 2048);
        idx.Flush();

        var entry1 = idx.Lookup(100);
        Assert.Equal(0, entry1.RelativeOffset);
        Assert.Equal(0, entry1.PhysicalPosition);

        var entry2 = idx.Lookup(115); // Between 110 and 120 -> should return 110
        Assert.Equal(10, entry2.RelativeOffset); // 110 - 100 = 10
        Assert.Equal(500, entry2.PhysicalPosition);

        var entry3 = idx.Lookup(200); // Beyond last -> should return 130
        Assert.Equal(30, entry3.RelativeOffset);
        Assert.Equal(2048, entry3.PhysicalPosition);

        idx.Close();
    }

    [Fact]
    public void TimeIndex_BinarySearch_LocatesNearestTimestamp()
    {
        string timePath = Path.Combine(_tempDir, "00000000000000000000.timeindex");
        var idx = TimeIndexFactory.OpenOrCreate(timePath, 50);

        idx.Append(1000, 50);
        idx.Append(2000, 60);
        idx.Append(3000, 70);
        idx.Flush();

        var e1 = idx.Lookup(1500);
        Assert.Equal(1000, e1.Timestamp);
        Assert.Equal(0, e1.RelativeOffset);

        var e2 = idx.Lookup(2500);
        Assert.Equal(2000, e2.Timestamp);
        Assert.Equal(10, e2.RelativeOffset); // 60 - 50 = 10

        idx.Close();
    }

    [Fact]
    public void PartitionLog_AppendAndRead_MaintainsMonotonicOffsets()
    {
        var log = PartitionLogFactory.OpenOrCreate("orders", 0, _tempDir);
        log.MaxSegmentBytes = 512; // Force frequent segment rolls

        for (int b = 0; b < 10; b++)
        {
            var records = new List<VortexRecord>();
            for (int r = 0; r < 5; r++)
            {
                var rec = new VortexRecord();
                rec.Timestamp = 1700000000000L + (b * 5 + r);
                rec.Key = Encoding.UTF8.GetBytes("key-" + (b * 5 + r));
                rec.Value = Encoding.UTF8.GetBytes("value-" + (b * 5 + r));
                rec.Headers = new List<RecordHeader>();
                records.Add(rec);
            }

            var result = log.Append(records, 1, 101, 0);
            Assert.Equal(b * 5, result.BaseOffset);
            Assert.Equal((b + 1) * 5 - 1, result.LastOffset);
        }

        Assert.Equal(50, log.LogEndOffset());
        Assert.True(log.Segments.Count > 1); // Verifies rolling occurred

        // Read all records starting at offset 12
        var readBatches = log.Read(12, 100000, 50);
        Assert.NotEmpty(readBatches);

        long firstObserved = -1;
        foreach (var batch in readBatches)
        {
            if (firstObserved < 0) firstObserved = batch.BaseOffset;
            Assert.True(batch.LastOffset() >= 12);
        }
        Assert.True(firstObserved <= 12);

        log.Close();
    }

    [Fact]
    public void SkimpyOffsetMap_KeyDeduplication_ResolvesCollisions()
    {
        var map = SkimpyOffsetMapFactory.Create(64);

        for (int i = 0; i < 40; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes("account-" + i);
            bool added = map.Put(key, 1000 + i);
            Assert.True(added);
        }

        // Overwrite some keys with newer offsets
        map.Put(Encoding.UTF8.GetBytes("account-5"), 9999);
        map.Put(Encoding.UTF8.GetBytes("account-12"), 8888);

        Assert.Equal(9999, map.Get(Encoding.UTF8.GetBytes("account-5")));
        Assert.Equal(8888, map.Get(Encoding.UTF8.GetBytes("account-12")));
        Assert.Equal(1000, map.Get(Encoding.UTF8.GetBytes("account-0")));
        Assert.Equal(-1, map.Get(Encoding.UTF8.GetBytes("unknown-key")));
    }

    [Fact]
    public void LogCleaner_Compaction_RetainsLatestKeyVersions()
    {
        string partDir = Path.Combine(_tempDir, "compact-test");
        var segment = LogSegmentFactory.OpenOrCreate(partDir, 0);

        // Batch 1: keys A=10, B=11
        var recs1 = new List<VortexRecord>
        {
            CreateRecord(0, 100, "A", "val-A-v1"),
            CreateRecord(1, 101, "B", "val-B-v1")
        };
        var b1 = CreateBatch(0, recs1);
        segment.Append(b1);

        // Batch 2: keys A=12 (update), C=13
        var recs2 = new List<VortexRecord>
        {
            CreateRecord(2, 102, "A", "val-A-v2"),
            CreateRecord(3, 103, "C", "val-C-v1")
        };
        var b2 = CreateBatch(2, recs2);
        segment.Append(b2);
        segment.Flush();

        var cleaner = new LogCleaner();
        var map = cleaner.BuildOffsetMap(new List<LogSegment> { segment });

        Assert.Equal(2, map.Get(Encoding.UTF8.GetBytes("A")));
        Assert.Equal(1, map.Get(Encoding.UTF8.GetBytes("B")));
        Assert.Equal(3, map.Get(Encoding.UTF8.GetBytes("C")));

        var cleanedSegment = cleaner.CleanSegment(segment, map);
        var batches = cleanedSegment.Read(0, 10000);

        int totalRecords = 0;
        foreach (var b in batches)
        {
            totalRecords += b.Records.Count;
            foreach (var r in b.Records)
            {
                if (r.KeyString() == "A")
                {
                    Assert.Equal("val-A-v2", r.ValueString()); // Only v2 retained
                }
            }
        }
        Assert.Equal(3, totalRecords); // A(v2), B(v1), C(v1)

        cleanedSegment.Close();
    }

    [Fact]
    public void RetentionManager_EnforcesRetention_DeletesOldestSegments()
    {
        var log = PartitionLogFactory.OpenOrCreate("metrics", 0, _tempDir);
        log.MaxSegmentBytes = 256;

        for (int i = 0; i < 16; i++)
        {
            var recs = new List<VortexRecord>
            {
                CreateRecord(i * 2, 1000L + i * 100, "k", "v"),
                CreateRecord(i * 2 + 1, 1005L + i * 100, "k", "v")
            };
            log.Append(recs, 1, 0, 0);
        }

        Assert.True(log.Segments.Count >= 3);
        int initialSegmentCount = log.Segments.Count;

        var retention = new RetentionManager();
        retention.RetentionMs = 500; // Only keep segments newer than 500ms

        int pruned = retention.EnforceRetention(log, 3000L); // Current time 3000ms
        Assert.True(pruned > 0);
        Assert.True(log.Segments.Count < initialSegmentCount);
        Assert.NotNull(log.ActiveSegment);

        log.Close();
    }

    private static VortexRecord CreateRecord(long offset, long timestamp, string key, string value)
    {
        var r = new VortexRecord();
        r.Offset = offset;
        r.Timestamp = timestamp;
        r.Key = Encoding.UTF8.GetBytes(key);
        r.Value = Encoding.UTF8.GetBytes(value);
        r.Headers = new List<RecordHeader>();
        return r;
    }

    private static RecordBatch CreateBatch(long baseOffset, List<VortexRecord> records)
    {
        var b = new RecordBatch();
        b.BaseOffset = baseOffset;
        b.PartitionLeaderEpoch = 1;
        b.LastOffsetDelta = records.Count - 1;
        b.FirstTimestamp = records[0].Timestamp;
        b.MaxTimestamp = records[records.Count - 1].Timestamp;
        b.Records = records;
        return b;
    }
}
