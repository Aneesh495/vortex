// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

namespace Vortex.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Vortex.Core;
using VortexRecord = Vortex.Core.Record;
using Xunit;

public class RecordBatchTests
{
    [Fact]
    public void Record_TombstoneDetection_WorksAccurately()
    {
        var active = new VortexRecord();
        active.Offset = 10;
        active.Timestamp = 1000;
        active.Key = Encoding.UTF8.GetBytes("user-1");
        active.Value = Encoding.UTF8.GetBytes("{\"active\": true}");
        active.Headers = new List<RecordHeader>();

        var tombstone = new VortexRecord();
        tombstone.Offset = 11;
        tombstone.Timestamp = 1005;
        tombstone.Key = Encoding.UTF8.GetBytes("user-1");
        tombstone.Value = Array.Empty<byte>();
        tombstone.Headers = new List<RecordHeader>();

        Assert.False(active.IsTombstone());
        Assert.True(tombstone.IsTombstone());
    }

    [Fact]
    public void RecordBatch_SerializeAndDeserialize_RoundTripsExactly()
    {
        var records = new List<VortexRecord>();
        for (int i = 0; i < 5; i++)
        {
            var headers = new List<RecordHeader>
            {
                new RecordHeader("trace-id", Encoding.UTF8.GetBytes("trace-" + i)),
                new RecordHeader("env", Encoding.UTF8.GetBytes("prod"))
            };

            var r = new VortexRecord();
            r.Offset = 100 + i;
            r.Timestamp = 1700000000000L + i * 10;
            r.Key = Encoding.UTF8.GetBytes("k" + i);
            r.Value = Encoding.UTF8.GetBytes("payload-event-data-" + i);
            r.Headers = headers;
            records.Add(r);
        }

        var batch = new RecordBatch();
        batch.BaseOffset = 100;
        batch.PartitionLeaderEpoch = 3;
        batch.LastOffsetDelta = 4;
        batch.FirstTimestamp = 1700000000000L;
        batch.MaxTimestamp = 1700000000040L;
        batch.ProducerId = 999;
        batch.ProducerEpoch = 1;
        batch.BaseSequence = 0;
        batch.Records = records;

        using var ms = new MemoryStream();
        batch.Serialize(ms);

        Assert.True(ms.Length > 0);
        Assert.True(batch.Crc32 > 0);

        ms.Position = 0;
        var recovered = RecordBatchCodec.Deserialize(ms);

        Assert.Equal(batch.BaseOffset, recovered.BaseOffset);
        Assert.Equal(batch.PartitionLeaderEpoch, recovered.PartitionLeaderEpoch);
        Assert.Equal(batch.Crc32, recovered.Crc32);
        Assert.Equal(5, recovered.RecordCount());
        Assert.Equal(104, recovered.LastOffset());

        for (int i = 0; i < 5; i++)
        {
            var original = batch.Records[i];
            var actual = recovered.Records[i];

            Assert.Equal(original.Offset, actual.Offset);
            Assert.Equal(original.Timestamp, actual.Timestamp);
            Assert.Equal(original.KeyString(), actual.KeyString());
            Assert.Equal(original.ValueString(), actual.ValueString());
            Assert.Equal(2, actual.Headers.Count);
            Assert.Equal("trace-id", actual.Headers[0].Key);
        }
    }

    [Fact]
    public void RecordBatch_CorruptedPayload_ThrowsInvalidDataException()
    {
        var records = new List<VortexRecord>();
        var r = new VortexRecord();
        r.Offset = 50;
        r.Timestamp = 1000;
        r.Key = Encoding.UTF8.GetBytes("key");
        r.Value = Encoding.UTF8.GetBytes("value");
        r.Headers = new List<RecordHeader>();
        records.Add(r);

        var batch = new RecordBatch();
        batch.BaseOffset = 50;
        batch.PartitionLeaderEpoch = 1;
        batch.LastOffsetDelta = 0;
        batch.FirstTimestamp = 1000;
        batch.MaxTimestamp = 1000;
        batch.Records = records;

        using var ms = new MemoryStream();
        batch.Serialize(ms);

        byte[] rawBytes = ms.ToArray();
        // Corrupt one byte inside the payload (after header)
        rawBytes[rawBytes.Length - 1] ^= 0xFF;

        using var corruptedMs = new MemoryStream(rawBytes);
        Assert.Throws<InvalidDataException>(() => RecordBatchCodec.Deserialize(corruptedMs));
    }
}
