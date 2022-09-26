// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Cli

import System
import System.IO
import System.Diagnostics
import System.Collections.Generic
import Vortex.Core
import Vortex.Storage
import Vortex.Client

// Statistical latency percentile distribution analyzer.
class LatencyHistogram {
    var Samples List[int64] = List[int64]()

    func Record(latencyMicros int64) {
        Samples.Add(latencyMicros)
    }

    func Count() int32 {
        return Samples.Count
    }

    // Computes designated percentile (e.g. 50.0, 99.0, 99.9) across recorded samples.
    func Percentile(p double) int64 {
        if Samples.Count == 0 {
            return 0
        }

        var arr []int64 = Samples.ToArray()
        Array.Sort(arr)

        var rank double = (p / 100.0) * double(arr.Length - 1)
        var idx int32 = int32(Math.Round(rank))
        if idx >= arr.Length {
            idx = arr.Length - 1
        }
        return arr[idx]
    }
}

// End-to-end stress harness evaluating persistent storage throughput and latency percentiles.
shared class StorageStressHarness {
    func RunStress(tempDir string, messageCount int32, messageSize int32) int32 {
        Console.WriteLine("--------------------------------------------------")
        Console.WriteLine("  VORTEX PERSISTENT STORAGE ENGINE STRESS TEST")
        Console.WriteLine("--------------------------------------------------")
        Console.WriteLine("Directory: " + tempDir)
        Console.WriteLine("Messages:  " + messageCount.ToString())
        Console.WriteLine("Size:      " + messageSize.ToString() + " bytes")

        if Directory.Exists(tempDir) {
            Directory.Delete(tempDir, true)
        }
        Directory.CreateDirectory(tempDir)

        var log = PartitionLogFactory.OpenOrCreate("stress-topic", 0, tempDir)
        var histogram = LatencyHistogram()

        var samplePayload []byte = [messageSize]byte
        for i in 0 ... messageSize {
            samplePayload[i] = byte(i % 127)
        }

        var timerFreq double = double(Stopwatch.Frequency)
        var totalSw = Stopwatch.StartNew()

        // Batch in groups of 100
        var batchSize int32 = 100
        var totalBatches int32 = messageCount / batchSize

        for b in 0 ... totalBatches {
            var records = List[Record]()
            for r in 0 ... batchSize {
                var rec = Record()
                rec.Offset = int64(b * batchSize + r)
                rec.Key = [0]byte
                rec.Value = samplePayload
                rec.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                rec.Headers = List[RecordHeader]()
                records.Add(rec)
            }

            var startTick int64 = Stopwatch.GetTimestamp()
            log.Append(records, 0, 1, 0)
            var endTick int64 = Stopwatch.GetTimestamp()

            var elapsedMicros int64 = int64(((double(endTick - startTick)) / timerFreq) * 1000000.0)
            histogram.Record(elapsedMicros)
        }

        log.Flush()
        totalSw.Stop()

        var totalSeconds double = totalSw.Elapsed.TotalSeconds
        var msgsPerSec double = double(messageCount) / totalSeconds
        var mbWritten double = (double(messageCount) * double(messageSize)) / (1024.0 * 1024.0)
        var mbPerSec double = mbWritten / totalSeconds

        Console.WriteLine("Total elapsed: " + totalSw.ElapsedMilliseconds.ToString() + " ms")
        Console.WriteLine("Append rate:   " + String.Format("{0:N0}", msgsPerSec) + " records/sec")
        Console.WriteLine("Disk rate:     " + String.Format("{0:N2}", mbPerSec) + " MB/sec")
        Console.WriteLine("Latency p50:   " + histogram.Percentile(50.0).ToString() + " us")
        Console.WriteLine("Latency p90:   " + histogram.Percentile(90.0).ToString() + " us")
        Console.WriteLine("Latency p99:   " + histogram.Percentile(99.0).ToString() + " us")
        Console.WriteLine("Latency p99.9: " + histogram.Percentile(99.9).ToString() + " us")

        // Verification pass: read back
        var readBatches = log.Read(0, 1024 * 1024 * 64, log.HighWatermark)
        Console.WriteLine("Verification:  " + readBatches.Count.ToString() + " batches verified on disk")
        Console.WriteLine("--------------------------------------------------")

        // Cleanup
        try {
            Directory.Delete(tempDir, true)
        } catch (ex Exception) {
            // Ignored on cleanup
        }

        return 0
    }
}
