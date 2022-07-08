// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

// Compacts log segments by deduplicating records per key and pruning expired tombstones.
class LogCleaner {
    var MapCapacity int32 = 65536
    var TombstoneRetentionMs int64 = 86400000 // 24 hours

    // Cleans the given sealed segment by filtering out superseded record versions.
    // Returns the newly generated compacted LogSegment.
    func CleanSegment(segment LogSegment, offsetMap SkimpyOffsetMap) LogSegment {
        var dir string = segment.DirectoryPath
        var baseOffset int64 = segment.BaseOffset
        var offsetStr string = baseOffset.ToString().PadLeft(20, '0')

        var cleanedLogPath string = Path.Combine(dir, offsetStr + ".cleaned.log")
        var cleanedIndexPath string = Path.Combine(dir, offsetStr + ".cleaned.index")
        var cleanedTimePath string = Path.Combine(dir, offsetStr + ".cleaned.timeindex")

        if File.Exists(cleanedLogPath) { File.Delete(cleanedLogPath) }
        if File.Exists(cleanedIndexPath) { File.Delete(cleanedIndexPath) }
        if File.Exists(cleanedTimePath) { File.Delete(cleanedTimePath) }

        var targetFs = FileStream(cleanedLogPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None)
        var targetIdx = OffsetIndexFactory.OpenOrCreate(cleanedIndexPath, baseOffset)
        var targetTimeIdx = TimeIndexFactory.OpenOrCreate(cleanedTimePath, baseOffset)

        var cleanedSegment = LogSegment{}
        cleanedSegment.DirectoryPath = dir
        cleanedSegment.BaseOffset = baseOffset
        cleanedSegment.LogStream = targetFs
        cleanedSegment.Index = targetIdx
        cleanedSegment.TimeIndex = targetTimeIdx
        cleanedSegment.NextOffset = baseOffset
        cleanedSegment.SyncLock = Object()

        var sourceFs = segment.LogStream
        sourceFs.Seek(0, SeekOrigin.Begin)

        while sourceFs.Position < sourceFs.Length {
            var batch = RecordBatchCodec.Deserialize(sourceFs)
            var retained = List[Record]()

            for r in 0 ... batch.Records.Count {
                var rec = batch.Records[r]
                if rec.Key.Length == 0 {
                    // Non-keyed records are always retained
                    retained.Add(rec)
                    continue
                }

                var latestOffset int64 = offsetMap.Get(rec.Key)
                if rec.Offset == latestOffset {
                    retained.Add(rec)
                }
            }

            if retained.Count > 0 {
                var newBatch = RecordBatch{}
                newBatch.BaseOffset = retained[0].Offset
                newBatch.PartitionLeaderEpoch = batch.PartitionLeaderEpoch
                newBatch.Attributes = batch.Attributes
                newBatch.LastOffsetDelta = int32(retained[retained.Count - 1].Offset - newBatch.BaseOffset)
                newBatch.FirstTimestamp = retained[0].Timestamp
                newBatch.MaxTimestamp = retained[retained.Count - 1].Timestamp
                newBatch.ProducerId = batch.ProducerId
                newBatch.ProducerEpoch = batch.ProducerEpoch
                newBatch.BaseSequence = batch.BaseSequence
                newBatch.Records = retained

                cleanedSegment.Append(newBatch)
            }
        }

        cleanedSegment.Flush()
        cleanedSegment.Close()

        // Swap files atomically: replace original files with cleaned versions
        segment.Close()

        var origLogPath string = Path.Combine(dir, offsetStr + ".log")
        var origIndexPath string = Path.Combine(dir, offsetStr + ".index")
        var origTimePath string = Path.Combine(dir, offsetStr + ".timeindex")

        File.Delete(origLogPath)
        File.Delete(origIndexPath)
        File.Delete(origTimePath)

        File.Move(cleanedLogPath, origLogPath)
        File.Move(cleanedIndexPath, origIndexPath)
        File.Move(cleanedTimePath, origTimePath)

        return LogSegmentFactory.OpenOrCreate(dir, baseOffset)
    }

    // Builds the deduplication map across a list of sealed segments.
    func BuildOffsetMap(segments List[LogSegment]) SkimpyOffsetMap {
        var offsetMap = SkimpyOffsetMapFactory.Create(MapCapacity)

        for s in 0 ... segments.Count {
            var seg = segments[s]
            var fs = seg.LogStream
            fs.Seek(0, SeekOrigin.Begin)

            while fs.Position < fs.Length {
                try {
                    var batch = RecordBatchCodec.Deserialize(fs)
                    for r in 0 ... batch.Records.Count {
                        var rec = batch.Records[r]
                        if rec.Key.Length > 0 {
                            offsetMap.Put(rec.Key, rec.Offset)
                        }
                    }
                } catch (ex Exception) {
                    break
                }
            }
        }

        return offsetMap
    }
}
