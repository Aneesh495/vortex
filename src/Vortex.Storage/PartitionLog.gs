// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

// Result of appending a record batch to a partition log.
data class LogAppendResult(BaseOffset int64, LastOffset int64, RecordCount int32)

// Manages the sequential segments, high watermark, and log end offset for a single partition.
class PartitionLog {
    var Topic string
    var Partition int32
    var DirectoryPath string
    var MaxSegmentBytes int64 = 10485760 // 10 MB default segment roll threshold
    var Segments List[LogSegment] = List[LogSegment]()
    var ActiveSegment LogSegment
    var HighWatermark int64 = 0
    var NextLogEndOffset int64 = 0
    var SyncLock Object = Object()

    func LogEndOffset() int64 {
        lock SyncLock {
            return NextLogEndOffset
        }
    }

    func HighWatermarkOffset() int64 {
        lock SyncLock {
            return HighWatermark
        }
    }

    // Advances the high watermark if the proposed offset is higher and does not exceed LEO.
    func UpdateHighWatermark(newHighWatermark int64) {
        lock SyncLock {
            if newHighWatermark > HighWatermark && newHighWatermark <= NextLogEndOffset {
                HighWatermark = newHighWatermark
            }
        }
    }

    // Appends a collection of records to the active segment, rolling if capacity is reached.
    func Append(records List[Record], leaderEpoch int32, producerId int64, producerEpoch int16) LogAppendResult {
        lock SyncLock {
            if records.Count == 0 {
                return LogAppendResult(NextLogEndOffset, NextLogEndOffset, 0)
            }

            // Segment roll check: active segment exceeds max allowed byte size
            if ActiveSegment.SizeBytes() >= MaxSegmentBytes {
                Roll(NextLogEndOffset)
            }

            var baseOffset int64 = NextLogEndOffset
            var firstTimestamp int64 = records[0].Timestamp
            var maxTimestamp int64 = firstTimestamp

            for i in 0 ... records.Count {
                var rec = records[i]
                rec.Offset = baseOffset + int64(i)
                if rec.Timestamp > maxTimestamp {
                    maxTimestamp = rec.Timestamp
                }
            }

            var lastOffsetDelta int32 = records.Count - 1
            var batch = RecordBatch{}
            batch.BaseOffset = baseOffset
            batch.PartitionLeaderEpoch = leaderEpoch
            batch.Attributes = 0
            batch.LastOffsetDelta = lastOffsetDelta
            batch.FirstTimestamp = firstTimestamp
            batch.MaxTimestamp = maxTimestamp
            batch.ProducerId = producerId
            batch.ProducerEpoch = producerEpoch
            batch.BaseSequence = 0
            batch.Records = records

            ActiveSegment.Append(batch)
            NextLogEndOffset = baseOffset + int64(records.Count)

            return LogAppendResult(baseOffset, NextLogEndOffset - 1, records.Count)
        }
    }

    // Reads batches beginning at startOffset up to maxBytes, bounded by maxOffset.
    func Read(startOffset int64, maxBytes int32, maxOffset int64) List[RecordBatch] {
        lock SyncLock {
            var results = List[RecordBatch]()
            if startOffset >= NextLogEndOffset || startOffset >= maxOffset {
                return results
            }

            var remainingBytes int32 = maxBytes

            // Iterate through segments starting from the segment containing startOffset
            for s in 0 ... Segments.Count {
                var segment = Segments[s]
                if segment.NextOffset <= startOffset {
                    continue
                }
                if segment.BaseOffset > maxOffset {
                    break
                }

                var batches = segment.Read(startOffset, remainingBytes)
                for b in 0 ... batches.Count {
                    var batch = batches[b]
                    if batch.BaseOffset >= maxOffset {
                        break
                    }
                    results.Add(batch)
                    remainingBytes = remainingBytes - batch.BatchLength
                    if remainingBytes <= 0 {
                        break
                    }
                }

                if remainingBytes <= 0 {
                    break
                }
            }

            return results
        }
    }

    // Rolls the active segment and creates a new active segment starting at newBaseOffset.
    func Roll(newBaseOffset int64) LogSegment {
        lock SyncLock {
            ActiveSegment.Flush()
            var newSegment = LogSegmentFactory.OpenOrCreate(DirectoryPath, newBaseOffset)
            Segments.Add(newSegment)
            ActiveSegment = newSegment
            return newSegment
        }
    }

    // Truncates the partition log back to targetOffset, deleting any entirely subsequent segments.
    func TruncateTo(targetOffset int64) {
        lock SyncLock {
            if targetOffset >= NextLogEndOffset {
                return
            }

            var surviving = List[LogSegment]()
            for s in 0 ... Segments.Count {
                var segment = Segments[s]
                if segment.BaseOffset > targetOffset {
                    // Segment is entirely beyond the target offset, delete on disk
                    segment.Close()
                    var prefix string = segment.BaseOffset.ToString().PadLeft(20, '0')
                    File.Delete(Path.Combine(DirectoryPath, prefix + ".log"))
                    File.Delete(Path.Combine(DirectoryPath, prefix + ".index"))
                    File.Delete(Path.Combine(DirectoryPath, prefix + ".timeindex"))
                } else {
                    if segment.NextOffset > targetOffset {
                        segment.TruncateTo(targetOffset)
                    }
                    surviving.Add(segment)
                }
            }

            if surviving.Count == 0 {
                var fresh = LogSegmentFactory.OpenOrCreate(DirectoryPath, targetOffset)
                surviving.Add(fresh)
            }

            Segments = surviving
            ActiveSegment = Segments[Segments.Count - 1]
            NextLogEndOffset = targetOffset

            if HighWatermark > targetOffset {
                HighWatermark = targetOffset
            }
        }
    }

    func Flush() {
        lock SyncLock {
            ActiveSegment.Flush()
        }
    }

    func Close() {
        lock SyncLock {
            for s in 0 ... Segments.Count {
                Segments[s].Close()
            }
        }
    }
}

// Factory helper for opening or initializing a PartitionLog directory.
shared class PartitionLogFactory {
    func OpenOrCreate(topic string, partition int32, logDir string) PartitionLog {
        var partDir string = Path.Combine(logDir, topic + "-" + partition.ToString())
        Directory.CreateDirectory(partDir)

        var logFiles []string = Directory.GetFiles(partDir, "*.log")
        Array.Sort(logFiles)

        var segments = List[LogSegment]()
        if logFiles.Length == 0 {
            var initial = LogSegmentFactory.OpenOrCreate(partDir, 0)
            segments.Add(initial)
        } else {
            for i in 0 ... logFiles.Length {
                var file string = logFiles[i]
                var fileName string = Path.GetFileNameWithoutExtension(file)
                var baseOffset int64 = int64.Parse(fileName)
                var seg = LogSegmentFactory.OpenOrCreate(partDir, baseOffset)
                segments.Add(seg)
            }
        }

        var plog = PartitionLog{}
        plog.Topic = topic
        plog.Partition = partition
        plog.DirectoryPath = partDir
        plog.Segments = segments
        plog.ActiveSegment = segments[segments.Count - 1]
        plog.NextLogEndOffset = plog.ActiveSegment.NextOffset
        plog.HighWatermark = plog.ActiveSegment.NextOffset
        plog.SyncLock = Object()

        return plog
    }
}
