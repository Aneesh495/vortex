// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

// An individual physical segment comprising a commit log file (.log),
// an offset index file (.index), and a timestamp index file (.timeindex).
class LogSegment {
    var DirectoryPath string
    var BaseOffset int64
    var IndexIntervalBytes int32 = 4096
    var LogStream FileStream
    var Index OffsetIndex
    var TimeIndex TimeIndex
    var NextOffset int64
    var BytesSinceLastIndex int32 = 0
    var SyncLock Object = Object()

    func SizeBytes() int64 {
        lock SyncLock {
            return LogStream.Length
        }
    }

    // Appends a physical batch of records to the segment file and updates the sparse indices.
    func Append(batch RecordBatch) {
        lock SyncLock {
            var physicalPos int32 = int32(LogStream.Position)

            // Index boundary check: insert sparse index entries when byte threshold crossed
            if BytesSinceLastIndex >= IndexIntervalBytes || Index.EntryCount == 0 {
                Index.Append(batch.BaseOffset, physicalPos)
                TimeIndex.Append(batch.MaxTimestamp, batch.BaseOffset)
                BytesSinceLastIndex = 0
            }

            batch.Serialize(LogStream)
            var batchBytes int32 = batch.BatchLength + 8 + 4
            BytesSinceLastIndex = BytesSinceLastIndex + batchBytes
            NextOffset = batch.LastOffset() + 1
        }
    }

    // Reads batches beginning at or after the given startOffset up to maxBytes.
    func Read(startOffset int64, maxBytes int32) List[RecordBatch] {
        lock SyncLock {
            var batches = List[RecordBatch]()
            if startOffset >= NextOffset {
                return batches
            }

            // 1. Locate starting physical file position via sparse index binary search
            var entry = Index.Lookup(startOffset)
            var startPos int64 = int64(entry.PhysicalPosition)
            if startPos >= LogStream.Length {
                return batches
            }

            LogStream.Seek(startPos, SeekOrigin.Begin)
            var bytesRead int32 = 0

            while LogStream.Position < LogStream.Length && bytesRead < maxBytes {
                var currentPos int64 = LogStream.Position
                var batch = RecordBatchCodec.Deserialize(LogStream)

                // If batch contains records matching the target offset range, include it
                if batch.LastOffset() >= startOffset {
                    batches.Add(batch)
                }

                var delta int32 = int32(LogStream.Position - currentPos)
                bytesRead = bytesRead + delta
            }

            return batches
        }
    }

    // Truncates this segment to the specified target offset, resetting indices and file length.
    func TruncateTo(targetOffset int64) {
        lock SyncLock {
            if targetOffset <= BaseOffset {
                LogStream.SetLength(0)
                LogStream.Seek(0, SeekOrigin.Begin)
                Index.TruncateTo(BaseOffset)
                TimeIndex.TruncateTo(BaseOffset)
                NextOffset = BaseOffset
                BytesSinceLastIndex = 0
                return
            }

            // Find physical boundary corresponding to targetOffset
            var entry = Index.Lookup(targetOffset)
            LogStream.Seek(int64(entry.PhysicalPosition), SeekOrigin.Begin)

            var validPos int64 = int64(entry.PhysicalPosition)
            var lastValidOffset int64 = BaseOffset

            while LogStream.Position < LogStream.Length {
                var batchStart int64 = LogStream.Position
                try {
                    var batch = RecordBatchCodec.Deserialize(LogStream)
                    if batch.BaseOffset <= targetOffset {
                        validPos = LogStream.Position
                        lastValidOffset = batch.LastOffset()
                    } else {
                        break
                    }
                } catch (ex Exception) {
                    break
                }
            }

            LogStream.SetLength(validPos)
            LogStream.Seek(validPos, SeekOrigin.Begin)
            Index.TruncateTo(targetOffset)
            TimeIndex.TruncateTo(targetOffset)
            NextOffset = lastValidOffset + 1
            BytesSinceLastIndex = 0
            Flush()
        }
    }

    func Flush() {
        lock SyncLock {
            LogStream.Flush(true)
            Index.Flush()
            TimeIndex.Flush()
        }
    }

    func Close() {
        lock SyncLock {
            Flush()
            LogStream.Dispose()
            Index.Close()
            TimeIndex.Close()
        }
    }
}

// Factory helper for opening or creating an active/sealed LogSegment.
shared class LogSegmentFactory {
    func OpenOrCreate(directoryPath string, baseOffset int64) LogSegment {
        Directory.CreateDirectory(directoryPath)
        var offsetStr string = baseOffset.ToString().PadLeft(20, '0')

        var logPath string = Path.Combine(directoryPath, offsetStr + ".log")
        var indexPath string = Path.Combine(directoryPath, offsetStr + ".index")
        var timePath string = Path.Combine(directoryPath, offsetStr + ".timeindex")

        var fs = FileStream(logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)
        fs.Seek(fs.Length, SeekOrigin.Begin)

        var idx = OffsetIndexFactory.OpenOrCreate(indexPath, baseOffset)
        var timeIdx = TimeIndexFactory.OpenOrCreate(timePath, baseOffset)

        var segment = LogSegment{}
        segment.DirectoryPath = directoryPath
        segment.BaseOffset = baseOffset
        segment.LogStream = fs
        segment.Index = idx
        segment.TimeIndex = timeIdx
        segment.NextOffset = baseOffset
        segment.SyncLock = Object()

        // Recover NextOffset by inspecting the end of the log
        if fs.Length > 0 && idx.EntryCount > 0 {
            var lastEntry = idx.ReadEntry(idx.EntryCount - 1)
            fs.Seek(int64(lastEntry.PhysicalPosition), SeekOrigin.Begin)
            while fs.Position < fs.Length {
                try {
                    var batch = RecordBatchCodec.Deserialize(fs)
                    segment.NextOffset = batch.LastOffset() + 1
                } catch (ex Exception) {
                    break
                }
            }
        }

        return segment
    }
}
