// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import System.IO
import Vortex.Core

// An entry in the time-to-offset index mapping wall-clock timestamp to relative offset.
data class TimeIndexEntry(Timestamp int64, RelativeOffset int32)

// Sparse index file (.timeindex) mapping monotonic timestamps to relative logical offsets
// to support fast time-travel and temporal replay queries.
class TimeIndex {
    var FilePath string
    var BaseOffset int64
    var Stream FileStream
    var EntryCount int32 = 0
    var LastTimestamp int64 = -1
    var LastRelativeOffset int32 = -1
    var SyncLock Object = Object()

    // Appends a new timestamp-to-offset mapping if both timestamp and offset strictly increase.
    func Append(timestamp int64, offset int64) bool {
        lock SyncLock {
            var relOffset int64 = offset - BaseOffset
            if relOffset < 0 || relOffset > 0x7FFFFFFF {
                throw ArgumentOutOfRangeException("Offset delta exceeds 31-bit relative index range")
            }
            var rel int32 = int32(relOffset)
            if timestamp <= LastTimestamp || rel <= LastRelativeOffset {
                return false
            }

            Stream.Seek(int64(EntryCount * 12), SeekOrigin.Begin)
            BinaryCodecs.WriteInt64BE(Stream, timestamp)
            BinaryCodecs.WriteInt32BE(Stream, rel)

            LastTimestamp = timestamp
            LastRelativeOffset = rel
            EntryCount = EntryCount + 1
            return true
        }
    }

    // Looks up the index entry with the largest timestamp <= targetTimestamp.
    func Lookup(timestamp int64) TimeIndexEntry {
        lock SyncLock {
            if EntryCount == 0 {
                return TimeIndexEntry(0, 0)
            }

            if timestamp <= ReadEntry(0).Timestamp {
                return ReadEntry(0)
            }
            if timestamp >= LastTimestamp {
                return ReadEntry(EntryCount - 1)
            }

            var low int32 = 0
            var high int32 = EntryCount - 1
            var bestIdx int32 = 0

            while low <= high {
                var mid int32 = low + ((high - low) >> 1)
                var entry = ReadEntry(mid)

                if entry.Timestamp <= timestamp {
                    bestIdx = mid
                    low = mid + 1
                } else {
                    high = mid - 1
                }
            }

            return ReadEntry(bestIdx)
        }
    }

    func ReadEntry(index int32) TimeIndexEntry {
        var pos int64 = int64(index * 12)
        Stream.Seek(pos, SeekOrigin.Begin)
        var ts int64 = BinaryCodecs.ReadInt64BE(Stream)
        var rel int32 = BinaryCodecs.ReadInt32BE(Stream)
        return TimeIndexEntry(ts, rel)
    }

    // Truncates index entries whose relative offset is strictly greater than target offset.
    func TruncateTo(offset int64) {
        lock SyncLock {
            var targetRel int64 = offset - BaseOffset
            if targetRel < 0 {
                EntryCount = 0
                LastTimestamp = -1
                LastRelativeOffset = -1
                Stream.SetLength(0)
                Stream.Flush()
                return
            }

            var newCount int32 = 0
            for i in 0 ... EntryCount {
                var entry = ReadEntry(i)
                if int64(entry.RelativeOffset) <= targetRel {
                    newCount = i + 1
                    LastTimestamp = entry.Timestamp
                    LastRelativeOffset = entry.RelativeOffset
                } else {
                    break
                }
            }

            EntryCount = newCount
            if EntryCount == 0 {
                LastTimestamp = -1
                LastRelativeOffset = -1
            }
            Stream.SetLength(int64(EntryCount * 12))
            Stream.Flush()
        }
    }

    func Flush() {
        lock SyncLock {
            Stream.Flush()
        }
    }

    func Close() {
        lock SyncLock {
            Stream.Flush()
            Stream.Dispose()
        }
    }
}

// Factory helper for opening or creating a TimeIndex instance.
shared class TimeIndexFactory {
    func OpenOrCreate(filePath string, baseOffset int64) TimeIndex {
        var fs = FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)
        var length int64 = fs.Length
        var entries int32 = int32(length / 12)

        var idx = TimeIndex{}
        idx.FilePath = filePath
        idx.BaseOffset = baseOffset
        idx.Stream = fs
        idx.EntryCount = entries
        idx.SyncLock = Object()

        if entries > 0 {
            fs.Seek(int64((entries - 1) * 12), SeekOrigin.Begin)
            idx.LastTimestamp = BinaryCodecs.ReadInt64BE(fs)
            idx.LastRelativeOffset = BinaryCodecs.ReadInt32BE(fs)
        } else {
            idx.LastTimestamp = -1
            idx.LastRelativeOffset = -1
        }

        return idx
    }
}
