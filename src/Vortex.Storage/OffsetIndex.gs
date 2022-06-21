// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import System.IO
import Vortex.Core

// An entry in the sparse offset index mapping logical relative offset to physical file position.
data class IndexEntry(RelativeOffset int32, PhysicalPosition int32)

// Sparse memory-mapped index file (.index) mapping monotonically increasing logical offsets
// to physical file byte positions for rapid O(log N) binary search lookups.
class OffsetIndex {
    var FilePath string
    var BaseOffset int64
    var MaxEntries int32 = 1000000
    var Stream FileStream
    var EntryCount int32 = 0
    var LastRelativeOffset int32 = -1
    var SyncLock Object = Object()

    // Appends a new sparse index entry if the relative offset is greater than the previous entry.
    func Append(offset int64, physicalPosition int32) bool {
        lock SyncLock {
            var relOffset int64 = offset - BaseOffset
            if relOffset < 0 || relOffset > 0x7FFFFFFF {
                throw ArgumentOutOfRangeException("Offset delta exceeds 31-bit relative index range")
            }
            var rel int32 = int32(relOffset)
            if rel <= LastRelativeOffset {
                return false
            }

            Stream.Seek(int64(EntryCount * 8), SeekOrigin.Begin)
            BinaryCodecs.WriteInt32BE(Stream, rel)
            BinaryCodecs.WriteInt32BE(Stream, physicalPosition)

            LastRelativeOffset = rel
            EntryCount = EntryCount + 1
            return true
        }
    }

    // Performs binary search across the 8-byte index entries to locate the greatest offset <= target.
    // Returns the physical file byte position to begin linear scanning from in the .log file.
    func Lookup(offset int64) IndexEntry {
        lock SyncLock {
            if EntryCount == 0 {
                return IndexEntry(0, 0)
            }

            var targetRel int64 = offset - BaseOffset
            if targetRel <= 0 {
                return ReadEntry(0)
            }
            if targetRel >= int64(LastRelativeOffset) {
                return ReadEntry(EntryCount - 1)
            }

            var low int32 = 0
            var high int32 = EntryCount - 1
            var bestIdx int32 = 0

            while low <= high {
                var mid int32 = low + ((high - low) >> 1)
                var entry = ReadEntry(mid)

                if int64(entry.RelativeOffset) <= targetRel {
                    bestIdx = mid
                    low = mid + 1
                } else {
                    high = mid - 1
                }
            }

            return ReadEntry(bestIdx)
        }
    }

    // Reads an index entry at the given entry index.
    func ReadEntry(index int32) IndexEntry {
        var pos int64 = int64(index * 8)
        Stream.Seek(pos, SeekOrigin.Begin)
        var rel int32 = BinaryCodecs.ReadInt32BE(Stream)
        var phys int32 = BinaryCodecs.ReadInt32BE(Stream)
        return IndexEntry(rel, phys)
    }

    // Truncates index entries whose relative offset is strictly greater than the target offset.
    func TruncateTo(offset int64) {
        lock SyncLock {
            var targetRel int64 = offset - BaseOffset
            if targetRel < 0 {
                EntryCount = 0
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
                    LastRelativeOffset = entry.RelativeOffset
                } else {
                    break
                }
            }

            EntryCount = newCount
            if EntryCount == 0 {
                LastRelativeOffset = -1
            }
            Stream.SetLength(int64(EntryCount * 8))
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

// Factory helper for opening or creating an OffsetIndex instance.
shared class OffsetIndexFactory {
    func OpenOrCreate(filePath string, baseOffset int64) OffsetIndex {
        var fs = FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read)
        var length int64 = fs.Length
        var entries int32 = int32(length / 8)

        var idx = OffsetIndex{}
        idx.FilePath = filePath
        idx.BaseOffset = baseOffset
        idx.Stream = fs
        idx.EntryCount = entries
        idx.SyncLock = Object()

        if entries > 0 {
            fs.Seek(int64((entries - 1) * 8), SeekOrigin.Begin)
            idx.LastRelativeOffset = BinaryCodecs.ReadInt32BE(fs)
        } else {
            idx.LastRelativeOffset = -1
        }

        return idx
    }
}
