// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import System.Collections.Generic
import System.IO
import Vortex.Core

// Manages time-based and size-based retention policies across sealed partition segments.
class RetentionManager {
    var RetentionMs int64 = 604800000 // 7 days default
    var RetentionBytes int64 = -1     // Unlimited by default

    // Enforces configured retention constraints against the partition log.
    // Returns the number of expired segments pruned from disk.
    func EnforceRetention(log PartitionLog, currentTimestampMs int64) int32 {
        var pruned int32 = 0

        // Active segment is always retained
        if log.Segments.Count <= 1 {
            return 0
        }

        var candidates = List[LogSegment]()
        var totalBytes int64 = 0

        for i in 0 ... log.Segments.Count {
            totalBytes = totalBytes + log.Segments[i].SizeBytes()
        }

        // Evaluate retention from oldest to newest sealed segments
        var sealedCount int32 = log.Segments.Count - 1
        for s in 0 ... sealedCount {
            var seg = log.Segments[s]
            var shouldDelete bool = false

            // 1. Time-based retention check
            if RetentionMs > 0 {
                var segAge int64 = currentTimestampMs - seg.TimeIndex.LastTimestamp
                if seg.TimeIndex.LastTimestamp > 0 && segAge > RetentionMs {
                    shouldDelete = true
                }
            }

            // 2. Size-based retention check
            if !shouldDelete && RetentionBytes > 0 && totalBytes > RetentionBytes {
                shouldDelete = true
            }

            if shouldDelete {
                candidates.Add(seg)
                totalBytes = totalBytes - seg.SizeBytes()
                pruned = pruned + 1
            } else {
                break
            }
        }

        if candidates.Count > 0 {
            var surviving = List[LogSegment]()
            for s in 0 ... log.Segments.Count {
                var seg = log.Segments[s]
                var isDeleted bool = false
                for c in 0 ... candidates.Count {
                    if seg.BaseOffset == candidates[c].BaseOffset {
                        isDeleted = true
                        break
                    }
                }

                if isDeleted {
                    seg.Close()
                    var prefix string = seg.BaseOffset.ToString().PadLeft(20, '0')
                    var dir string = seg.DirectoryPath
                    File.Delete(Path.Combine(dir, prefix + ".log"))
                    File.Delete(Path.Combine(dir, prefix + ".index"))
                    File.Delete(Path.Combine(dir, prefix + ".timeindex"))
                } else {
                    surviving.Add(seg)
                }
            }
            log.Segments = surviving
        }

        return pruned
    }
}
