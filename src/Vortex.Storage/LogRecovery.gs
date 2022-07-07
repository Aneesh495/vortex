// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Storage

import System
import System.IO
import Vortex.Core

// Verifies physical segment integrity and performs tail truncation upon unclean shutdown.
shared class LogRecovery {
    func RecoverSegment(segment LogSegment) int64 {
        var fs = segment.LogStream
        fs.Seek(0, SeekOrigin.Begin)

        var validPosition int64 = 0
        var lastValidOffset int64 = segment.BaseOffset - 1
        var recoveredBatches int32 = 0

        while fs.Position < fs.Length {
            var startPos int64 = fs.Position
            try {
                var batch = RecordBatchCodec.Deserialize(fs)
                validPosition = fs.Position
                lastValidOffset = batch.LastOffset()
                recoveredBatches = recoveredBatches + 1
            } catch (ex Exception) {
                // Detected partial or corrupted tail batch. Truncate back to last valid position
                fs.SetLength(validPosition)
                fs.Seek(validPosition, SeekOrigin.Begin)
                segment.Index.TruncateTo(lastValidOffset)
                segment.TimeIndex.TruncateTo(lastValidOffset)
                break
            }
        }

        segment.NextOffset = lastValidOffset + 1
        segment.Flush()
        return segment.NextOffset
    }
}
