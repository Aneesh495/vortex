// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.Collections.Generic

// Represents an individual event or message within a stream partition.
class Record {
    var Offset int64
    var Timestamp int64
    var Key []byte = [0]byte
    var Value []byte = [0]byte
    var Headers List[RecordHeader] = List[RecordHeader]()

    func IsTombstone() bool {
        return Value.Length == 0
    }

    func KeyString() string {
        if Key.Length == 0 {
            return ""
        }
        return System.Text.Encoding.UTF8.GetString(Key)
    }

    func ValueString() string {
        if Value.Length == 0 {
            return ""
        }
        return System.Text.Encoding.UTF8.GetString(Value)
    }

    func Size() int32 {
        var headerBytes int32 = 0
        if Headers != nil {
            for i in 0 ... Headers.Count {
                headerBytes = headerBytes + Headers[i].Size()
            }
        }
        var kLen int32 = Key.Length
        var vLen int32 = Value.Length
        return 8 + 8 + 4 + kLen + 4 + vLen + headerBytes
    }
}
