// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.Collections.Generic

// Represents an individual event or message within a stream partition.
class Record {
    var Offset int64
    var Timestamp int64
    var Key []byte
    var Value []byte
    var Headers List[RecordHeader]

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
        for i in 0 ... Headers.Count {
            headerBytes = headerBytes + Headers[i].Size()
        }
        return 8 + 8 + 4 + Key.Length + 4 + Value.Length + headerBytes
    }
}
