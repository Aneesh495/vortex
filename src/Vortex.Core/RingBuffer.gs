// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.Threading

// High performance bounded ring buffer with power-of-two masking for event sequencing.
class RingBuffer[T] {
    var Buffer []T
    var Mask int64
    var Head int64 = 0
    var Tail int64 = 0
    var SyncLock Object = Object()

    func Capacity() int64 {
        return Mask + 1
    }

    func Count() int64 {
        lock SyncLock {
            return Tail - Head
        }
    }

    func IsEmpty() bool {
        lock SyncLock {
            return Head >= Tail
        }
    }

    func IsFull() bool {
        lock SyncLock {
            return (Tail - Head) >= (Mask + 1)
        }
    }

    // Offers an item into the ring buffer. Returns false if the buffer is currently full.
    func TryEnqueue(item T) bool {
        lock SyncLock {
            if (Tail - Head) >= (Mask + 1) {
                return false
            }
            var slot int32 = int32(Tail & Mask)
            Buffer[slot] = item
            Tail = Tail + 1
            return true
        }
    }

    // Consumes an item from the head of the ring buffer. Returns default when empty.
    func TryDequeue(out item T?) bool {
        lock SyncLock {
            if Head >= Tail {
                item = nil
                return false
            }
            var slot int32 = int32(Head & Mask)
            item = Buffer[slot]
            Head = Head + 1
            return true
        }
    }

    func Clear() {
        lock SyncLock {
            Head = 0
            Tail = 0
        }
    }
}

// Factory helper for initializing generic RingBuffer instances.
shared class RingBufferFactory {
    func Create[T](capacityPow2 int32) RingBuffer[T] {
        var cap int32 = 1
        while cap < capacityPow2 {
            cap = cap << 1
        }

        var rb = RingBuffer[T]{}
        rb.Buffer = [cap]T
        rb.Mask = int64(cap - 1)
        rb.Head = 0
        rb.Tail = 0
        rb.SyncLock = Object()
        return rb
    }
}
