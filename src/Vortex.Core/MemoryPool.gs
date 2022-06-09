// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.Collections.Concurrent
import System.IO

// A managed byte buffer providing sequential read and write pointers over an allocated byte slice.
class ByteBuffer {
    var Data []byte
    var ReadPos int32 = 0
    var WritePos int32 = 0

    func Capacity() int32 {
        return Data.Length
    }

    func ReadableBytes() int32 {
        return WritePos - ReadPos
    }

    func WritableBytes() int32 {
        return Data.Length - WritePos
    }

    func WriteByte(b byte) {
        if WritePos >= Data.Length {
            throw InvalidOperationException("Buffer overflow: capacity exceeded")
        }
        Data[WritePos] = b
        WritePos = WritePos + 1
    }

    func Write(source []byte, offset int32, count int32) {
        if WritePos + count > Data.Length {
            throw InvalidOperationException("Buffer overflow: insufficient space")
        }
        Buffer.BlockCopy(source, offset, Data, WritePos, count)
        WritePos = WritePos + count
    }

    func ReadByte() byte {
        if ReadPos >= WritePos {
            throw InvalidOperationException("Buffer underflow: no readable bytes")
        }
        var b byte = Data[ReadPos]
        ReadPos = ReadPos + 1
        return b
    }

    func Read(destination []byte, offset int32, count int32) int32 {
        var available int32 = ReadableBytes()
        var toRead int32 = count
        if toRead > available {
            toRead = available
        }
        if toRead <= 0 {
            return 0
        }
        Buffer.BlockCopy(Data, ReadPos, destination, offset, toRead)
        ReadPos = ReadPos + toRead
        return toRead
    }

    func Reset() {
        ReadPos = 0
        WritePos = 0
    }

    func ToArray() []byte {
        var len int32 = ReadableBytes()
        var copy []byte = [len]byte
        Buffer.BlockCopy(Data, ReadPos, copy, 0, len)
        return copy
    }
}

// Thread-safe fixed-size slab allocator pooling ByteBuffers to minimize garbage collection overhead.
class MemoryPool {
    var BlockSize int32
    var MaxBlocks int32
    var Pool ConcurrentBag[ByteBuffer] = ConcurrentBag[ByteBuffer]()
    var AllocatedCount int32 = 0

    func Rent() ByteBuffer {
        var buf ByteBuffer? = nil
        var found = Pool.TryTake(out buf)
        if found && buf != nil {
            buf.Reset()
            return buf
        }

        AllocatedCount = AllocatedCount + 1
        var raw []byte = [BlockSize]byte
        var created = ByteBuffer{}
        created.Data = raw
        return created
    }

    func Return(buffer ByteBuffer) {
        if buffer.Capacity() != BlockSize {
            return
        }
        buffer.Reset()
        if Pool.Count < MaxBlocks {
            Pool.Add(buffer)
        }
    }

    func AvailableBlocks() int32 {
        return Pool.Count
    }
}
