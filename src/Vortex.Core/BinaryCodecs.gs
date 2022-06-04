// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.IO
import System.Text

// High efficiency binary stream codecs for serializing protocol frames and records.
shared class BinaryCodecs {
    func ZigZagEncode32(n int32) uint32 {
        var shifted int32 = n << 1
        var sign int32 = n >> 31
        return uint32(shifted ^ sign)
    }

    func ZigZagDecode32(n uint32) int32 {
        var shifted int32 = int32(n >> 1)
        var mask int32 = -int32(n & 1)
        return shifted ^ mask
    }

    func ZigZagEncode64(n int64) uint64 {
        var shifted int64 = n << 1
        var sign int64 = n >> 63
        return uint64(shifted ^ sign)
    }

    func ZigZagDecode64(n uint64) int64 {
        var shifted int64 = int64(n >> 1)
        var mask int64 = -int64(n & 1)
        return shifted ^ mask
    }

    func WriteInt16BE(stream Stream, value int16) {
        stream.WriteByte(byte((value >> 8) & 0xFF))
        stream.WriteByte(byte(value & 0xFF))
    }

    func ReadInt16BE(stream Stream) int16 {
        var b0 int32 = stream.ReadByte()
        var b1 int32 = stream.ReadByte()
        if b0 < 0 || b1 < 0 {
            throw EndOfStreamException("Unexpected end of binary stream reading int16")
        }
        return int16((b0 << 8) | b1)
    }

    func WriteInt32BE(stream Stream, value int32) {
        stream.WriteByte(byte((value >> 24) & 0xFF))
        stream.WriteByte(byte((value >> 16) & 0xFF))
        stream.WriteByte(byte((value >> 8) & 0xFF))
        stream.WriteByte(byte(value & 0xFF))
    }

    func ReadInt32BE(stream Stream) int32 {
        var b0 int32 = stream.ReadByte()
        var b1 int32 = stream.ReadByte()
        var b2 int32 = stream.ReadByte()
        var b3 int32 = stream.ReadByte()
        if b0 < 0 || b1 < 0 || b2 < 0 || b3 < 0 {
            throw EndOfStreamException("Unexpected end of binary stream reading int32")
        }
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
    }

    func WriteInt64BE(stream Stream, value int64) {
        WriteInt32BE(stream, int32((value >> 32) & 0xFFFFFFFF))
        WriteInt32BE(stream, int32(value & 0xFFFFFFFF))
    }

    func ReadInt64BE(stream Stream) int64 {
        var high int64 = int64(uint32(ReadInt32BE(stream)))
        var low int64 = int64(uint32(ReadInt32BE(stream)))
        return (high << 32) | low
    }

    func WriteVarint32(stream Stream, value uint32) {
        var curr uint32 = value
        while curr >= 0x80 {
            stream.WriteByte(byte((curr & 0x7F) | 0x80))
            curr = curr >> 7
        }
        stream.WriteByte(byte(curr))
    }

    func ReadVarint32(stream Stream) uint32 {
        var result uint32 = 0
        var shift int32 = 0
        while shift < 35 {
            var b int32 = stream.ReadByte()
            if b < 0 {
                throw EndOfStreamException("Unexpected end of binary stream reading varint32")
            }
            result = result | (uint32(b & 0x7F) << shift)
            if (b & 0x80) == 0 {
                return result
            }
            shift = shift + 7
        }
        throw InvalidDataException("Malformed varint32 exceeding 5 bytes")
    }

    func WriteVarint64(stream Stream, value uint64) {
        var curr uint64 = value
        while curr >= 0x80 {
            stream.WriteByte(byte((curr & 0x7F) | 0x80))
            curr = curr >> 7
        }
        stream.WriteByte(byte(curr))
    }

    func ReadVarint64(stream Stream) uint64 {
        var result uint64 = 0
        var shift int32 = 0
        while shift < 70 {
            var b int32 = stream.ReadByte()
            if b < 0 {
                throw EndOfStreamException("Unexpected end of binary stream reading varint64")
            }
            result = result | (uint64(b & 0x7F) << shift)
            if (b & 0x80) == 0 {
                return result
            }
            shift = shift + 7
        }
        throw InvalidDataException("Malformed varint64 exceeding 10 bytes")
    }

    func WriteString(stream Stream, text string) {
        var bytes []byte = Encoding.UTF8.GetBytes(text)
        WriteInt16BE(stream, int16(bytes.Length))
        stream.Write(bytes, 0, bytes.Length)
    }

    func ReadString(stream Stream) string {
        var length int16 = ReadInt16BE(stream)
        if length <= 0 {
            return ""
        }
        var totalLen int32 = int32(length)
        var buffer []byte = [totalLen]byte
        var readCount int32 = 0
        while readCount < totalLen {
            var r int32 = stream.Read(buffer, readCount, totalLen - readCount)
            if r <= 0 {
                throw EndOfStreamException("Unexpected end of binary stream reading string payload")
            }
            readCount = readCount + r
        }
        return Encoding.UTF8.GetString(buffer)
    }
}
