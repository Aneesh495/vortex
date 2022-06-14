// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.IO
import System.IO.Compression

// Codec enumeration identifying payload compression algorithms.
shared class CompressionType {
    let None int16 = 0
    let GZip int16 = 1
    let Snappy int16 = 2
    let LZ4 int16 = 3
}

// Contract for compression strategies applied to record batch bodies.
interface ICompressionCodec {
    func Type() int16;
    func Compress(data []byte) []byte;
    func Decompress(compressed []byte) []byte;
}

// Identity compression codec performing zero transformations on the payload.
class NoneCompressionCodec : ICompressionCodec {
    func Type() int16 {
        return 0
    }

    func Compress(data []byte) []byte {
        return data
    }

    func Decompress(compressed []byte) []byte {
        return compressed
    }
}

// Standard DEFLATE / GZip compression codec.
class GZipCompressionCodec : ICompressionCodec {
    func Type() int16 {
        return 1
    }

    func Compress(data []byte) []byte {
        if data.Length == 0 {
            return data
        }
        var ms = MemoryStream()
        var gz = GZipStream(ms, CompressionLevel.Fastest)
        gz.Write(data, 0, data.Length)
        gz.Dispose()
        return ms.ToArray()
    }

    func Decompress(compressed []byte) []byte {
        if compressed.Length == 0 {
            return compressed
        }
        var inputMs = MemoryStream(compressed)
        var gz = GZipStream(inputMs, CompressionMode.Decompress)
        var outputMs = MemoryStream()
        gz.CopyTo(outputMs)
        gz.Dispose()
        return outputMs.ToArray()
    }
}

// Factory and dispatcher for record batch compression codecs.
shared class CompressionRegistry {
    func Resolve(typeId int16) ICompressionCodec {
        if typeId == 1 {
            return GZipCompressionCodec{}
        }
        return NoneCompressionCodec{}
    }
}
