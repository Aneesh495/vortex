// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Network

import System
import System.IO
import Vortex.Core
import Vortex.Native

// Standard API key definitions for requests routed across the Vortex binary protocol.
shared class ApiKey {
    let Produce int16 = 0
    let Fetch int16 = 1
    let ListOffsets int16 = 2
    let Metadata int16 = 3
    let OffsetCommit int16 = 8
    let OffsetFetch int16 = 9
    let FindCoordinator int16 = 10
    let JoinGroup int16 = 11
    let Heartbeat int16 = 12
    let LeaveGroup int16 = 13
    let SyncGroup int16 = 14
    let CreateTopics int16 = 19
    let DeleteTopics int16 = 20

    func Describe(key int16) string {
        if key == 0 { return "Produce" }
        if key == 1 { return "Fetch" }
        if key == 2 { return "ListOffsets" }
        if key == 3 { return "Metadata" }
        if key == 8 { return "OffsetCommit" }
        if key == 9 { return "OffsetFetch" }
        if key == 10 { return "FindCoordinator" }
        if key == 11 { return "JoinGroup" }
        if key == 12 { return "Heartbeat" }
        if key == 13 { return "LeaveGroup" }
        if key == 14 { return "SyncGroup" }
        if key == 19 { return "CreateTopics" }
        if key == 20 { return "DeleteTopics" }
        return "UnknownApiKey(" + key.ToString() + ")"
    }
}

// Low-level framed binary wire message with 32-bit length prefix, header, and CRC32C checksum.
class WireFrame {
    var ApiKey int16
    var ApiVersion int16
    var CorrelationId int32
    var ClientId string = ""
    var Payload []byte = [0]byte

    // Encodes the wire frame into the stream with length prefix and CRC32C validation.
    func Serialize(stream Stream) {
        var headerMs = MemoryStream()
        BinaryCodecs.WriteInt16BE(headerMs, ApiKey)
        BinaryCodecs.WriteInt16BE(headerMs, ApiVersion)
        BinaryCodecs.WriteInt32BE(headerMs, CorrelationId)
        BinaryCodecs.WriteString(headerMs, ClientId)

        var headerBytes []byte = headerMs.ToArray()
        var bodyLength int32 = headerBytes.Length + Payload.Length
        var totalLength int32 = bodyLength + 4 // Include 4 bytes for CRC32C

        var checkMs = MemoryStream()
        checkMs.Write(headerBytes, 0, headerBytes.Length)
        if Payload.Length > 0 {
            checkMs.Write(Payload, 0, Payload.Length)
        }

        var checkBytes []byte = checkMs.ToArray()
        var crc uint32 = Crc32C.Compute(checkBytes)

        BinaryCodecs.WriteInt32BE(stream, totalLength)
        stream.Write(checkBytes, 0, checkBytes.Length)
        BinaryCodecs.WriteInt32BE(stream, int32(crc))
    }
}

// Codec helper for reading WireFrame messages from a binary stream.
shared class WireFrameCodec {
    func Deserialize(stream Stream) WireFrame {
        var totalLength int32 = BinaryCodecs.ReadInt32BE(stream)
        if totalLength < 4 {
            throw InvalidDataException("Invalid frame length: too small for CRC32C")
        }

        var bodyLength int32 = totalLength - 4
        var bodyBytes []byte = [bodyLength]byte
        var readTotal int32 = 0
        while readTotal < bodyLength {
            var r int32 = stream.Read(bodyBytes, readTotal, bodyLength - readTotal)
            if r <= 0 {
                throw EndOfStreamException("Unexpected end of stream reading frame body")
            }
            readTotal = readTotal + r
        }

        var expectedCrc uint32 = uint32(BinaryCodecs.ReadInt32BE(stream))
        var actualCrc uint32 = Crc32C.Compute(bodyBytes)
        if actualCrc != expectedCrc {
            throw InvalidDataException("Wire frame CRC32C mismatch. Expected: " + expectedCrc.ToString() + ", Actual: " + actualCrc.ToString())
        }

        var ms = MemoryStream(bodyBytes)
        var apiKey int16 = BinaryCodecs.ReadInt16BE(ms)
        var apiVersion int16 = BinaryCodecs.ReadInt16BE(ms)
        var correlationId int32 = BinaryCodecs.ReadInt32BE(ms)
        var clientId string = BinaryCodecs.ReadString(ms)

        var payloadLen int32 = int32(ms.Length - ms.Position)
        var payloadBytes []byte = [payloadLen]byte
        if payloadLen > 0 {
            ms.Read(payloadBytes, 0, payloadLen)
        }

        var frame = WireFrame{}
        frame.ApiKey = apiKey
        frame.ApiVersion = apiVersion
        frame.CorrelationId = correlationId
        frame.ClientId = clientId
        frame.Payload = payloadBytes
        return frame
    }
}
