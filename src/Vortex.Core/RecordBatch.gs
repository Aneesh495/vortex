// Copyright (c) Vortex Systems. All rights reserved.
// Licensed under the MIT License.

package Vortex.Core

import System
import System.Collections.Generic
import System.IO
import Vortex.Native

// An immutable physical batch of records persisted together to the commit log.
class RecordBatch {
    var BaseOffset int64
    var BatchLength int32
    var PartitionLeaderEpoch int32
    var Magic byte = byte(2)
    var Crc32 uint32
    var Attributes int16
    var LastOffsetDelta int32
    var FirstTimestamp int64
    var MaxTimestamp int64
    var ProducerId int64
    var ProducerEpoch int16
    var BaseSequence int32
    var Records List[Record]

    func LastOffset() int64 {
        return BaseOffset + int64(LastOffsetDelta)
    }

    func RecordCount() int32 {
        return Records.Count
    }

    // Encodes the records into an uncompressed memory buffer and calculates CRC32C.
    func Serialize(stream Stream) {
        var bodyStream = MemoryStream()

        // 1. Write attributes and batch metadata
        BinaryCodecs.WriteInt16BE(bodyStream, Attributes)
        BinaryCodecs.WriteInt32BE(bodyStream, LastOffsetDelta)
        BinaryCodecs.WriteInt64BE(bodyStream, FirstTimestamp)
        BinaryCodecs.WriteInt64BE(bodyStream, MaxTimestamp)
        BinaryCodecs.WriteInt64BE(bodyStream, ProducerId)
        BinaryCodecs.WriteInt16BE(bodyStream, ProducerEpoch)
        BinaryCodecs.WriteInt32BE(bodyStream, BaseSequence)
        BinaryCodecs.WriteInt32BE(bodyStream, Records.Count)

        // 2. Write individual records
        for i in 0 ... Records.Count {
            var rec = Records[i]
            var offsetDelta int32 = int32(rec.Offset - BaseOffset)
            var timestampDelta int64 = rec.Timestamp - FirstTimestamp

            BinaryCodecs.WriteInt32BE(bodyStream, offsetDelta)
            BinaryCodecs.WriteInt64BE(bodyStream, timestampDelta)

            // Write key
            BinaryCodecs.WriteInt32BE(bodyStream, rec.Key.Length)
            if rec.Key.Length > 0 {
                bodyStream.Write(rec.Key, 0, rec.Key.Length)
            }

            // Write value
            BinaryCodecs.WriteInt32BE(bodyStream, rec.Value.Length)
            if rec.Value.Length > 0 {
                bodyStream.Write(rec.Value, 0, rec.Value.Length)
            }

            // Write headers count
            BinaryCodecs.WriteInt32BE(bodyStream, rec.Headers.Count)
            for h in 0 ... rec.Headers.Count {
                var hdr = rec.Headers[h]
                BinaryCodecs.WriteString(bodyStream, hdr.Key)
                BinaryCodecs.WriteInt32BE(bodyStream, hdr.Value.Length)
                if hdr.Value.Length > 0 {
                    bodyStream.Write(hdr.Value, 0, hdr.Value.Length)
                }
            }
        }

        var bodyBytes []byte = bodyStream.ToArray()
        Crc32 = Crc32C.Compute(bodyBytes)
        BatchLength = 4 + 1 + 4 + bodyBytes.Length

        // Write batch header to destination stream
        BinaryCodecs.WriteInt64BE(stream, BaseOffset)
        BinaryCodecs.WriteInt32BE(stream, BatchLength)
        BinaryCodecs.WriteInt32BE(stream, PartitionLeaderEpoch)
        stream.WriteByte(Magic)
        BinaryCodecs.WriteInt32BE(stream, int32(Crc32))
        stream.Write(bodyBytes, 0, bodyBytes.Length)
    }
}

// Deserializer and factory codecs for RecordBatch structures.
shared class RecordBatchCodec {
    func Deserialize(stream Stream) RecordBatch {
        var baseOffset int64 = BinaryCodecs.ReadInt64BE(stream)
        var batchLength int32 = BinaryCodecs.ReadInt32BE(stream)
        var leaderEpoch int32 = BinaryCodecs.ReadInt32BE(stream)
        var magic int32 = stream.ReadByte()
        if magic < 0 {
            throw EndOfStreamException("Unexpected EOF reading magic byte")
        }
        var expectedCrc uint32 = uint32(BinaryCodecs.ReadInt32BE(stream))

        var bodyLength int32 = batchLength - (4 + 1 + 4)
        if bodyLength <= 0 {
            throw InvalidDataException("Invalid record batch body length")
        }

        var bodyBytes []byte = [bodyLength]byte
        var readTotal int32 = 0
        while readTotal < bodyLength {
            var r int32 = stream.Read(bodyBytes, readTotal, bodyLength - readTotal)
            if r <= 0 {
                throw EndOfStreamException("Unexpected EOF reading batch payload")
            }
            readTotal = readTotal + r
        }

        var actualCrc uint32 = Crc32C.Compute(bodyBytes)
        if actualCrc != expectedCrc {
            throw InvalidDataException("Record batch CRC32C mismatch")
        }

        var bodyMs = MemoryStream(bodyBytes)
        var attributes int16 = BinaryCodecs.ReadInt16BE(bodyMs)
        var lastOffsetDelta int32 = BinaryCodecs.ReadInt32BE(bodyMs)
        var firstTimestamp int64 = BinaryCodecs.ReadInt64BE(bodyMs)
        var maxTimestamp int64 = BinaryCodecs.ReadInt64BE(bodyMs)
        var producerId int64 = BinaryCodecs.ReadInt64BE(bodyMs)
        var producerEpoch int16 = BinaryCodecs.ReadInt16BE(bodyMs)
        var baseSequence int32 = BinaryCodecs.ReadInt32BE(bodyMs)
        var recordCount int32 = BinaryCodecs.ReadInt32BE(bodyMs)

        var records = List[Record]()
        for i in 0 ... recordCount {
            var offsetDelta int32 = BinaryCodecs.ReadInt32BE(bodyMs)
            var timestampDelta int64 = BinaryCodecs.ReadInt64BE(bodyMs)

            var keyLen int32 = BinaryCodecs.ReadInt32BE(bodyMs)
            var keyBytes []byte = [keyLen]byte
            if keyLen > 0 {
                bodyMs.Read(keyBytes, 0, keyLen)
            }

            var valLen int32 = BinaryCodecs.ReadInt32BE(bodyMs)
            var valBytes []byte = [valLen]byte
            if valLen > 0 {
                bodyMs.Read(valBytes, 0, valLen)
            }

            var headerCount int32 = BinaryCodecs.ReadInt32BE(bodyMs)
            var headers = List[RecordHeader]()
            for h in 0 ... headerCount {
                var hKey string = BinaryCodecs.ReadString(bodyMs)
                var hValLen int32 = BinaryCodecs.ReadInt32BE(bodyMs)
                var hValBytes []byte = [hValLen]byte
                if hValLen > 0 {
                    bodyMs.Read(hValBytes, 0, hValLen)
                }
                headers.Add(RecordHeader(hKey, hValBytes))
            }

            var rec = Record{}
            rec.Offset = baseOffset + int64(offsetDelta)
            rec.Timestamp = firstTimestamp + timestampDelta
            rec.Key = keyBytes
            rec.Value = valBytes
            rec.Headers = headers
            records.Add(rec)
        }

        var batch = RecordBatch{}
        batch.BaseOffset = baseOffset
        batch.BatchLength = batchLength
        batch.PartitionLeaderEpoch = leaderEpoch
        batch.Magic = byte(magic)
        batch.Crc32 = expectedCrc
        batch.Attributes = attributes
        batch.LastOffsetDelta = lastOffsetDelta
        batch.FirstTimestamp = firstTimestamp
        batch.MaxTimestamp = maxTimestamp
        batch.ProducerId = producerId
        batch.ProducerEpoch = producerEpoch
        batch.BaseSequence = baseSequence
        batch.Records = records
        return batch
    }
}
