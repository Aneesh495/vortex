# Copyright (c) Vortex Systems. All rights reserved.
# Licensed under the MIT License.

import struct
import zlib
from dataclasses import dataclass, field
from typing import List, Optional

class ApiKey:
    PRODUCE = 0
    FETCH = 1
    METADATA = 3
    OFFSET_COMMIT = 8
    OFFSET_FETCH = 9
    JOIN_GROUP = 11
    HEARTBEAT = 12
    LEAVE_GROUP = 13
    SYNC_GROUP = 14

class VortexError:
    NONE = 0
    OFFSET_OUT_OF_RANGE = 1
    CORRUPT_MESSAGE = 2
    UNKNOWN_TOPIC_OR_PARTITION = 3
    LEADER_NOT_AVAILABLE = 4
    NOT_LEADER_OR_FOLLOWER = 5
    REQUEST_TIMED_OUT = 6
    RECORD_TOO_LARGE = 7
    NETWORK_EXCEPTION = 8
    ILLEGAL_GENERATION = 9
    UNKNOWN_MEMBER_ID = 10
    REBALANCE_IN_PROGRESS = 11
    COMMIT_FAILED = 14
    TOPIC_ALREADY_EXISTS = 15

@dataclass
class RecordHeader:
    key: str
    value: bytes

@dataclass
class Record:
    offset: int = 0
    timestamp: int = 0
    key: bytes = b""
    value: bytes = b""
    headers: List[RecordHeader] = field(default_factory=list)

@dataclass
class RecordBatch:
    base_offset: int = 0
    partition_leader_epoch: int = 0
    magic: int = 2
    crc32: int = 0
    attributes: int = 0
    last_offset_delta: int = 0
    first_timestamp: int = 0
    max_timestamp: int = 0
    producer_id: int = 1
    producer_epoch: int = 0
    base_sequence: int = 0
    records: List[Record] = field(default_factory=list)

    def serialize(self) -> bytes:
        body = bytearray()
        body.extend(struct.pack(">h", self.attributes))
        body.extend(struct.pack(">i", self.last_offset_delta))
        body.extend(struct.pack(">q", self.first_timestamp))
        body.extend(struct.pack(">q", self.max_timestamp))
        body.extend(struct.pack(">q", self.producer_id))
        body.extend(struct.pack(">h", self.producer_epoch))
        body.extend(struct.pack(">i", self.base_sequence))
        body.extend(struct.pack(">i", len(self.records)))

        for rec in self.records:
            offset_delta = int(rec.offset - self.base_offset)
            ts_delta = int(rec.timestamp - self.first_timestamp)
            body.extend(struct.pack(">i", offset_delta))
            body.extend(struct.pack(">q", ts_delta))
            body.extend(struct.pack(">i", len(rec.key)))
            if rec.key:
                body.extend(rec.key)
            body.extend(struct.pack(">i", len(rec.value)))
            if rec.value:
                body.extend(rec.value)
            body.extend(struct.pack(">i", len(rec.headers)))
            for hdr in rec.headers:
                k_bytes = hdr.key.encode("utf-8")
                body.extend(struct.pack(">h", len(k_bytes)))
                body.extend(k_bytes)
                body.extend(struct.pack(">i", len(hdr.value)))
                body.extend(hdr.value)

        crc = zlib.crc32(body) & 0xFFFFFFFF
        batch_length = 4 + 1 + 4 + len(body)

        header = bytearray()
        header.extend(struct.pack(">q", self.base_offset))
        header.extend(struct.pack(">i", batch_length))
        header.extend(struct.pack(">i", self.partition_leader_epoch))
        header.append(self.magic)
        header.extend(struct.pack(">I", crc))
        header.extend(body)
        return bytes(header)

@dataclass
class WireFrame:
    api_key: int
    api_version: int
    correlation_id: int
    client_id: str
    payload: bytes

    def encode(self) -> bytes:
        header_buf = bytearray()
        header_buf.extend(struct.pack(">h", self.api_key))
        header_buf.extend(struct.pack(">h", self.api_version))
        header_buf.extend(struct.pack(">i", self.correlation_id))
        cid_bytes = self.client_id.encode("utf-8")
        header_buf.extend(struct.pack(">h", len(cid_bytes)))
        header_buf.extend(cid_bytes)

        body_buf = bytearray()
        body_buf.extend(header_buf)
        body_buf.extend(self.payload)

        crc = zlib.crc32(body_buf) & 0xFFFFFFFF
        total_length = 4 + len(body_buf)

        frame = bytearray()
        frame.extend(struct.pack(">i", total_length))
        frame.extend(struct.pack(">I", crc))
        frame.extend(body_buf)
        return bytes(frame)

    @classmethod
    def decode(cls, data: bytes) -> "WireFrame":
        if len(data) < 8:
            raise ValueError("Frame data too short for header")
        total_len = struct.unpack(">i", data[0:4])[0]
        crc = struct.unpack(">I", data[4:8])[0]
        body = data[8:4 + total_len]

        calc_crc = zlib.crc32(body) & 0xFFFFFFFF
        if crc != calc_crc:
            raise ValueError("CRC32 mismatch in wire frame")

        api_key, api_version, correlation_id, cid_len = struct.unpack(">hhih", body[0:10])
        client_id = body[10:10 + cid_len].decode("utf-8")
        payload = body[10 + cid_len:]

        return cls(
            api_key=api_key,
            api_version=api_version,
            correlation_id=correlation_id,
            client_id=client_id,
            payload=payload,
        )
