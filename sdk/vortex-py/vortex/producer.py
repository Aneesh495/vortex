# Copyright (c) Vortex Systems. All rights reserved.
# Licensed under the MIT License.

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from .protocol import Record, RecordBatch

@dataclass
class ProducerConfig:
    bootstrap_servers: str = "localhost:9092"
    batch_size: int = 16384
    linger_ms: int = 5
    acks: int = 1
    timeout_ms: int = 30000
    compression: int = 0
    max_retries: int = 3

@dataclass
class RecordMetadata:
    topic: str
    partition: int
    offset: int
    timestamp: int
    error_code: int = 0

class Partitioner:
    def __init__(self):
        self._counter = 0

    def partition(self, key: Optional[bytes], num_partitions: int) -> int:
        if num_partitions <= 0:
            return 0
        if key:
            h = 2166136261
            for b in key:
                h = ((h ^ b) * 16777619) & 0xFFFFFFFF
            return h % num_partitions
        p = self._counter % num_partitions
        self._counter += 1
        return p

class VortexProducer:
    def __init__(self, config: Optional[ProducerConfig] = None):
        self.config = config or ProducerConfig()
        self.partitioner = Partitioner()
        self.metadata: Dict[str, int] = {}
        self.accumulator: Dict[str, List[Record]] = {}
        self.sent_records_count = 0

    def register_topic_metadata(self, topic: str, num_partitions: int) -> None:
        self.metadata[topic] = num_partitions

    def get_partition_count(self, topic: str) -> int:
        return self.metadata.get(topic, 1)

    def send(self, topic: str, key: Optional[bytes], value: bytes) -> RecordMetadata:
        num_partitions = self.get_partition_count(topic)
        partition = self.partitioner.partition(key, num_partitions)
        return self.send_to_partition(topic, partition, key, value)

    def send_to_partition(self, topic: str, partition: int, key: Optional[bytes], value: bytes) -> RecordMetadata:
        bucket_key = f"{topic}:{partition}"
        if bucket_key not in self.accumulator:
            self.accumulator[bucket_key] = []

        now_ms = int(time.time() * 1000)
        offset = len(self.accumulator[bucket_key])
        rec = Record(
            offset=offset,
            timestamp=now_ms,
            key=key or b"",
            value=value,
        )
        self.accumulator[bucket_key].append(rec)
        self.sent_records_count += 1

        return RecordMetadata(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=now_ms,
            error_code=0,
        )

    def drain_batches(self) -> List[tuple]:
        drained = []
        for bucket_key, records in list(self.accumulator.items()):
            if not records:
                continue
            topic, p_str = bucket_key.split(":")
            partition = int(p_str)

            batch = RecordBatch(
                base_offset=records[0].offset,
                magic=2,
                attributes=self.config.compression,
                producer_id=1,
                producer_epoch=0,
                first_timestamp=records[0].timestamp,
                max_timestamp=records[-1].timestamp,
                last_offset_delta=records[-1].offset - records[0].offset,
                records=records,
            )
            drained.append((topic, partition, batch.serialize()))

        self.accumulator.clear()
        return drained
