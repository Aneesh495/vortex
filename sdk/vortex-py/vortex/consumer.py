# Copyright (c) Vortex Systems. All rights reserved.
# Licensed under the MIT License.

from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class ConsumerConfig:
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "default-group"
    member_id: str = ""
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    max_poll_records: int = 500

@dataclass
class ConsumerRecord:
    topic: str
    partition: int
    offset: int
    timestamp: int
    key: bytes = b""
    value: bytes = b""

    def key_str(self) -> str:
        return self.key.decode("utf-8") if self.key else ""

    def value_str(self) -> str:
        return self.value.decode("utf-8") if self.value else ""

class VortexConsumer:
    def __init__(self, config: Optional[ConsumerConfig] = None):
        self.config = config or ConsumerConfig()
        self.subscribed_topics: List[str] = []
        self.assigned_partitions: List[tuple] = []
        self.positions: Dict[str, int] = {}
        self.committed_offsets: Dict[str, int] = {}
        self.fetch_buffer: List[ConsumerRecord] = []

    def subscribe(self, topics: List[str]) -> None:
        self.subscribed_topics = list(topics)

    def assign(self, partitions: List[tuple]) -> None:
        self.assigned_partitions = list(partitions)
        for tp in partitions:
            key = f"{tp[0]}-{tp[1]}"
            if key not in self.positions:
                self.positions[key] = 0

    def assignment(self) -> List[tuple]:
        return list(self.assigned_partitions)

    def buffer_records(self, records: List[ConsumerRecord]) -> None:
        self.fetch_buffer.extend(records)

    def poll(self, timeout_ms: int = 1000) -> List[ConsumerRecord]:
        take = min(self.config.max_poll_records, len(self.fetch_buffer))
        batch = self.fetch_buffer[:take]
        self.fetch_buffer = self.fetch_buffer[take:]

        for rec in batch:
            key = f"{rec.topic}-{rec.partition}"
            self.positions[key] = rec.offset + 1

        return batch

    def position(self, topic: str, partition: int) -> int:
        key = f"{topic}-{partition}"
        return self.positions.get(key, 0)

    def seek(self, topic: str, partition: int, offset: int) -> None:
        key = f"{topic}-{partition}"
        self.positions[key] = offset

    def commit_sync(self) -> None:
        for tp in self.assigned_partitions:
            key = f"{tp[0]}-{tp[1]}"
            if key in self.positions:
                self.committed_offsets[key] = self.positions[key]

    def committed(self, topic: str, partition: int) -> int:
        key = f"{topic}-{partition}"
        return self.committed_offsets.get(key, -1)
