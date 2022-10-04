# Copyright (c) Vortex Systems. All rights reserved.
# Licensed under the MIT License.

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from .protocol import VortexError

@dataclass
class TopicDescription:
    name: str
    partition_count: int
    replication_factor: int
    configs: Dict[str, str] = field(default_factory=dict)

class VortexAdmin:
    def __init__(self):
        self.topics: Dict[str, TopicDescription] = {}

    def create_topic(self, name: str, partition_count: int, replication_factor: int = 1) -> int:
        if not name:
            return VortexError.UNKNOWN_TOPIC_OR_PARTITION
        if partition_count <= 0:
            return VortexError.CORRUPT_MESSAGE
        if name in self.topics:
            return VortexError.TOPIC_ALREADY_EXISTS

        self.topics[name] = TopicDescription(
            name=name,
            partition_count=partition_count,
            replication_factor=replication_factor,
        )
        return VortexError.NONE

    def list_topics(self) -> List[str]:
        return list(self.topics.keys())

    def describe_topic(self, name: str) -> Optional[TopicDescription]:
        return self.topics.get(name)

    def delete_topic(self, name: str) -> int:
        if name not in self.topics:
            return VortexError.UNKNOWN_TOPIC_OR_PARTITION
        del self.topics[name]
        return VortexError.NONE
