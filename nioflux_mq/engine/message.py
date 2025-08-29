import time
import uuid
from dataclasses import dataclass


@dataclass
class Message:
    id: str
    payload: str
    timestamp: float
    ttl: float
    tags: list[str]

    @staticmethod
    def build(payload: str, tags: list[str], ttl: float):
        return Message(payload=payload,
                       timestamp=time.perf_counter(),
                       id=uuid.uuid4().hex, tags=tags, ttl=ttl)
