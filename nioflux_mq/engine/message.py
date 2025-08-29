import time
import uuid
from dataclasses import dataclass


@dataclass
class Message:
    id: str
    payload: bytes
    timestamp: float
    ttl: float
    tags: list[str]
    timeout: bool = False

    @staticmethod
    def build(payload: bytes, tags: list[str], ttl: float):
        return Message(payload=payload,
                       timestamp=time.perf_counter(),
                       id=uuid.uuid4().hex, tags=tags, ttl=ttl)

    @staticmethod
    def serialize(obj):
        if isinstance(obj, Message):
            return {
                '__class__': Message.__class__.__name__,
                '__dict__': obj.__dict__
            }

    @staticmethod
    def deserialize(dct):
        if dct.get('__class__') == Message.__class__.__name__:
            message = Message.build(b'', [], -1)
            message.__dict__.update(dct['__dict__'])
            return message
        return dct
