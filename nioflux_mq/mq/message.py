import time
import uuid
from dataclasses import dataclass


@dataclass
class Message:
    id: str
    payload: bytes
    timestamp: float
    ttl: float
    timeout: bool = False

    @staticmethod
    def build(payload: bytes, ttl: float):
        return Message(payload=payload,
                       timestamp=time.perf_counter(),
                       id=uuid.uuid4().hex, ttl=ttl)

    @staticmethod
    def serialize(obj):
        if isinstance(obj, Message):
            obj.__dict__['payload'] = obj.__dict__['payload'].decode('utf-8')
            return {
                '__class__': 'Message',
                '__dict__': obj.__dict__
            }

    @staticmethod
    def deserialize(dct):
        if dct.get('__class__') == 'Message':
            message = Message.build(b'', -1)
            dct['__dict__']['payload'] = dct['__dict__']['payload'].encode('utf-8')
            message.__dict__.update(dct['__dict__'])
            return message
        return dct
