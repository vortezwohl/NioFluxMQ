from threading import RLock

from vortezwohl.cache import BaseCache

from nioflux_mq.engine.message import Message


class MessageQueue:
    def __init__(self):
        self._topic_pool = []
        self.__topic_pool_lock = RLock()
        self._consumer_pool = []
        self.__consumer_pool_lock = RLock()
        self._consumer_topic_offset = dict()
        self.__consumer_topic_offset_lock = RLock()
        self._queue_pool = BaseCache()

    @property
    def topics(self):
        return self._topic_pool

    @property
    def consumers(self):
        return self._consumer_pool

    @property
    def consumer_topic_offset(self):
        return self._consumer_topic_offset

    @property
    def queues(self):
        return self._queue_pool

    def register_topic(self, topic: str):
        assert topic not in self._queue_pool, f'topic "{topic}" already registered.'
        with self.__topic_pool_lock:
            self._topic_pool.append(topic)
        self._queue_pool[topic] = []

    def unregister_topic(self, topic: str) -> list:
        assert topic in self._queue_pool, f'topic "{topic}" does\'t exist.'
        with self.__topic_pool_lock:
            self._topic_pool.remove(topic)
        queue = self._queue_pool[topic]
        del self._queue_pool[topic]
        return queue

    def register_consumer(self, consumer: str):
        with self.__consumer_pool_lock:
            self._consumer_pool.append(consumer)
            with self.__consumer_topic_offset_lock:
                self._consumer_topic_offset[consumer] = dict()

    def unregister_consumer(self, consumer: str):
        with self.__consumer_pool_lock:
            self._consumer_pool.remove(consumer)
            with self.__consumer_topic_offset_lock:
                if consumer in self._consumer_topic_offset.keys():
                    del self._consumer_topic_offset[consumer]

    def push(self, message: bytes, tags: list[str], ttl: float = -1., topic: str | None = None):
        if topic is not None:
            assert topic in self._queue_pool, f'topic "{topic}" does\'t exist.'
            self._queue_pool[topic].append(Message.build(payload=message, tags=tags, ttl=ttl))
        for k in self._topic_pool:
            self._queue_pool[k].append(Message.build(payload=message, tags=tags, ttl=ttl))

    def pull(self, consumer: str, tags: list[str], topic: str) -> Message | None:
        assert topic in self._queue_pool, f'topic "{topic}" does\'t exist.'
        offset = self._consumer_topic_offset[consumer].get(topic, 0)
        for message in self._queue_pool[topic][offset:]:
            if len(tags) < 1 or len([_ for _ in tags if _ in message.tags]) > 0:
                return message
        return None

    def ack(self, consumer: str, topic: str):
        with self.__consumer_topic_offset_lock:
            if consumer not in self._consumer_topic_offset.keys():
                self._consumer_topic_offset[consumer] = dict()
            if topic not in self._consumer_topic_offset[consumer]:
                self._consumer_topic_offset[consumer][topic] = 0
            self._consumer_topic_offset[consumer][topic] += 1
