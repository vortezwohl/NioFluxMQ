from threading import RLock

from nioflux_mq.engine.message import Message


class MessageQueue:
    def __init__(self):
        """
        Lock order:
        queue_pool_lock -> consumer_topic_offset_lock -> consumer_pool_lock -> topic_pool_lock
        """
        self._topic_pool = []
        self.__topic_pool_lock = RLock()
        self._consumer_pool = []
        self.__consumer_pool_lock = RLock()
        self._consumer_topic_offset = dict()
        self.__consumer_topic_offset_lock = RLock()
        self._queue_pool = dict()
        self.__queue_pool_lock = RLock()

    @property
    def topics(self):
        with self.__topic_pool_lock:
            return self._topic_pool.copy()

    @property
    def consumers(self):
        with self.__consumer_pool_lock:
            return self._consumer_pool.copy()

    @property
    def consumer_topic_offset(self):
        with self.__consumer_topic_offset_lock:
            return self._consumer_topic_offset.copy()

    @property
    def queues(self):
        with self.__queue_pool_lock:
            return self._queue_pool.copy()

    def register_topic(self, topic: str):
        with self.__queue_pool_lock:
            with self.__topic_pool_lock:
                if topic in self._topic_pool:
                    return
                self._topic_pool.append(topic)
                self._queue_pool[topic] = []

    def unregister_topic(self, topic: str) -> list:
        with self.__queue_pool_lock:
            if topic not in self._queue_pool:
                return []
            with self.__topic_pool_lock:
                self._topic_pool.remove(topic)
            with self.__consumer_topic_offset_lock:
                for k in self._consumer_topic_offset.keys():
                    if topic in list(self._consumer_topic_offset[k].keys()):
                        del self._consumer_topic_offset[k][topic]
            queue = self._queue_pool[topic]
            del self._queue_pool[topic]
            return queue

    def register_consumer(self, consumer: str):
        with self.__consumer_pool_lock:
            if consumer in self._consumer_pool:
                return
            self._consumer_pool.append(consumer)
            with self.__consumer_topic_offset_lock:
                self._consumer_topic_offset[consumer] = dict()

    def unregister_consumer(self, consumer: str):
        with self.__consumer_pool_lock:
            if consumer not in self._consumer_pool:
                return
            self._consumer_pool.remove(consumer)
            with self.__consumer_topic_offset_lock:
                if consumer in self._consumer_topic_offset.keys():
                    del self._consumer_topic_offset[consumer]

    def produce(self, message: bytes, topic: str | None = None, tags: list[str] | None = None, ttl: float = -1.):
        tags = tags if tags is not None else []
        with self.__queue_pool_lock:
            if topic is not None:
                assert topic in self._queue_pool, f'topic "{topic}" does\'t exist.'
                self._queue_pool[topic].append(Message.build(payload=message, tags=tags, ttl=ttl))
            else:
                with self.__topic_pool_lock:
                    for k in self._topic_pool:
                        self._queue_pool[k].append(Message.build(payload=message, tags=tags, ttl=ttl))

    def peek(self, consumer: str, topic: str, tags: list[str] | None = None) -> Message | None:
        tags = tags if tags is not None else []
        with self.__queue_pool_lock:
            assert topic in self._queue_pool, f'topic "{topic}" does\'t exist.'
            with self.__consumer_topic_offset_lock:
                offset = self._consumer_topic_offset[consumer].get(topic, 0)
                for i in range(offset, len(self._queue_pool[topic])):
                    message = self._queue_pool[topic][i]
                    if len(tags) < 1 or len([_ for _ in tags if _ in message.tags]) > 0:
                        return message
                return None

    def consume(self, consumer: str, topic: str, tags: list[str] | None = None) -> Message | None:
        tags = tags if tags is not None else []
        with self.__queue_pool_lock:
            with self.__consumer_topic_offset_lock:
                assert topic in self._queue_pool, f'topic "{topic}" does\'t exist.'
                actual_message = None
                offset = self._consumer_topic_offset[consumer].get(topic, 0)
                for i in range(offset, len(self._queue_pool[topic])):
                    message = self._queue_pool[topic][i]
                    if len(tags) < 1 or len([_ for _ in tags if _ in message.tags]) > 0:
                        actual_message = message
                        break
                if consumer not in self._consumer_topic_offset.keys():
                    self._consumer_topic_offset[consumer] = dict()
                if topic not in self._consumer_topic_offset[consumer]:
                    self._consumer_topic_offset[consumer][topic] = 0
                self._consumer_topic_offset[consumer][topic] += 1
                return actual_message

    def advance(self, consumer: str, topic: str, n: int = 1):
        with self.__consumer_topic_offset_lock:
            if consumer not in self._consumer_topic_offset.keys():
                self._consumer_topic_offset[consumer] = dict()
            if topic not in self._consumer_topic_offset[consumer]:
                self._consumer_topic_offset[consumer][topic] = 0
            self._consumer_topic_offset[consumer][topic] += n

    def retreat(self, consumer: str, topic: str, n: int = 1):
        with self.__consumer_topic_offset_lock:
            if consumer not in self._consumer_topic_offset.keys():
                self._consumer_topic_offset[consumer] = dict()
            if topic not in self._consumer_topic_offset[consumer]:
                self._consumer_topic_offset[consumer][topic] = 0
            self._consumer_topic_offset[consumer][topic] = max(self._consumer_topic_offset[consumer][topic] - n, 0)
