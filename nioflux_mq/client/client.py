import json

from nioflux.util import tcp_send
from nioflux.server.server import DEFAULT_EOT

from nioflux_mq.mq.message import Message
from nioflux_mq.client.response import Response


class NioFluxMQClient:
    def __init__(self, host: str, port: int, eot: bytes = DEFAULT_EOT):
        self._host = host
        self._port = port
        self._eot = eot

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @staticmethod
    def connect(host: str, port: int, eot: bytes = DEFAULT_EOT):
        return NioFluxMQClient(host=host, port=port, eot=eot)

    @staticmethod
    def response_postprocess(response: bytes) -> Response:
        _dict = json.loads(response.decode('utf-8'))
        if isinstance(_dict['info'], dict):
            if _dict['info'].get('__class__') == 'Message':
                _dict['info'] = json.loads(json.dumps(_dict['info'], ensure_ascii=True),
                                           object_hook=Message.deserialize)
        return Response(
            success=_dict['success'],
            data=_dict['info'],
            err=_dict['err']
        )

    @property
    def topics(self) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'topics',
            'payload': None
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    @property
    def consumers(self) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'consumers',
            'payload': None
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def snapshot(self) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'snapshot',
            'payload': None
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def register_topic(self, topic: str) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'register_topic',
            'payload': {
                'topic': topic
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def unregister_topic(self, topic: str) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'unregister_topic',
            'payload': {
                'topic': topic
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def register_consumer(self, consumer: str) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'register_consumer',
            'payload': {
                'consumer': consumer
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def unregister_consumer(self, consumer: str) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'unregister_consumer',
            'payload': {
                'consumer': consumer
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def produce(self, message: bytes, topic: str | None = None, ttl: float = -1.) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'produce',
            'payload': {
                'message': message.decode('utf-8'),
                'topic': topic,
                'ttl': ttl
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def consume(self, consumer: str, topic: str) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'consume',
            'payload': {
                'consumer': consumer,
                'topic': topic
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def advance(self, consumer: str, topic: str, n: int = 1) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'advance',
            'payload': {
                'consumer': consumer,
                'topic': topic,
                'n': n
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))

    def retreat(self, consumer: str, topic: str, n: int = 1) -> Response:
        return self.response_postprocess(tcp_send(json.dumps({
            'instruction': 'retreat',
            'payload': {
                'consumer': consumer,
                'topic': topic,
                'n': n
            }
        }).encode('utf-8') + self._eot, host=self._host, port=self._port, wait=True))
