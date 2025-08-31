import asyncio

from nioflux.server.server import DEFAULT_EOT, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE
from nioflux import Server, StrDecode, StrEncode, ErrorNotify

from nioflux_mq.mq import MessageQueue
from nioflux_mq.handler.json_load_handler import JsonLoadHandler
from nioflux_mq.handler.json_dump_handler import JsonDumpHandler
from nioflux_mq.handler.mq_protocol_handler import NioFluxMQProtocolHandler
from nioflux_mq.handler.response_handler import ResponseHandler


class NioFluxMQServer:
    def __init__(self, host: str, port: int, timeout: float = DEFAULT_TIMEOUT,
                 buffer_size: int = DEFAULT_BUFFER_SIZE, eot: bytes = DEFAULT_EOT):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._buffer_size = buffer_size
        self._eot = eot
        self._mq = MessageQueue()
        self._server = Server(pipeline=[StrDecode(), JsonLoadHandler(),
                                        NioFluxMQProtocolHandler(),
                                        JsonDumpHandler(), StrEncode(),
                                        ErrorNotify(), ResponseHandler()],
                              host=self._host, port=self._port,
                              timeout=self._timeout, buffer_size=self._buffer_size,
                              eot=self._eot, extra=self._mq)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def run(self):
        asyncio.run(self._server.run())
