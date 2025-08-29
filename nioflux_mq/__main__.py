import asyncio

from nioflux import Server, StrDecode, StrEncode, ErrorNotify

from nioflux_mq.mq import MessageQueue
from nioflux_mq.handler.json_load_handler import JsonLoadHandler
from nioflux_mq.handler.mq_protocol_handler import NioFluxMQProtocolHandler
from nioflux_mq.handler.response_handler import ResponseHandler

MQ = MessageQueue()
server = Server(pipeline=[StrEncode(), JsonLoadHandler(), NioFluxMQProtocolHandler(),
                          StrDecode(), ErrorNotify(), ResponseHandler()],
                extra=MQ)


async def main():
    await server.run()


if __name__ == '__main__':
    asyncio.run(main())
