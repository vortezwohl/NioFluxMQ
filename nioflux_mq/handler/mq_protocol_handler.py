import asyncio
import os

from typing_extensions import Any, override

from nioflux import PipelineStage

from nioflux_mq.snapshot import __PATH__
from nioflux_mq.mq.message_queue import MessageQueue


class NioFluxMQProtocolHandler(PipelineStage):
    def __init__(self):
        super().__init__(label='nioflux_mq_protocol_handler')

    @override
    async def __call__(self, data: dict, extra: MessageQueue, err: list[Exception], fire: bool,
                       io_ctx: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None) -> tuple[Any, Any, list[Exception], bool]:
        mq = extra
        instruction = data['instruction']
        payload = data['payload']
        resp = {'success': True, 'info': None, 'err': []}
        try:
            match instruction:
                case 'snapshot':
                    path = os.path.join(os.getenv('MQ_SNAPSHOT_DIR', __PATH__), 'snapshot')
                    extra.save(path=path)
                case 'topics':
                    resp['info'] = mq.topics
                case 'consumers':
                    resp['info'] = mq.consumers
                case 'register_topic':
                    mq.register_topic(**payload)
                case 'unregister_topic':
                    mq.unregister_topic(**payload)
                case 'register_consumer':
                    mq.register_consumer(**payload)
                case 'unregister_consumer':
                    mq.unregister_consumer(**payload)
                case 'produce':
                    payload['message'] = payload['message'].encode('utf-8')
                    mq.produce(**payload)
                case 'consume':
                    resp['info'] = mq.consume(**payload)
                case 'advance':
                    mq.advance(**payload)
                case 'retreat':
                    mq.retreat(**payload)
                case _:
                    raise ValueError(f'Unsupported instruction: {instruction}')
        except Exception as e:
            err.append(e)
            resp['success'] = False
        return resp, mq, err, fire
