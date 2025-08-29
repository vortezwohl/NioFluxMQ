import asyncio
from typing_extensions import Any
from typing_extensions import override

from nioflux.pipeline.stage import PipelineStage

from nioflux_mq.mq import MessageQueue


class ResponseHandler(PipelineStage):
    def __init__(self):
        super().__init__(label='response_handler')

    @override
    async def __call__(self, data: bytes, extra: MessageQueue, err: list[Exception], fire: bool,
                       io_ctx: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None) -> tuple[Any, Any, list[Exception], bool]:
        io_ctx[1].write(data)
        return data, extra, err, not fire
