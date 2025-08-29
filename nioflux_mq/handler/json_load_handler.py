import asyncio
import json

from typing_extensions import Any
from typing_extensions import override

from nioflux.pipeline.stage import PipelineStage

from nioflux_mq.mq import MessageQueue


class JsonLoadHandler(PipelineStage):
    def __init__(self):
        super().__init__(label='json_load_handler')

    @override
    async def __call__(self, data: str, extra: MessageQueue, err: list[Exception], fire: bool,
                       io_ctx: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None) -> tuple[Any, Any, list[Exception], bool]:
        data = json.loads(data)
        return data, extra, err, fire
