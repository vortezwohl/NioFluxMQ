import asyncio
import json

from typing_extensions import Any
from typing_extensions import override

from nioflux.pipeline.stage import PipelineStage

from nioflux_mq.mq import MessageQueue


class JsonDumpHandler(PipelineStage):
    def __init__(self):
        super().__init__(label='json_dump_handler')

    @override
    async def __call__(self, data: dict, extra: MessageQueue, err: list[Exception], fire: bool,
                       io_ctx: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None) -> tuple[Any, Any, list[Exception], bool]:
        data = json.dumps(data, ensure_ascii=False)
        return data, extra, err, fire
