from dataclasses import dataclass
from typing import Any


@dataclass
class Response:
    success: bool
    data: Any
    err: list[Exception]
