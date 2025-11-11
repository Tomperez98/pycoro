from __future__ import annotations

from dataclasses import dataclass
from queue import Queue
from typing import TYPE_CHECKING, Final

from pycoro.kernel import t_aio
from pycoro.kernel.bus import SQE

if TYPE_CHECKING:
    from datetime import timedelta

    from pycoro.aio import AIO


@dataclass(frozen=True)
class Config:
    size: int
    batch_size: int
    path: str
    tx_timeout: timedelta
    reset: bool


def new(aio: AIO, config: Config) -> _SqliteStore:
    return _SqliteStore(aio, config)


class _SqliteStore:
    def __init__(self, aio: AIO, config: Config) -> None:
        self.config: Final = config
        self.aio: Final = aio
        self.sq: Final = Queue[SQE[t_aio.Submission, t_aio.Completion]](config.size)
