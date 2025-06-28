from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from pycoro.io.aio import AIO
from pycoro.io.fio import FIO

if TYPE_CHECKING:
    from pycoro.bus import CQE, SQE


__all__ = ["AIO", "FIO"]


class IO[I, O](Protocol):
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def dispatch(self, sqe: SQE[I, O]) -> None: ...
    def dequeue(self, n: int) -> list[CQE[O]]: ...
    def flush(self, time: int) -> None: ...
