from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pycoro.bus import CQE, SQE


class IO[I, O](Protocol):
    def dispatch(self, sqe: SQE[I, O]) -> None: ...
    def dequeue(self, n: int) -> list[CQE[O]]: ...
    def shutdown(self) -> None: ...
