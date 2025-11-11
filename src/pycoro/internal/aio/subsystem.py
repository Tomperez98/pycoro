from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from queue import Queue

    from pycoro.internal.kernel import t_aio
    from pycoro.internal.kernel.bus import CQE, SQE
    from pycoro.internal.kernel.t_api.error import Error


class _SubsystemBase(Protocol):
    def kind(self) -> str: ...
    def start(self, errors: Queue[Error] | None) -> None: ...
    def stop(self) -> None: ...


class Subsystem(_SubsystemBase, Protocol):
    def enqueue(self, sqe: SQE[t_aio.Submission, t_aio.Completion]) -> bool: ...
    def flush(self, time: int) -> None: ...


class SubsystemDST(_SubsystemBase, Protocol):
    def process(
        self, sqes: list[SQE[t_aio.Submission, t_aio.Completion]]
    ) -> list[CQE[t_aio.Submission, t_aio.Completion]]: ...
