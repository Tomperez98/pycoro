from __future__ import annotations

import threading
from dataclasses import dataclass, field
from queue import Queue, ShutDown
from threading import Event
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pycoro.kernel import t_api
    from pycoro.kernel.bus import CQE, SQE


class Subsystem(Protocol):
    def kind(self) -> str: ...
    def addr(self) -> str: ...
    def start(self, q: Queue[Exception]) -> None: ...
    def stop(self) -> None: ...


class API(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def shutdown(self) -> None: ...
    def done(self) -> bool: ...
    def errors(self) -> Queue[Exception]: ...
    def signal(self) -> None: ...
    def enqueue_sqe(self, sqe: SQE[t_api.Request, t_api.Response]) -> None: ...
    def dequeue_sqe(self, n: int) -> list[SQE[t_api.Request, t_api.Response]]: ...
    def enqueue_cqe(self, cqe: CQE[t_api.Response]) -> None: ...
    def dequeue_cqe(self, cq: Queue[CQE[t_api.Response]]) -> CQE[t_api.Response]: ...


@dataclass
class _API:
    sq: Queue[SQE[t_api.Request, t_api.Response]]
    _errors: Queue[Exception] = field(default_factory=lambda: Queue())
    _buffer: SQE[t_api.Request, t_api.Response] | None = None
    _subsystems: list[Subsystem] = field(default_factory=list)
    _done: bool = False

    def add_subsystem(self, subsystem: Subsystem) -> None:
        self._subsystems.append(subsystem)

    def addr(self) -> str:
        for subsystem in self._subsystems:
            if subsystem.kind() == "http":
                return subsystem.addr()

        return ""

    def start(self) -> None:
        for subsystem in self._subsystems:
            subsystem.start(self._errors)

    def stop(self) -> None:
        for subsystem in self._subsystems:
            subsystem.stop()

        self.sq.shutdown()

    def shutdown(self) -> None:
        assert not self._done
        self._done = True

    def done(self) -> bool:
        return self.done and self.sq.qsize() == 0

    def errors(self) -> Queue[Exception]:
        return self._errors

    def signal(self) -> Event:
        ch = threading.Event()  # signal channel

        if self._buffer is not None:
            ch.set()
            return ch

        def _() -> None:
            try:
                sqe = self.sq.get()
            except ShutDown:
                return

            assert self._buffer is None
            self._buffer = sqe
            ch.set()

        threading.Thread(target=_, daemon=True).start()
        return ch


def new(size: int) -> _API:
    return _API(sq=Queue(size))
