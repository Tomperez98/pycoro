from __future__ import annotations

from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import TYPE_CHECKING, Final, Protocol

from pycoro.internal.kernel import t_api
from pycoro.internal.kernel.bus import SQE
from pycoro.internal.kernel.t_api.error import Error
from pycoro.internal.kernel.t_api.status import StatusCode

if TYPE_CHECKING:
    from pycoro.internal.api.subsystem import Subsystem
    from pycoro.internal.kernel.bus import CQE


class API(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def shutdown(self) -> None: ...
    def done(self) -> bool: ...
    @property
    def errors(self) -> Queue[Error]: ...
    def signal(self, cancel: Event) -> Event: ...
    def enqueue_sqe(self, sqe: SQE[t_api.Request, t_api.Response]) -> None: ...
    def dequeue_sqe(self, n: int) -> list[SQE[t_api.Request, t_api.Response]]: ...
    def enqueue_cqe(self, cqe: CQE[t_api.Request, t_api.Response]) -> None: ...
    def dequeue_cqe(
        self, cqes: Queue[CQE[t_api.Request, t_api.Response]]
    ) -> CQE[t_api.Request, t_api.Response]: ...


def new(size: int) -> API:
    return _API(size)


class _API:
    def __init__(self, size: int) -> None:
        self.sq: Final = Queue[SQE[t_api.Request, t_api.Response]](size)
        self.buffer: SQE[t_api.Request, t_api.Response] | None = None
        self.subsystems: list[Subsystem] = []
        self.completed: bool = False
        self.errors: Final = Queue[Error]()
        self.threads: list[Thread] = []

    def add_subsystems(self, subsystem: Subsystem) -> None:
        self.subsystems.append(subsystem)

    def addr(self) -> str:
        for subsystem in self.subsystems:
            if subsystem.kind() == "kind":
                return subsystem.addr()

        return ""

    def start(self) -> None:
        for subsystem in self.subsystems:
            t = Thread(target=subsystem.start, daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self) -> None:
        for subsystem in self.subsystems:
            subsystem.stop()

        self.sq.shutdown()
        self.sq.join()

    def shutdown(self) -> None:
        self.completed = True

    def done(self) -> bool:
        return self.completed and self.sq.qsize() == 0

    def signal(self, cancel: Event) -> Event:
        signal_event = Event()

        # If buffer is already filled, signal immediately.
        if self.buffer is not None:
            signal_event.set()
            return signal_event

        def wait_for_signal() -> None:
            while not cancel.is_set():
                try:
                    sqe = self.sq.get(timeout=0.1)
                    assert self.buffer is None, "buffer must be None"
                    self.buffer = sqe
                    signal_event.set()
                except Empty:
                    continue
                else:
                    return
            # Cancel triggered
            signal_event.set()

        Thread(target=wait_for_signal, daemon=True).start()
        return signal_event

    def enqueue_sqe(self, sqe: SQE[t_api.Request, t_api.Response]) -> None:
        assert sqe.submission is not None, "submission must not be None"
        assert sqe.submission.metadata is not None, "submission tags must not be None"

        if self.completed:
            sqe.callback(Error(StatusCode.STATUS_SYSTEM_SHUTTING_DOWN))
            return

        try:
            sqe.submission.validate()
        except Exception as err:
            sqe.callback(Error(StatusCode.STATUS_FIELD_VALIDATION_ERROR, err))
            return

        # Try to enqueue without blocking
        try:
            self.sq.put_nowait(sqe)
        except Full:
            sqe.callback(Error(StatusCode.STATUS_API_SUBMISSION_QUEUE_FULL))

    def dequeue_sqe(self, n: int) -> list[SQE[t_api.Request, t_api.Response]]:
        sqes: list[SQE[t_api.Request, t_api.Response]] = []

        if self.buffer is not None:
            sqes.append(self.buffer)
            self.buffer = None

        for _ in range(n - len(sqes)):
            try:
                sqe = self.sq.get_nowait()  # non-blocking
            except Empty:
                break

            sqes.append(sqe)

        return sqes

    def enqueue_cqe(self, cqe: CQE[t_api.Request, t_api.Response]) -> None:
        return cqe.invoke()

    def dequeue_cqe(
        self, cqes: Queue[CQE[t_api.Request, t_api.Response]]
    ) -> CQE[t_api.Request, t_api.Response]:
        return cqes.get()
