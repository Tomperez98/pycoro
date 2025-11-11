from __future__ import annotations

from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Final, Protocol

if TYPE_CHECKING:
    from pycoro.internal.api.subsystem import Subsystem


class API(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def shutdown(self) -> None: ...
    def done(self) -> bool: ...
    @property
    def errors(self) -> Queue[Exception]: ...
    def signal(self, cancel: Event) -> Event: ...
    def enqueue_sqe(self, sqe: Any) -> None: ...
    def dequeue_sqe(self, n: int) -> list[Any]: ...
    def enqueue_cqe(self, cqe: Any) -> None: ...
    def dequeue_cqe(self, cqes: Queue[Any]) -> Any: ...


def new(size: int) -> API:
    return _API(size)


class _API:
    def __init__(self, size: int) -> None:
        self.sq: Final = Queue[Any](size)
        self.buffer: Any | None = None
        self.subsystems: list[Subsystem] = []
        self.completed: bool = False
        self.errors: Final = Queue[Exception]()
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
            nonlocal signal_event
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

    def enqueue_sqe(self, sqe: Any) -> None:
        assert sqe.submission is not None, "submission must not be None"
        assert sqe.submission.metadata is not None, "submission tags must not be None"

        if self.completed:
            sqe.callback(None, Exception("STATUS_SYSTEM_SHUTTING_DOWN"))
            return

        try:
            sqe.submission.validate()
        except Exception as err:
            sqe.callback(None, Exception("STATUS_FIELD_VALIDATION_ERROR", err))
            return

        # Try to enqueue without blocking
        try:
            self.sq.put_nowait(sqe)
        except Full:
            sqe.callback(None, Exception("STATUS_API_SUBMISSION_QUEUE_FULL"))

    def dequeue_sqe(self, n: int) -> list[Any]:
        sqes: list[Any] = []

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

    def enqueue_cqe(self, cqe: Any) -> None:
        cqe.callback(cqe.value)

    def dequeue_cqe(self, cqes: Queue[Any]) -> Any:
        return cqes.get()
