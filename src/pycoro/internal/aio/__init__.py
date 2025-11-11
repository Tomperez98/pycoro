from __future__ import annotations

from queue import Empty, Queue
from threading import Event, Thread
from typing import TYPE_CHECKING, Final, Protocol

from pycoro.internal.kernel import t_aio
from pycoro.internal.kernel.bus import CQE, SQE
from pycoro.internal.kernel.t_api.error import APIError
from pycoro.internal.kernel.t_api.status import StatusCode
from pycoro.internal.typing import Kind

if TYPE_CHECKING:
    from collections.abc import Callable

    from pycoro.internal.aio.subsystem import Subsystem


class AIO(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def shutdown(self) -> None: ...
    @property
    def errors(self) -> Queue[APIError]: ...
    def signal(self, cancel: Event) -> Event: ...
    def flush(self, time: int) -> None: ...
    def dispatch(
        self,
        v: t_aio.Submission[Kind] | None,
        cb: Callable[[t_aio.Completion[Kind] | Exception], None],
    ) -> None: ...
    def enqueue_sqe(self, sqe: SQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]) -> None: ...
    def enqueue_cqe(self, cqe: CQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]) -> None: ...
    def dequeue_cqe(self, n: int) -> list[CQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]]: ...


def new(size: int) -> AIO:
    return _AIO(size)


class _AIO:
    def __init__(self, size: int) -> None:
        self.cq: Final = Queue[CQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]](size)
        self.buffer: CQE[t_aio.Submission[Kind], t_aio.Completion[Kind]] | None = None
        self.subsystems: dict[str, Subsystem] = {}
        self.errors: Final = Queue[APIError]()

    def add_subsystem(self, subsystem: Subsystem) -> None:
        self.subsystems[subsystem.kind()] = subsystem

    def start(self) -> None:
        for subsystem in self.subsystems.values():
            subsystem.start(self.errors)

    def stop(self) -> None:
        for subsystem in self.subsystems.values():
            subsystem.stop()

    def shutdown(self) -> None: ...

    def flush(self, time: int) -> None:
        for subsystem in self.subsystems.values():
            subsystem.flush(time)

    def dispatch(
        self,
        v: t_aio.Submission[Kind] | None,
        cb: Callable[[t_aio.Completion[Kind] | Exception], None],
    ) -> None:
        assert v is not None
        assert v.tags.get("id") is not None, "id tag must be set"
        self.enqueue_sqe(SQE(id=v.tags["id"], callback=cb, submission=v))

    def enqueue_sqe(self, sqe: SQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]) -> None:
        subsystem = self.subsystems.get(sqe.submission.value.kind())
        assert subsystem is not None, "invalid aio submission"

        if not subsystem.enqueue(sqe):
            sqe.callback(APIError(StatusCode.STATUS_AIO_SUBMISSION_QUEUE_FULL))

    def enqueue_cqe(self, cqe: CQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]) -> None:
        self.cq.put(cqe)

    def dequeue_cqe(self, n: int) -> list[CQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]]:
        cqes: list[CQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]] = []

        if self.buffer is not None:
            cqes.append(self.buffer)
            self.buffer = None

        for _ in range(n - len(cqes)):
            try:
                sqe = self.cq.get_nowait()  # non-blocking
            except Empty:
                break

            cqes.append(sqe)

        return cqes

    def signal(self, cancel: Event) -> Event:
        signal_event = Event()

        # If buffer is already filled, signal immediately.
        if self.buffer is not None:
            signal_event.set()
            return signal_event

        def wait_for_signal() -> None:
            while not cancel.is_set():
                try:
                    sqe = self.cq.get(timeout=0.1)
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
