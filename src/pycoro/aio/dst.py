from __future__ import annotations

from typing import TYPE_CHECKING, Final

from pycoro import util
from pycoro.kernel.bus import CQE, SQE

if TYPE_CHECKING:
    from collections.abc import Callable
    from random import Random
    from threading import Event

    from pycoro.aio import AIO
    from pycoro.aio.subsystem import SubsystemDST
    from pycoro.kernel import t_aio


def new(r: Random, p: float) -> AIO:
    return _AIODst(r, p)


class _AIODst:
    def __init__(self, r: Random, p: float) -> None:
        self.r: Final = r
        self.p: Final = p
        self.sqes: list[SQE[t_aio.Submission, t_aio.Completion]] = []
        self.cqes: list[CQE[t_aio.Submission, t_aio.Completion]] = []
        self.subsystems: dict[str, SubsystemDST] = {}

    def add_subsystem(self, subsystem: SubsystemDST) -> None:
        self.subsystems[subsystem.kind()] = subsystem

    def start(self) -> None:
        for subsystem in self.subsystems.values():
            subsystem.start(None)

    def stop(self) -> None:
        for subsystem in self.subsystems.values():
            subsystem.stop()

    def shutdown(self) -> None: ...

    @property
    def errors(self) -> None:
        return None

    def signal(self, cancel: Event) -> Event:  # pyright: ignore[reportUnusedParameter]
        raise NotImplementedError

    def flush(self, time: int) -> None:  # pyright: ignore[reportUnusedParameter]
        flush: dict[str, list[SQE[t_aio.Submission, t_aio.Completion]]] = {}
        for sqe in self.sqes:
            flush.setdefault(sqe.submission.value.kind(), []).append(sqe)

        for sqes in util.ordered_range_kv(flush):
            subsystem = self.subsystems.get(sqes.key)
            assert subsystem is not None, "invalid aio submission"
            to_process: list[SQE[t_aio.Submission, t_aio.Completion]] = []
            pre_failure: dict[int, bool] = {}
            post_failure: dict[int, bool] = {}
            n: int = 0

            for i, sqe in enumerate(sqes.value):
                # simulate p% chance of pre/post failure
                if self.r.random() < self.p:
                    match self.r.randint(0, 1):
                        case 0:
                            pre_failure[i] = True
                        case 1:
                            post_failure[n] = True
                        case _:
                            msg = "invalid path"
                            raise AssertionError(msg)

                if pre_failure[i]:
                    self.enqueue_cqe(
                        CQE(None, sqe.callback, Exception("simulated failure before processing"))
                    )
                else:
                    to_process.append(sqe)
                    n += 1

            for i, cqe in enumerate(subsystem.process(to_process)):
                if post_failure[i]:
                    cqe.completion = Exception("simulated failure after processing")
                self.enqueue_cqe(cqe)

        self.sqes.clear()

    def dispatch(
        self,
        v: t_aio.Submission | None,
        cb: Callable[[t_aio.Completion | Exception], None],
    ) -> None:
        assert v is not None
        assert v.tags.get("id") is not None, "id tag must be set"
        self.enqueue_sqe(SQE(v.tags["id"], cb, v))

    def enqueue_sqe(self, sqe: SQE[t_aio.Submission, t_aio.Completion]) -> None:
        self.sqes.insert(self.r.randint(0, len(self.sqes)), sqe)

    def enqueue_cqe(self, cqe: CQE[t_aio.Submission, t_aio.Completion]) -> None:
        self.cqes.append(cqe)

    def dequeue_cqe(self, n: int) -> list[CQE[t_aio.Submission, t_aio.Completion]]:
        cqes = self.cqes[: min(n, len(self.cqes))]
        self.cqes = self.cqes[min(n, len(self.cqes)) :]
        return cqes
