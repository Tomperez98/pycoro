from __future__ import annotations

from typing import TYPE_CHECKING, Final

from pycoro.kernel import bus

if TYPE_CHECKING:
    import queue
    import random
    import threading
    from collections.abc import Callable

    from pycoro.aio.subsystem import SubsystemDST
    from pycoro.kernel import t_aio
    from pycoro.typing import Kind


class AioDST:
    def __init__(self, r: random.Random, p: float) -> None:
        self._r: Final = r
        self._p: Final = p
        self._sqes: list[bus.SQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]] = []
        self._cqes: list[bus.CQE[t_aio.Completion[Kind]]] = []
        self._subsystems: dict[str, SubsystemDST] = {}

    def add_subsystem(self, subsystem: SubsystemDST) -> None:
        assert subsystem.kind() not in self._subsystems, (
            f"cannot register subsystem {subsystem.kind()} twice"
        )
        self._subsystems[subsystem.kind()] = subsystem

    def start(self) -> None:
        for s in self._subsystems.values():
            s.start(None)

    def stop(self) -> None:
        for s in self._subsystems.values():
            s.stop()

    def shutdown(self) -> None:
        return

    def errors(self) -> queue.Queue[Exception] | None:
        return

    def signal(self, cancel: threading.Event) -> threading.Event:
        raise NotImplementedError

    def flush(self, time: int) -> None:
        flush: dict[str, list[bus.SQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]]] = {}
        for sqe in self._sqes:
            flush.setdefault(sqe.submission.kind(), []).append(sqe)

        for kind, sqes in flush.items():
            subsystem = self._subsystems.get(kind)
            assert subsystem is not None, "invalid aio submission"

            to_process: list[bus.SQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]] = []
            pre_failure: dict[int, bool] = {}
            post_failure: dict[int, bool] = {}
            n = 0

            for i, sqe in enumerate(sqes):
                if self._r.random() < self._p:
                    # choose pre or post failure with 50/50
                    if self._r.randint(0, 1) == 0:
                        pre_failure[i] = True
                    else:
                        post_failure[n] = True
                if pre_failure.get(i):
                    # simulated failure before processing
                    cqe = bus.CQE(
                        id=None,
                        result=RuntimeError("simulated failure before processing"),
                        callback=sqe.callback,
                    )
                    self.enqueue_cqe(cqe)
                else:
                    to_process.append(sqe)
                    n += 1

            for i, cqe in enumerate(subsystem.process(to_process)):
                if post_failure.get(i):
                    # simulate failure after processing
                    cqe.completion = None
                    cqe.error = RuntimeError("simulated failure after processing")
                self.enqueue_cqe(cqe)

        self._sqes.clear()

    def dispatch(
        self,
        submission: t_aio.Submission[Kind],
        callback: Callable[[t_aio.Completion[Kind] | Exception], None],
    ) -> None:
        assert submission.tags, "submission tags must be set"
        assert submission.tags.get("id"), "id tag must be set"
        self.enqueue_sqe(
            bus.SQE(id=submission.tags["id"], submission=submission, callback=callback)
        )

    def enqueue_sqe(self, sqe: bus.SQE[t_aio.Submission[Kind], t_aio.Completion[Kind]]) -> None:
        i = self._r.randint(0, len(self._sqes))
        self._sqes.insert(i, sqe)

    def enqueue_cqe(self, cqe: bus.CQE[t_aio.Completion[Kind]]) -> None:
        self._cqes.append(cqe)

    def dequeue_cqe(self, n: int) -> list[bus.CQE[t_aio.Completion[Kind]]]:
        cqes = self._cqes[: min(n, len(self._cqes))]
        self._cqes = self._cqes[min(n, len(self._cqes)) :]
        return cqes
