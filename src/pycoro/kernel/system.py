from __future__ import annotations

import contextlib
import datetime
import threading
import time
from dataclasses import dataclass
from threading import Event
from typing import TYPE_CHECKING, Any, Final

import pycoro
from pycoro.kernel.bus import CQE
from pycoro.kernel.t_api.error import Error
from pycoro.kernel.t_api.status import StatusCode

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from pycoro.aio import AIO
    from pycoro.api import API
    from pycoro.kernel.t_aio import Kind
    from pycoro.kernel.t_api import Request, Response


@dataclass(frozen=True)
class Config:
    coroutine_max_size: int
    submission_batch_size: int
    completion_batch_size: int
    signal_timeout: datetime.timedelta = datetime.timedelta()


@dataclass
class BackgroundCoroutine:
    coroutine: Callable[[], pycoro.CoroutineFunc[Kind, Kind, Any]]
    name: str
    last: int = 0
    future: Future[Any] | None = None


def new(api: API, aio: AIO, config: Config) -> _System:
    return _System(api, aio, config)


class _System:
    def __init__(self, api: API, aio: AIO, config: Config) -> None:
        self.config: Final = config
        self.aio: Final = aio
        self.api: Final = api
        self.scheduler: Final = pycoro.Scheduler(aio, config.coroutine_max_size)
        self.on_request: dict[
            str,
            Callable[
                [Request[Any], Callable[[Response[Any] | Exception], None]],
                pycoro.CoroutineFunc[Kind, Kind, Any],
            ],
        ] = {}
        self.background: list[BackgroundCoroutine] = []
        self.shutdown_event: Final = Event()
        self.short_circuit_event: Final = Event()

    def loop(self) -> None:
        try:
            while True:
                self.tick(int(time.time() * 1000))

                if self.done():
                    self.aio.shutdown()
                    self.scheduler.shutdown()
                    return

                cancel = Event()

                # These methods accept the cancel event and return their own completion Events
                api_signal = self.api.signal(cancel)
                aio_signal = self.aio.signal(cancel)

                # wait for a signal, short circuit, or timeout; whichever occurs first.
                # Python threading lacks a native 'select', so we poll the events.
                timeout_seconds = self.config.signal_timeout.total_seconds()
                start_time = time.time()

                while True:
                    # Check if any event is set (Simulating Go 'case <-signal')
                    if (
                        api_signal.is_set()
                        or aio_signal.is_set()
                        or self.short_circuit_event.is_set()
                    ):
                        break

                    if (time.time() - start_time) > timeout_seconds:
                        break

                    # Short sleep to prevent CPU spinning
                    time.sleep(0.01)

                cancel.set()

                _ = api_signal.wait()
                _ = aio_signal.wait()

        finally:
            self.shutdown_event.set()

    def tick(self, time: int) -> None:
        assert self.config.submission_batch_size > 0, (
            "submission batch size must be greater that zero"
        )
        assert self.config.completion_batch_size > 0, (
            "completion batch size must be greater than zero"
        )

        for i, cqe in enumerate(self.aio.dequeue_cqe(self.config.completion_batch_size)):
            assert i < self.config.completion_batch_size, (
                "cqes length be no greater than the completion batch size"
            )
            cqe.invoke()

        for bg in self.background:
            timeout_ms = self.config.signal_timeout.total_seconds()
            if (
                not self.api.done()
                and (time - bg.last) >= timeout_ms
                and (bg.future is None or bg.future.done())
            ):
                bg.last = time
                future = pycoro.add(self.scheduler, bg.coroutine())
                if future is None:
                    continue
                bg.future = future
                self.await_in_background(bg.future)

        for i, sqe in enumerate(self.api.dequeue_sqe(self.config.submission_batch_size)):
            assert i < self.config.submission_batch_size, (
                "sqes length be no greater than the submission batch size"
            )

            coroutine = self.on_request.get(sqe.submission.kind())
            assert coroutine is not None, (
                f"no registered coroutine for request kind {sqe.submission.kind()}"
            )

            future = pycoro.add(self.scheduler, coroutine(sqe.submission, sqe.callback))
            if future is None:
                sqe.callback(Error(StatusCode.STATUS_SCHEDULER_QUEUE_FULL))
            else:
                self.await_in_background(future)

        self.scheduler.run_until_blocked(time)
        self.aio.flush(time)

    def await_in_background(self, future: Future[Any]) -> None:
        def _() -> None:
            with contextlib.suppress(Exception):
                future.result()

        thread = threading.Thread(target=_, daemon=True)
        thread.start()

    def shutdown(self) -> Event:
        self.api.shutdown()
        self.short_circuit_event.set()
        return self.shutdown_event

    def done(self) -> bool:
        return self.api.done() and self.scheduler.size() == 0

    def add_on_request(
        self,
        kind: str,
        constructor: Callable[[pycoro.Coroutine[Kind, Kind, Any], Request[Any]], Response[Any]],
    ) -> None:
        def _(
            req: Request[Any], callback: Callable[[Response[Any] | Exception], None]
        ) -> pycoro.CoroutineFunc[Kind, Kind, Any]:
            def _(c: pycoro.Coroutine[Kind, Kind, Any]) -> Any:
                c.set("config", self.config)

                res = constructor(c, req)
                self.api.enqueue_cqe(CQE(completion=res, callback=callback))

            return _

        self.on_request[kind] = _

    def add_background(
        self,
        name: str,
        constructor: Callable[[pycoro.Coroutine[Kind, Kind, Any]], Any],
    ) -> None:
        def _() -> pycoro.CoroutineFunc[Kind, Kind, Any]:
            def _(c: pycoro.Coroutine[Kind, Kind, Any]) -> Any:
                c.set("config", self.config)
                return constructor(c)

            return _

        self.background.append(BackgroundCoroutine(name=name, coroutine=_))
