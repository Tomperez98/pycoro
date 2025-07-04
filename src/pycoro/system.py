from __future__ import annotations

import time
from threading import Event, Thread
from typing import TYPE_CHECKING

from pycoro.scheduler import Computation, Scheduler

if TYPE_CHECKING:
    from concurrent.futures import Future

    from pycoro.io import IO


class Pycoro[I, O]:
    def __init__(self, io: IO[I, O], size: int, dequeue_size: int, tick_freq: float = 0.1) -> None:
        self._io = io
        self._scheduler = Scheduler(self._io, size)
        self._dequeue_size = dequeue_size
        self._tick_freq = tick_freq
        self._thread = Thread(target=self._loop, daemon=True, name="pycoro-main-thread")
        self._stop = Event()
        self._stopped = Event()
        self._stopped.set()

    def add(self, c: Computation[I, O] | I) -> Future[O]:
        self._start()
        return self._scheduler.add(c)

    def shutdown(self) -> None:
        self._stop.set()
        self._stopped.wait()
        self._thread.join()
        self._scheduler.shutdown()
        self._stopped.set()
        self._stop.clear()

    def _start(self) -> None:
        if self._stopped.is_set():
            self._stopped.clear()
            self._io.start()
            self._thread.start()

    def _loop(self) -> None:
        while True:
            self._tick(int(time.time() * 1_000))

            if self._stop.wait(self._tick_freq) and self._scheduler.size() == 0:
                self._stopped.set()
                return

    def _tick(self, time: int) -> None:
        for cqe in self._io.dequeue(self._dequeue_size):
            cqe.callback(cqe.value)

        self._scheduler.run_until_blocked(time)
        self._io.flush(time)
