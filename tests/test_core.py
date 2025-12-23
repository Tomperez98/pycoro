from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from pycoro.core import Pycoro
from pycoro.processor import AsyncProcessor, Processor, SyncProcessor

if TYPE_CHECKING:
    from concurrent.futures import Future


@pytest.mark.parametrize("processor", [AsyncProcessor(), SyncProcessor()])
def test_concurrent_processing(processor: Processor) -> None:
    pycoro = Pycoro(maxsize=5, processor=processor)
    pycoro.start()
    results: list[Future[Any]] = []

    def producer(core: Pycoro, results: list[Future[Any]]) -> None:
        for i in range(5):
            # Slow down producers to simulate real-world flow
            f = core.add(f"id_{i}", lambda x: x + 1, i)
            results.append(f)

    # Start producer thread
    p_thread = threading.Thread(target=producer, args=(pycoro, results), daemon=True)
    p_thread.start()

    # Process everything
    while any(not f.done() for f in results):
        pycoro.tick()
        time.sleep(0.1)

    p_thread.join()
    pycoro.stop()

    # Verify results
    processed_values = [f.result() for f in results]
    assert processed_values == [1, 2, 3, 4, 5]
