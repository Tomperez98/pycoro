from __future__ import annotations

import random
from typing import Any

import pytest

from pycoro.processor import CQE, AsyncProcessor, Processor, SyncProcessor


@pytest.mark.parametrize("processor", [AsyncProcessor(max_workers=1), SyncProcessor()])
def test_processor(processor: Processor) -> None:
    processor.start()
    num_tasks = 20
    expected_results: dict[str, int] = {}
    for i in range(num_tasks):
        taskid = f"task::{i}"
        val1 = random.randint(1, 100)
        val2 = random.randint(1, 100)
        processor.submit(
            taskid,
            lambda *nums: sum(nums),
            val1,
            val2,
        )
        assert taskid not in expected_results
        expected_results[taskid] = val1 + val2

    processor.flush()
    batch: list[CQE[Any]] = []
    while len(batch) < num_tasks:
        batch.extend(processor.results())

    assert len(batch) == num_tasks
    for res in batch:
        assert isinstance(res.result, int)
        expected_val = expected_results.pop(res.info.id)

        assert res.result == expected_val
        assert sum(res.info.args) == expected_val

    assert len(expected_results) == 0

    processor.stop()
