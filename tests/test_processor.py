from __future__ import annotations

import random

from pycoro.processor import Processor


def test_processor() -> None:
    p = Processor(max_workers=1)
    p.start()
    num_tasks = 20
    expected_results: dict[str, int] = {}
    for i in range(num_tasks):
        taskid = f"task::{i}"
        val1 = random.randint(1, 100)
        val2 = random.randint(1, 100)
        p.submit(
            taskid,
            lambda *nums: sum(nums),
            val1,
            val2,
        )
        assert taskid not in expected_results
        expected_results[taskid] = val1 + val2

    submitted_count = p.flush()

    processed_count = 0

    while processed_count < submitted_count:
        batch = p.wait_for_batch(count=5)

        assert batch, "Timed out waiting for batch results"

        for res in batch:
            assert isinstance(res.result, int)
            expected_val = expected_results.pop(res.info.id)

            assert res.result == expected_val
            assert sum(res.info.args) == expected_val

            processed_count += 1

    assert processed_count == submitted_count == num_tasks
    assert len(expected_results) == 0

    p.stop()
