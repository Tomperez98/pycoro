from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pycoro.commands import Run, Sendable, Yieldable
from pycoro.itertools import gen_from_top

if TYPE_CHECKING:
    from collections.abc import Generator


def test_gen_reproducer() -> None:
    # A dummy task function
    def task(name: str) -> str:
        return f"result_{name}"

    # 1. Define the generator factory
    def workflow() -> Generator[Yieldable, Sendable[Any], str]:
        val1 = yield Run(fn=task, args=("a",), kwargs={}).options(id="step1")
        val2 = yield Run(fn=task, args=("b",), kwargs={}).options(id="step2")
        return f"{val1}_{val2}"

    # --- SCENARIO 1: Empty Cache ---
    # Should return the first run
    run1 = gen_from_top(workflow(), {})
    assert run1.id == "step1"

    # --- SCENARIO 2: Partial Cache (Successful Send) ---
    # Calling next(workflow()) again iterates from top,
    # sends "data1" into step1, and should return step2.
    run2 = gen_from_top(workflow(), {"step1": "data1"})
    assert run2.id == "step2"

    # --- SCENARIO 3: Exception Handling ---
    # Clear and test exception injection
    def error_workflow() -> Generator[Yieldable, Sendable[Any], str]:
        try:
            yield Run(fn=task, args=(), kwargs={}).options(id="step1")
        except ValueError as e:
            # If the iterator correctly uses gen.throw(), we land here
            yield Run(fn=task, args=(str(e),), kwargs={}).options(id="recovery")

        return "done"

    run_recovery = gen_from_top(error_workflow(), {"step1": ValueError("fail")})
    assert run_recovery.id == "recovery"
    assert run_recovery.args == ("fail",)

    # --- SCENARIO 4: Completion ---
    # All steps cached; the generator should hit StopIteration
    with pytest.raises(StopIteration) as exc_info:
        gen_from_top(workflow(), {"step1": "done1", "step2": "done2"})

    assert exc_info.value.value == "done1_done2"
