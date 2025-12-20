from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

    from pycoro.commands import Sendable, Yieldable


def gen_from_top(
    gen: Generator[Yieldable, Sendable[Any], Any], cache: dict[str, Sendable[Any]]
) -> Yieldable:
    assert inspect.getgeneratorstate(gen) == inspect.GEN_CREATED, (
        "Generator has already started or is finished!"
    )
    current_run = next(gen)

    while current_run.id in cache:
        cached_value = cache[current_run.id]
        match cached_value:
            case Exception():
                current_run = gen.throw(cached_value)
            case _:
                current_run = gen.send(cached_value)

    return current_run
