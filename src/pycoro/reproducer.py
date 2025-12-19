from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

    from pycoro.commands import Sendable, Yieldable


class GenIterator:
    def __init__(self) -> None:
        self._cache: dict[str, Sendable[Any]] = {}

    def add_to_cache(self, id: str, s: Sendable[Any]) -> None:
        assert id not in self._cache
        self._cache[id] = s

    def next(self, gen: Generator[Yieldable, Sendable[Any], Any]) -> Yieldable:
        current_run = next(gen)

        while current_run.id in self._cache:
            cached_value = self._cache[current_run.id]
            match cached_value:
                case Exception():
                    current_run = gen.throw(cached_value)
                case _:
                    current_run = gen.send(cached_value)

        return current_run
