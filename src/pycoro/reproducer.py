from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


@dataclass(frozen=True)
class Run:
    id: str
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


type Yieldable = Run


@dataclass(frozen=True)
class P: ...


type Sendable[T] = P[T] | T | Exception


class GenIterator:
    def __init__(self) -> None:
        self._cache: dict[str, Sendable] = {}

    def add_to_cache(self, id: str, s: Sendable) -> None:
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
