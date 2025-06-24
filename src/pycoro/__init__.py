from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any, overload

from pycoro.io import IO
from pycoro.scheduler import P, Scheduler

__all__ = ["IO", "Scheduler"]

type Yieldable = Computation[Any] | P
type Computation[T] = Generator[Yieldable, Any, T] | Callable[[], T]


@overload
def typesafe[T](y: Computation[T]) -> Generator[Computation[T], P[T], P[T]]: ...
@overload
def typesafe[T](y: P[T]) -> Generator[P[T], T, T]: ...
def typesafe(y: Computation | P) -> Generator[Computation | P, Any, Any]:
    return (yield y)
