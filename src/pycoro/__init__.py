from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any, overload

from pycoro.io import IO
from pycoro.scheduler import Promise, Scheduler

__all__ = ["IO", "Scheduler"]

type Yieldable = Computation[Any] | Promise
type Computation[T] = Generator[Yieldable, Any, T] | Callable[[], T]


@overload
def typesafe[T](y: Computation[T]) -> Generator[Computation[T], Promise[T], Promise[T]]: ...
@overload
def typesafe[T](y: Promise[T]) -> Generator[Promise[T], T, T]: ...
def typesafe(y: Computation | Promise) -> Generator[Computation | Promise, Any, Any]:
    return (yield y)
