from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any

from pycoro.scheduler import Promise

type Yieldable = Computation[Any] | Promise
type Computation[T] = Generator[Yieldable, Any, T] | Callable[[], T]
