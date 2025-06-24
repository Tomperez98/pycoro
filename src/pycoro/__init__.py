from __future__ import annotations

from collections.abc import Callable, Generator
from typing import Any

from pycoro.scheduler import Promise

type Coroutine[T] = Generator[Callable[[], Any] | Coroutine[Any] | Promise, Any, T]
