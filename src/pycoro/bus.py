from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pycoro.aio import Kind

# Callback that receives either a result or an Exception
type Callback[O] = Callable[[O | Exception], None]


@dataclass(frozen=True)
class SQE[I: Kind | Callable[[], Any], O]:
    v: I
    cb: Callback[O]


@dataclass(frozen=True)
class CQE[O]:
    v: O | Exception
    cb: Callback[O]
