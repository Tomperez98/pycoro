from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from pycoro.aio import Kind


@dataclass(frozen=True)
class SQE[I: Kind, O: Kind]:
    value: I | Callable[[], Any]
    callback: Callable[[O | Any | Exception], None]


@dataclass(frozen=True)
class CQE[O: Kind]:
    value: O | Any | Exception
    callback: Callable[[O | Any | Exception], None]
