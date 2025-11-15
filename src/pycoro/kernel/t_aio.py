from __future__ import annotations

from typing import Protocol


class Kind(Protocol):
    def kind(self) -> str: ...
