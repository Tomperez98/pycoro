from __future__ import annotations

from dataclasses import dataclass

from pycoro.typing import Kind


@dataclass(frozen=True)
class Submission[T: Kind]:
    payload: T
    tags: dict[str, str]

    def kind(self) -> str:
        return self.payload.kind()


@dataclass(frozen=True)
class Completion[T: Kind]:
    payload: T
    tags: dict[str, str]

    def kind(self) -> str:
        return self.payload.kind()
