from __future__ import annotations

from dataclasses import dataclass

from pycoro.internal.typing import Kind


@dataclass(frozen=True)
class Submission[T: Kind]:
    tags: dict[str, str]
    kind: T


@dataclass(frozen=True)
class Completion[T: Kind]:
    tags: dict[str, str]
    kind: T
