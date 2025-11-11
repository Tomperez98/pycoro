from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


class _Kind:
    def kind(self) -> str:
        return "store"


@dataclass(frozen=True)
class StoreSubmission(_Kind):
    transaction: Transaction


@dataclass(frozen=True)
class Transaction:
    commands: list[Command]


class Command(Protocol):
    def is_command(self) -> Literal[True]: ...


@dataclass(frozen=True)
class StoreCompletion(_Kind):
    valid: bool
    results: list[Result]


class Result(Protocol):
    def is_result(self) -> Literal[True]: ...
