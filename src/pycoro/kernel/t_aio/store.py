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
    def name(self) -> str: ...
    def is_command(self) -> Literal[True]: ...


@dataclass(frozen=True)
class StoreCompletion(_Kind):
    valid: bool
    result: Result


class Result(Protocol):
    def name(self) -> str: ...
    def is_result(self) -> Literal[True]: ...
