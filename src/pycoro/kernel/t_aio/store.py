from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass(frozen=True)
class StoreSubmission:
    transaction: Transaction

    def kind(self) -> str:
        return "store"


@dataclass(frozen=True)
class Transaction:
    commands: list[Command]


class Command(Protocol):
    def name(self) -> str: ...
    def is_command(self) -> Literal[True]: ...


@dataclass(frozen=True)
class StoreCompletion:
    results: Result

    def kind(self) -> str:
        return "store"


class Result(Protocol):
    def name(self) -> str: ...
    def is_result(self) -> Literal[True]: ...
