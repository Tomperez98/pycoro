from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from pycoro.kernel.t_api.status import StatusCode


class Error(Exception):
    def __init__(self, code: StatusCode, original_error: Exception | None = None) -> None:
        self.code: Final = code
        self.original_error: Final = original_error
        super().__init__(code)

    def unwrap(self) -> Exception | None:
        return self.original_error

    def is_(self, target: Exception) -> bool:
        return isinstance(target, Error)


def new_error(code: StatusCode, err: Exception | None = None) -> Error:
    return Error(code, err)
