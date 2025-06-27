from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from collections.abc import Generator

    from pycoro.scheduler import Computation, Promise, Time


@overload
def typesafe[O](v: Promise[O]) -> Generator[Promise[O], O, O]: ...  # type: ignore[overload-overlap]
@overload
def typesafe[O](v: Time) -> Generator[Time, int, int]: ...  # type: ignore[overload-overlap]
@overload
def typesafe[I, O](v: Computation[I, O] | I) -> Generator[Computation[I, O] | I, Promise[O], Promise[O]]: ...
def typesafe[I, O](v: Any) -> Generator[Any, Any, Any]:
    return (yield v)
