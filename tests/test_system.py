from __future__ import annotations

import queue
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

import pycoro
from pycoro.aio import new as new_aio
from pycoro.api import new as new_api
from pycoro.app.subsystems.aio import echo
from pycoro.kernel import system
from pycoro.kernel.bus import SQE
from pycoro.kernel.t_api.error import Error
from pycoro.kernel.t_api.request import Request
from pycoro.kernel.t_api.response import Response
from pycoro.kernel.t_api.status import StatusCode

if TYPE_CHECKING:
    from pycoro.kernel.t_aio import Kind


@dataclass(frozen=True)
class EchoRequest:
    data: str

    def kind(self) -> str:
        return "echo"

    def validate(self) -> None:
        return

    def is_request_payload(self) -> Literal[True]:
        return True


@dataclass(frozen=True)
class EchoResponse:
    data: str

    def kind(self) -> str:
        return "echo"

    def is_response_payload(self) -> Literal[True]:
        return True


def echo_coroutine(
    c: pycoro.Coroutine[Kind, Kind, Any], r: Request[EchoRequest]
) -> Response[EchoResponse]:
    req = r.payload

    completion = pycoro.emit_and_wait(c, echo.EchoSubmission(req.data))
    assert isinstance(completion, echo.EchoCompletion)

    return Response(status=StatusCode.STATUS_OK, payload=EchoResponse(completion.data))


def test_system_loop() -> None:
    aio = new_aio(100)
    api = new_api(100)
    aio.add_subsystem(echo.new(aio, echo.Config(size=100, batch_size=1, workers=1)))
    api.start()
    aio.start()

    s = system.new(
        api,
        aio,
        system.Config(coroutine_max_size=100, submission_batch_size=1, completion_batch_size=1),
    )
    s.add_on_request("echo", echo_coroutine)

    received = queue.Queue[int](10)

    for i in range(5):
        data = str(i)

        def cb1(data: str, res: Response[EchoResponse] | Exception) -> None:
            received.put(1)
            assert not isinstance(res, Exception)
            assert data == res.payload.data

        api.enqueue_sqe(
            SQE[Request[EchoRequest], Response[EchoResponse]](
                submission=Request(payload=EchoRequest(data=data)),
                callback=partial(cb1, data),
            )
        )

    _ = s.shutdown()

    for _ in range(5):

        def cb2(res: Response[EchoResponse] | Exception) -> None:
            received.put(1)
            assert isinstance(res, Error)
            assert res.code == StatusCode.STATUS_SYSTEM_SHUTTING_DOWN

        api.enqueue_sqe(
            SQE[Request[EchoRequest], Response[EchoResponse]](
                submission=Request(payload=EchoRequest(data="nope")),
                callback=cb2,
            )
        )

    s.loop()

    for _ in range(10):
        _ = received.get()

    assert received.qsize() == 0
