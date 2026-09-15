"""Shared "run this on a side thread, hand back a Future" helper."""

from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import Callable, TypeVar

T = TypeVar("T")


def submit(fn: Callable[[], T], *, thread_name: str) -> Future[T]:
    """Run ``fn`` on a side thread, returning a ``Future`` for it immediately.

    ``.result(timeout=...)`` blocks and either returns the value or re-raises
    whatever ``fn`` raised; ``.done()`` polls without blocking;
    ``.add_done_callback(fn)`` still works if you want callback style.
    Nothing is printed or reported on its own -- if you never call
    ``.result()``/``.exception()``, a failure is silently discarded, same as
    any unread ``Future``.
    """
    fut: Future[T] = Future()

    def _run_in_background() -> None:
        try:
            fut.set_result(fn())
        except BaseException as exc:  # noqa: BLE001 -- carried by the future, not swallowed
            fut.set_exception(exc)

    threading.Thread(target=_run_in_background, daemon=True, name=thread_name).start()
    return fut
