"""Same job as examples/host.py, but through
pyproc_bridge.launcher.run_legacy_python_worker_sync -- the generic
spawn/wait/teardown helper. Any legacy interpreter plugs in the same way:
swap ``sys.executable`` below for the other process's real interpreter path,
``env`` for whatever environment it needs, and
``port_env_var``/``worker_label`` to taste.

examples/worker_echo.py sends plain dicts as EVENT payloads, so this relies
on the launcher's default ``parse_event`` (the identity function) -- a
worker streaming a structured event type (e.g. a pydantic model) would pass
its own parser instead.

    python -m examples.launcher_generic
"""

from __future__ import annotations

import os
import sys

from pyproc_bridge.launcher import add_bridge_to_pythonpath, run_legacy_python_worker
from pyproc_bridge.abort import AbortSignal

PORT = 5556


def on_event(data) -> None:
    print(f"progress: {data}")


def main() -> None:
    # Makes "import pyproc_bridge" work in the worker without it being
    # installed in that interpreter -- drop this if the legacy interpreter
    # already has its own install (or a vendored copy) of pyproc_bridge.
    env = add_bridge_to_pythonpath(os.environ.copy())
    abort_signal = AbortSignal()
    future = run_legacy_python_worker(
        sys.executable,  # stand-in for another legacy interpreter's path
        "examples.worker_echo",
        {"count": 10, "delay_s": 0.3},
        on_event,
        abort=abort_signal,
        port=PORT,
        env=env,
        port_env_var="IPC_PORT",
        worker_label="echo worker",
    )
    input("Press Enter to abort the worker (or wait for it to finish)...")
    abort_signal.abort()
    if future.done():
        print(f"result: {future.result()}")
    elif abort_signal.aborted: 
        print("worker aborted before finishing")
    else: 
        print("worker still running after abort signal, waiting for it to finish...")
        print(f"result: {future.result()}")



if __name__ == "__main__":
    main()
