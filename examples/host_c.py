"""Same job as examples/host.py, but the worker is the compiled C program
in examples/worker_echo.c instead of a Python module -- proof that the
Supervisor/Worker wire protocol (pyproc_bridge/protocol.py, framing in
pyproc_bridge/node.py) isn't Python-specific.

Build the worker first:

    MinGW-w64:  gcc -O2 -o examples/worker_echo.exe examples/worker_echo.c -lws2_32
    MSVC:       cl examples/worker_echo.c ws2_32.lib /Fe:examples/worker_echo.exe
    POSIX:      cc -O2 -o examples/worker_echo examples/worker_echo.c

Then:

    python -m examples.host_c
"""

from __future__ import annotations

import os
import subprocess
import sys

from pyproc_bridge import protocol
from pyproc_bridge.roles import Supervisor

PORT = 5557

_EXE_CANDIDATES = ["worker_echo.exe", "worker_echo"]


def _find_worker_exe() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    for name in _EXE_CANDIDATES:
        path = os.path.join(here, name)
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(
        "examples/worker_echo.exe (or worker_echo) not found -- build "
        "examples/worker_echo.c first, see the build commands in its header "
        "comment or in this file's module docstring."
    )


def main() -> None:
    worker_exe = _find_worker_exe()

    sup = Supervisor(port=PORT)

    @sup.on(protocol.EVENT)
    def _on_event(data) -> None:
        print(f"[host] progress: {data}")

    @sup.on(protocol.RESULT)
    def _on_result(data) -> None:
        print(f"[host] result: {data}")

    @sup.on(protocol.ERROR)
    def _on_error(data) -> None:
        print(f"[host] error: {data}")

    @sup.on(protocol.ABORTED)
    def _on_aborted(data) -> None:
        print("[host] worker aborted")

    env = os.environ.copy()
    env["IPC_PORT"] = str(PORT)
    process = subprocess.Popen([worker_exe], env=env)

    try:
        sup.wait_for_worker(timeout=10.0)
        sup.emit(protocol.TASK, {"count": 5, "delay_s": 0.3})
        process.wait(timeout=15.0)
    finally:
        sup.close()
        if process.poll() is None:
            process.terminate()


if __name__ == "__main__":
    main()
