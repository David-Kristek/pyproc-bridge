"""Raw pyproc_bridge usage, no launcher: start a Supervisor, spawn
examples/worker_echo.py as a subprocess, hand it a task, and print whatever
comes back -- the bare Supervisor/Worker mechanics that
pyproc_bridge.launcher.run_legacy_python_worker wraps for you (see
examples/launcher_generic.py for that shortcut).

pyproc_bridge has no dependency beyond the standard library, so both sides
work under any Python:

    python -m examples.host
"""

from __future__ import annotations

import os
import subprocess
import sys

from pyproc_bridge import protocol
from pyproc_bridge.roles import Supervisor

PORT = 5555


def main() -> None:
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

    env = os.environ.copy()
    env["IPC_PORT"] = str(PORT)
    # Stand-in for "a legacy interpreter" -- in a real case this would be the
    # other process's own python.exe.
    process = subprocess.Popen([sys.executable, "-m", "examples.worker_echo"], env=env)

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
