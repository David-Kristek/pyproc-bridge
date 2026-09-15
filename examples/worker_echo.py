"""Minimal generic IPC worker: connects back to a Supervisor, runs one small
job, and reports progress -- the "legacy Python process" side of the bridge.
Used by both examples/host.py (talks to pyproc_bridge.roles directly) and
examples/launcher_generic.py (spawns this via pyproc_bridge.launcher).

The job stands in for whatever real work a legacy process would do: count
from 0 up to ``task["count"]``, sleeping ``task["delay_s"]`` between steps,
sending an EVENT per step and one RESULT at the end. Follows the protocol
contract in ``pyproc_bridge/protocol.py``: connect, wait for TASK, run the
job on a side thread so the socket stays responsive to ABORT, send exactly
one terminal message, close.
"""

from __future__ import annotations

import os
import threading
import time

from pyproc_bridge import protocol
from pyproc_bridge.roles import Worker

port = int(os.getenv("IPC_PORT", "5555"))

worker = Worker()
abort_requested = False


@worker.on(protocol.ABORT)
def _on_abort(_data) -> None:
    global abort_requested
    print("[worker] abort requested")
    abort_requested = True


@worker.on(protocol.TASK)
def _on_task(data) -> None:
    threading.Thread(target=_run_job, args=(data,), daemon=True).start()


def _run_job(data) -> None:
    count = int(data.get("count", 5))
    delay_s = float(data.get("delay_s", 0.5))
    try:
        for i in range(count):
            if abort_requested:
                worker.emit(protocol.ABORTED, True)
                return
            time.sleep(delay_s)
            worker.emit(protocol.EVENT, {"step": i, "of": count})
        worker.emit(protocol.RESULT, f"counted to {count}")
    except Exception as exc:  # noqa: BLE001 -- reported to the host, then re-raised context is lost by design
        worker.emit(protocol.ERROR, {"error": repr(exc)})
    finally:
        worker.flush()  # make sure the terminal message is on the wire
        worker.close()


if __name__ == "__main__":
    worker.connect(port=port)
    try:
        worker.wait()
    except KeyboardInterrupt:
        pass
    finally:
        worker.close()
