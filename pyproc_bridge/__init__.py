"""pyproc_bridge -- spawn a legacy or other-arch Python interpreter as a
worker subprocess and drive it over a small length-prefixed JSON-over-TCP
protocol: hand it one task, stream events back while it runs, get exactly
one terminal result/error/abort.

Zero third-party dependencies -- everything here is standard library, so it
stays importable under very old interpreters (down to Python 3.7) as well as
modern ones.

Building blocks:
- ``protocol``: the wire message names (TASK/ABORT/EVENT/RESULT/ERROR/ABORTED).
- ``IPCNode`` / ``Supervisor`` / ``Worker`` (``node``/``roles``): the raw
  length-prefixed send/receive plumbing and the two connection roles.
- ``run_legacy_python_worker`` / ``run_legacy_python_worker_sync``
  (``launcher``): the generic spawn/wait/teardown helper built on top of
  ``Supervisor`` -- most callers want this rather than the raw roles.
- ``AbortSignal`` / ``AbortError`` (``abort``): cooperative cancellation,
  modelled on DOM AbortSignal.
- ``submit`` (``concurrency``): run a callable on a side thread, get a
  ``concurrent.futures.Future`` back immediately.

See ``examples/`` for runnable references: ``host.py`` (raw Supervisor/Worker
mechanics), ``worker_echo.py`` (a minimal worker), and ``launcher_generic.py``
(the same job through ``run_legacy_python_worker_sync``).
"""

from pyproc_bridge import protocol
from pyproc_bridge.abort import AbortError, AbortListener, AbortSignal
from pyproc_bridge.concurrency import submit
from pyproc_bridge.launcher import run_legacy_python_worker, run_legacy_python_worker_sync
from pyproc_bridge.node import IPCNode
from pyproc_bridge.roles import Supervisor, Worker

__all__ = [
    "AbortError",
    "AbortListener",
    "AbortSignal",
    "IPCNode",
    "Supervisor",
    "Worker",
    "protocol",
    "run_legacy_python_worker",
    "run_legacy_python_worker_sync",
    "submit",
]
