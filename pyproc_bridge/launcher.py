"""Generic launcher for running one task in a spawned Python interpreter over
the pyproc_bridge Supervisor/Worker protocol.

Not specific to any particular legacy interpreter -- any other-arch or
otherwise-legacy Python process can be driven through this, as long as its
worker module follows the protocol in ``pyproc_bridge.protocol``: connect
back, wait for TASK, stream zero or more EVENTs, and send exactly one
terminal message (RESULT, ERROR, or ABORTED). Interpreter resolution and
environment prep stay the caller's job.
"""

from __future__ import annotations

import concurrent.futures as futures
import subprocess
import time
from typing import Any, Callable

from pyproc_bridge import protocol
from pyproc_bridge.roles import Supervisor
from pyproc_bridge.abort import AbortSignal, AbortError
from pyproc_bridge.concurrency import submit

# Seconds to wait for the spawned interpreter to connect back before giving up.
WORKER_STARTUP_TIMEOUT = 30.0
# Seconds to let the worker shut down cleanly after an abort before it is killed.
WORKER_SHUTDOWN_TIMEOUT = 10.0


def run_legacy_python_worker(
    interpreter: str,
    module: str,
    task: dict[str, Any],
    on_event: Callable[[Any], None],
    abort: AbortSignal,
    *,
    port: int,
    env: dict[str, str],
    port_env_var: str,
    cwd: str | None = None,
    worker_label: str | None = None,
    thread_name: str | None = None,
    parse_event: Callable[[Any], Any] = lambda data: data,
) -> futures.Future[str]:
    """Launch ``interpreter -m module`` as an IPC worker, hand it ``task`` over
    the socket, and run it on a side thread.

    Returns a ``Future[str]`` immediately; call ``.result(timeout=...)`` to
    block for the worker's raw result JSON, or for a synchronous call just do
    ``run_legacy_python_worker(...).result()``. For custom threading (e.g. to
    parse the result on the same side thread), call
    ``run_legacy_python_worker_sync`` directly instead.

    The future's exception is ``RuntimeError`` if the worker errors, exits
    before sending a result, or never connects; ``AbortError`` if the run
    stopped on an abort before a result arrived.
    """
    label = worker_label or f"{module} worker"
    return submit(
        lambda: run_legacy_python_worker_sync(
            interpreter,
            module,
            task,
            on_event,
            abort,
            port=port,
            env=env,
            port_env_var=port_env_var,
            cwd=cwd,
            worker_label=label,
            parse_event=parse_event,
        ),
        thread_name=thread_name or f"legacy-worker[{module}]",
    )


def run_legacy_python_worker_sync(
    interpreter: str,
    module: str,
    task: dict[str, Any],
    on_event: Callable[[Any], None],
    abort: AbortSignal,
    *,
    port: int,
    env: dict[str, str],
    port_env_var: str,
    cwd: str | None = None,
    worker_label: str | None = None,
    parse_event: Callable[[Any], Any] = lambda data: data,
) -> str:
    """Blocking counterpart of ``run_legacy_python_worker`` -- use this when
    the caller wants to run its own follow-up work (e.g. result parsing) on
    the same side thread instead of chaining a second ``Future``.

    ``parse_event`` turns a raw EVENT payload into whatever ``on_event``
    expects -- it defaults to the identity function (the raw dict as-is). A
    worker that streams a structured event type should pass its own parser,
    e.g. a pydantic model's ``model_validate``.
    """
    label = worker_label or f"{module} worker"
    sup = Supervisor(port=port)
    # exactly one of these keys is set by the worker's terminal message
    outcome: dict[str, Any] = {}

    @sup.on(protocol.EVENT)
    def _on_event(data):
        on_event(parse_event(data))

    @sup.on(protocol.RESULT)
    def _on_result(data):
        outcome["result"] = data

    @sup.on(protocol.ERROR)
    def _on_error(data):
        outcome["error"] = data.get("error") if isinstance(data, dict) else str(data)

    @sup.on(protocol.ABORTED)
    def _on_aborted(data):
        outcome["aborted"] = True

    env = dict(env)
    env[port_env_var] = str(port)

    process = subprocess.Popen([interpreter, "-m", module], env=env, cwd=cwd)

    # Aborting only asks the worker to stop cooperatively; _wait_for_outcome
    # escalates to terminate()/kill() if it does not wrap up in time.
    abort.add_listener(lambda _reason: sup.emit(protocol.ABORT, True))

    try:
        _wait_for_worker(sup, process, label)
        sup.emit(protocol.TASK, task)
        _wait_for_outcome(process, outcome, abort)
    finally:
        sup.close()
        _terminate(process, timeout=5.0)

    if "error" in outcome:
        raise RuntimeError(f"{label} failed: {outcome['error']}")
    if "result" in outcome:
        return outcome["result"]
    if outcome.get("aborted") or abort.aborted:
        raise AbortError(abort.reason)
    raise RuntimeError(
        f"{label} exited (code {process.returncode}) without sending a result"
    )


def _wait_for_worker(sup: Supervisor, process: subprocess.Popen, worker_label: str) -> None:
    """Block until the worker connects back, or raise if it dies or times out."""
    deadline = time.monotonic() + WORKER_STARTUP_TIMEOUT
    while True:
        try:
            sup.wait_for_worker(timeout=1.0)
            return
        except TimeoutError:
            if process.poll() is not None:
                raise RuntimeError(
                    f"{worker_label} exited (code {process.returncode}) before connecting back"
                )
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"{worker_label} did not connect within {WORKER_STARTUP_TIMEOUT:.0f}s"
                )


def _wait_for_outcome(
    process: subprocess.Popen, outcome: dict[str, Any], abort: AbortSignal
) -> None:
    """Wait for the worker's terminal message or its exit, escalating to a
    forced teardown if an abort is not honoured within the grace period."""
    grace_deadline: float | None = None
    while process.poll() is None and not outcome:
        if abort.aborted and grace_deadline is None:
            grace_deadline = time.monotonic() + WORKER_SHUTDOWN_TIMEOUT
        if grace_deadline is not None and time.monotonic() > grace_deadline:
            break
        time.sleep(0.05)

    if process.poll() is None and not outcome:
        _terminate(process, timeout=5.0)
    else:
        # let a just-arrived terminal message settle / the process exit
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            _terminate(process, timeout=5.0)


def _terminate(process: subprocess.Popen, timeout: float) -> None:
    """Best-effort teardown: terminate, then kill if it does not stop in time."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
