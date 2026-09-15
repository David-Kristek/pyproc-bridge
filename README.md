# pyproc-bridge

Spawn a legacy or other-arch Python interpreter as a worker subprocess and
drive it over a small length-prefixed JSON-over-TCP protocol: hand it one
task, stream events back while it runs, get exactly one terminal
result/error/abort.

Written for the case where you're stuck calling into an old or
architecture-locked Python (a vendor SDK that only installs into 32-bit
Python 3.7, say) from a modern process, and want a clean request/response +
progress-streaming shape around that boundary instead of hand-rolled
subprocess plumbing each time.

Zero third-party dependencies -- everything is standard library
(`socket`/`struct`/`threading`/`queue`/`json`/`subprocess`/
`concurrent.futures`), so it stays importable on very old interpreters
(`requires-python >=3.7`) as well as modern ones. Both sides of the bridge
(host and worker) import the same package.

## Concepts

- **Supervisor** (`pyproc_bridge.roles.Supervisor`) -- the host process.
  Listens on a TCP port, accepts one worker connection, sends it a `TASK`,
  and receives `EVENT`s plus exactly one terminal message (`RESULT`, `ERROR`,
  or `ABORTED`).
- **Worker** (`pyproc_bridge.roles.Worker`) -- the spawned interpreter.
  Connects back to the Supervisor, waits for `TASK`, does the work (ideally
  on a side thread so its socket stays responsive to `ABORT`), and sends its
  one terminal message.
- **Protocol** (`pyproc_bridge.protocol`) -- the six wire message names
  (`TASK`/`ABORT`/`EVENT`/`RESULT`/`ERROR`/`ABORTED`). See the module
  docstring for the full contract.
- **Launcher** (`pyproc_bridge.launcher.run_legacy_python_worker[_sync]`) --
  the generic "spawn `interpreter -m module`, hand it a task, stream events,
  return the result" helper built on top of `Supervisor`. Most callers want
  this rather than driving `Supervisor`/`Worker` directly.
- **AbortSignal** (`pyproc_bridge.abort`) -- cooperative cancellation token,
  modelled on DOM `AbortSignal`, used to ask a running worker to stop.
- **submit** (`pyproc_bridge.concurrency`) -- run a callable on a side
  thread, get a `concurrent.futures.Future` back immediately; the convention
  the launcher itself follows so a host's main thread stays free (e.g. for a
  GUI event loop).

## Example

See `examples/`:

- `host.py` -- raw `Supervisor`/`Worker` mechanics, no launcher.
- `worker_echo.py` -- a minimal worker: counts up, streaming a progress
  `EVENT` per step, then one `RESULT`.
- `launcher_generic.py` -- the same job as `host.py`, through
  `run_legacy_python_worker_sync` instead of driving `Supervisor` by hand.

```bash
python -m examples.host
# or
python -m examples.launcher_generic
```

## Installing into an old/other-arch interpreter

Because this package has no dependencies beyond the standard library, a
plain (non-editable) `pip install pyproc-bridge` works even under a very old
`pip` that can't do a PEP 660 editable install -- there's no C extension or
build step to trip over. Point that interpreter's own `pip` at it directly:

```powershell
& C:\path\to\old\python.exe -m pip install pyproc-bridge
```
