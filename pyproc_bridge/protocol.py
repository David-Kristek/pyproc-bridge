"""Wire message names for the Supervisor <-> Worker channel.

Every message has exactly one handler on the receiving side -- no handler
chains. The worker sends exactly one terminal message (RESULT, ERROR, or
ABORTED) and then closes.

    host -> worker
        TASK    {config dict}      hand over the one job this process runs
        ABORT   true               request a cooperative stop

    worker -> host
        EVENT   {...}              streamed telemetry, zero or more
        RESULT  "<json>"           terminal: the run's result, worker-defined shape
        ERROR   {"error": "..."}   terminal: the run raised
        ABORTED true               terminal: the run stopped on request
"""

TASK = "task"
ABORT = "abort"

EVENT = "event"
RESULT = "result"
ERROR = "error"
ABORTED = "aborted"
