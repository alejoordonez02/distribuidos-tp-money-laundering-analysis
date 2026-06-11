"""Opt-in deterministic fault injection (test tool, off unless FAULT_INJECTION=1).

Env: FAULT_INJECTION=1, FAULT_CRASH_POINT=<name>, FAULT_CRASH_NODE=<id> (optional).
One-shot via a sentinel in STATE_DIR so the node recovers on restart instead of
crash-looping; clean STATE_DIR to re-arm.
"""

import logging
import os
import sys

_ENABLED = os.getenv("FAULT_INJECTION", "0") == "1"
_CRASH_POINT = os.getenv("FAULT_CRASH_POINT", "")
_CRASH_NODE = os.getenv("FAULT_CRASH_NODE", "")
_NODE_ID = os.getenv("NODE_ID") or os.getenv("STRATEGY") or os.getenv("HOSTNAME", "")
_STATE_DIR = os.getenv("STATE_DIR", "/tmp")
_SENTINEL = os.path.join(_STATE_DIR, ".fault_fired")


def maybe_crash(point: str) -> None:
    if not _ENABLED or point != _CRASH_POINT:
        return
    if _CRASH_NODE and _CRASH_NODE != _NODE_ID:
        return
    if os.path.exists(_SENTINEL):
        return  # already crashed once here; recover normally on this restart

    try:
        with open(_SENTINEL, "w") as f:
            f.write(point)
            f.flush()
            os.fsync(f.fileno())
    except OSError:
        pass

    logging.critical("FAULT INJECTION: crashing at point=%r node=%r", point, _NODE_ID)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(1)
