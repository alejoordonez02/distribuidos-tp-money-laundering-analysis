"""Opt-in deterministic fault injection — test tool, off unless FAULT_INJECTION=1.

Env vars: FAULT_INJECTION=1 (switch), FAULT_CRASH_POINT=<name>,
FAULT_CRASH_NODE=<id> (optional target). Crash points get wired in from Phase 2.
"""

import logging
import os
import sys

_ENABLED = os.getenv("FAULT_INJECTION", "0") == "1"
_CRASH_POINT = os.getenv("FAULT_CRASH_POINT", "")
_CRASH_NODE = os.getenv("FAULT_CRASH_NODE", "")
_NODE_ID = os.getenv("NODE_ID") or os.getenv("STRATEGY") or os.getenv("HOSTNAME", "")


def maybe_crash(point: str) -> None:
    if not _ENABLED or point != _CRASH_POINT:
        return
    if _CRASH_NODE and _CRASH_NODE != _NODE_ID:
        return

    logging.critical("FAULT INJECTION: crashing at point=%r node=%r", point, _NODE_ID)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(1)
