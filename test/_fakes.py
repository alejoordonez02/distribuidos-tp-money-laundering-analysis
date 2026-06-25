"""Shared test doubles reused across the unit-test suite.

These were copy-pasted across several test modules; consolidated here so a single
definition is shared. Test-specific fakes (the `_node` builders, `_FakeMergeFn` /
`_FakeAggFn`, `FakeMsg`, `FakeSeqSource`, ...) stay local to their module because
they differ in semantics.
"""

from typing import Callable

from common.comms.middleware.mom import MOM


class _Rec:
    """Shared event log so tests can assert ordering of flush/send calls."""

    def __init__(self):
        self.events: list[str] = []


class _FakeCkpt:
    """Records flush() calls into the shared event log so the test can assert the
    checkpoint that freezes the spill happens BEFORE the emit sends."""

    def __init__(self, rec: _Rec):
        self._rec = rec

    def flush(self, force: bool = False):
        self._rec.events.append("flush")


class _RecTx(MOM):
    """MOM stub that records every send() into the shared event log."""

    def __init__(self, rec: _Rec):
        self._rec = rec
        self.sent: list[bytes] = []

    def send(self, message: bytes):
        self._rec.events.append("send")
        self.sent.append(message)

    def start_consuming(self, on_message_callback: Callable):  # pragma: no cover
        raise NotImplementedError

    def stop_consuming(self):  # pragma: no cover
        raise NotImplementedError

    def close(self):  # pragma: no cover
        raise NotImplementedError

    def clone(self) -> "_RecTx":  # pragma: no cover
        return self


class FakeFn:
    """Stateful node-fn double: snapshots/restores an `applied` list.

    Superset of the no-op stub the abort test needs, so both can share it.
    """

    def __init__(self):
        self.applied: list[int] = []

    def snapshot_state(self):
        return {"applied": list(self.applied)}

    def restore_state(self, snapshot):
        self.applied = list(snapshot["applied"])
