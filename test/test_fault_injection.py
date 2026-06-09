"""Phase 1: fault injection must be a no-op when disabled (the default)."""

import importlib

import common.fault_injection as fi


def test_disabled_by_default_is_noop(monkeypatch):
    monkeypatch.delenv("FAULT_INJECTION", raising=False)
    module = importlib.reload(fi)

    # Must not raise / exit when the master switch is off.
    assert module.maybe_crash("any_point") is None


def test_armed_for_other_point_is_noop(monkeypatch):
    monkeypatch.setenv("FAULT_INJECTION", "1")
    monkeypatch.setenv("FAULT_CRASH_POINT", "some_point")
    module = importlib.reload(fi)

    # A point that does not match the armed one is a no-op.
    assert module.maybe_crash("a_different_point") is None
