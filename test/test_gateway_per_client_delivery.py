import os
import socket
import sys
import threading
import time

import pytest

# The gateway package uses flat imports (it runs with src/gateway on PYTHONPATH inside
# its container), so import the handler the same way instead of through the package.
sys.path.insert(0, os.path.join("src", "gateway"))

from common.comms.transport.connection import Connection
from client_stream_handler import ClientStreamHandler


class _FakeResponse:
    """Stand-in for a Response: the writer only needs `serialize()`."""

    def __init__(self, payload: bytes):
        self._payload = payload

    def serialize(self) -> bytes:
        return self._payload


def _make_handler(conn: Connection, unregistered: list) -> ClientStreamHandler:
    handler = ClientStreamHandler(
        conn,
        register=lambda h: None,
        unregister=lambda cid: unregistered.append(cid),
        trans_tx_factory=lambda: [],
        accs_tx_factory=lambda: [],
    )
    # Drive the writer directly: the handshake half of `_run` is not under test here.
    handler._writer = threading.Thread(target=handler._drain_outbox)
    handler._writer.start()
    return handler


def test_writer_delivers_responses_in_order():
    a, b = socket.socketpair()
    try:
        handler = _make_handler(Connection(a), [])
        receiver = Connection(b)
        for i in range(5):
            handler.send(_FakeResponse(f"r{i}".encode()))
        for i in range(5):
            assert receiver.recv() == f"r{i}".encode()
        handler.stop()
        handler.join()
    finally:
        a.close()
        b.close()


def test_dead_client_does_not_block_other_clients():
    # The whole point of #88: a dead client's writer must not stall delivery to a live
    # one, and the shared router (`send`) must never block on a stuck client.
    dead_a, dead_b = socket.socketpair()
    live_a, live_b = socket.socketpair()
    try:
        unregistered: list = []
        # dead_b never reads, so dead_a's send buffer fills and its writer stalls until
        # the 1s send timeout fires; live_b reads normally.
        dead = _make_handler(Connection(dead_a, send_timeout=1), unregistered)
        live = _make_handler(Connection(live_a), unregistered)
        receiver = Connection(live_b)

        start = time.monotonic()
        for _ in range(2000):
            dead.send(_FakeResponse(b"x" * 65536))
        for i in range(5):
            live.send(_FakeResponse(f"r{i}".encode()))
        enqueue_elapsed = time.monotonic() - start

        # Enqueuing never blocked on the dead client (bounded outbox + non-blocking put).
        assert enqueue_elapsed < 1.0
        # The live client gets every response, unaffected by the dead one.
        for i in range(5):
            assert receiver.recv() == f"r{i}".encode()

        # The dead client's writer self-cleans: it stops being routed to.
        deadline = time.monotonic() + 5
        while dead.id not in unregistered and time.monotonic() < deadline:
            time.sleep(0.05)
        assert dead.id in unregistered

        live.stop()
        live.join()
        dead.join()
    finally:
        for s in (dead_a, dead_b, live_a, live_b):
            s.close()


def test_handler_exits_cleanly_when_client_disconnects_before_handshake():
    # A client that drops before sending Hello makes recv() return b""; the handler
    # must exit quietly instead of crashing its thread with an IndexError (#92).
    a, b = socket.socketpair()
    b.close()
    registered: list = []
    errors: list = []
    old_hook = threading.excepthook
    threading.excepthook = lambda args: errors.append(args.exc_type)
    try:
        handler = ClientStreamHandler(
            Connection(a),
            register=lambda h: registered.append(h),
            unregister=lambda cid: None,
            trans_tx_factory=lambda: [],
            accs_tx_factory=lambda: [],
        )
        handler.start()
        handler.handle.join(timeout=2)
    finally:
        threading.excepthook = old_hook
        a.close()
    assert not handler.handle.is_alive()
    assert errors == []
    assert registered == []


def test_send_never_blocks_when_outbox_is_full():
    # With no writer draining and a full outbox, the router must still return at once,
    # dropping the overflow rather than wedging the shared consumer.
    a, b = socket.socketpair()
    try:
        handler = ClientStreamHandler(
            Connection(a),
            register=lambda h: None,
            unregister=lambda cid: None,
            trans_tx_factory=lambda: [],
            accs_tx_factory=lambda: [],
        )
        start = time.monotonic()
        for i in range(50000):
            handler.send(_FakeResponse(b"x"))
        assert time.monotonic() - start < 1.0
    finally:
        a.close()
        b.close()
