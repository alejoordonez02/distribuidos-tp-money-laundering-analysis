import os
import queue
import socket
import sys
import threading
import time

# The gateway runs with src/gateway on PYTHONPATH (flat imports), so import the handler the same way, not through the package.
sys.path.insert(0, os.path.join("src", "gateway"))

from common.comms.transport.connection import Connection
from client_stream_handler import ClientStreamHandler


class _FakeResponseQueue:
    """In-memory stand-in for a client's own broker response queue (one ExchangeRabbitMQ
    bound by client_id). It models the contract the handler relies on: FIFO delivery, a
    blocking `start_consuming` that hands each message to the callback with manual
    ack/nack, and `close` once the consumer stops. No real broker needed."""

    def __init__(self):
        self._q: "queue.Queue[bytes]" = queue.Queue()
        self._stop = threading.Event()
        self.acked: list[bytes] = []
        self.nacked: list[bytes] = []
        self.closed = False
        self.created_for = None

    def deliver(self, body: bytes):
        self._q.put(body)

    def start_consuming(self, on_message):
        while not self._stop.is_set():
            try:
                body = self._q.get(timeout=0.02)
            except queue.Empty:
                continue
            on_message(
                body,
                lambda b=body: self.acked.append(b),
                lambda b=body: self.nacked.append(b),
            )

    def stop_consuming(self):
        self._stop.set()

    def close(self):
        self.closed = True

    def send(self, message: bytes, routing_key=None):
        pass


def _make_handler(conn: Connection, fake: _FakeResponseQueue, unregistered=None):
    """Build a handler with an injected per-client response queue and bring its response
    consumer up directly — the handshake half of `_run` is exercised separately."""

    def factory(client_id):
        fake.created_for = client_id
        return fake

    handler = ClientStreamHandler(
        conn,
        register=lambda h: None,
        unregister=lambda cid: (unregistered.append(cid) if unregistered is not None else None),
        responses_rx_factory=factory,
        trans_tx_factory=lambda: [],
        accs_tx_factory=lambda: [],
    )
    handler._start_response_consumer()
    return handler


def _wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def test_delivers_responses_in_order_and_acks_after_send():
    """One client's queue is FIFO with a single consumer, so order holds, and each
    response is acked only after it is on the socket."""
    a, b = socket.socketpair()
    try:
        fake = _FakeResponseQueue()
        handler = _make_handler(Connection(a), fake)
        receiver = Connection(b)
        for i in range(5):
            fake.deliver(f"r{i}".encode())
        for i in range(5):
            assert receiver.recv() == f"r{i}".encode()
        assert _wait_until(lambda: len(fake.acked) == 5)
        assert fake.acked == [f"r{i}".encode() for i in range(5)]

        handler.stop()
        assert fake.closed
    finally:
        a.close()
        b.close()


def test_dead_client_does_not_affect_other_clients():
    """#88: each client drains its own queue on its own thread, so a dead client only
    stalls and tears down its own path, never another client's. dead_b never reads, so
    dead_a's buffer fills and its response thread blocks until the send timeout fires."""
    dead_a, dead_b = socket.socketpair()
    live_a, live_b = socket.socketpair()
    try:
        unregistered: list = []
        dead_fake = _FakeResponseQueue()
        live_fake = _FakeResponseQueue()
        dead = _make_handler(Connection(dead_a, send_timeout=1), dead_fake, unregistered)
        live = _make_handler(Connection(live_a), live_fake, unregistered)
        receiver = Connection(live_b)

        for _ in range(64):
            dead_fake.deliver(b"x" * 65536)
        for i in range(5):
            live_fake.deliver(f"r{i}".encode())

        for i in range(5):
            assert receiver.recv() == f"r{i}".encode()

        assert _wait_until(lambda: dead.id in unregistered)
        assert live.id not in unregistered

        live.stop()
        dead.stop()
        assert dead_fake.closed
    finally:
        for s in (dead_a, dead_b, live_a, live_b):
            s.close()


def test_response_is_not_acked_when_it_never_reaches_the_socket():
    """Nothing may be acked (and thus lost) without having been delivered: a send to a
    gone client fails, so the message stays un-acked and the client is torn down."""
    a, b = socket.socketpair()
    b.close()
    try:
        unregistered: list = []
        fake = _FakeResponseQueue()
        handler = _make_handler(Connection(a), fake, unregistered)
        fake.deliver(b"lost-if-acked")

        assert _wait_until(lambda: handler.id in unregistered)
        assert fake.acked == []
        assert _wait_until(lambda: fake.closed)
        handler.stop()
        assert handler._response_thread is not None
        assert not handler._response_thread.is_alive()
    finally:
        a.close()


def test_shutdown_stops_consumer_and_thread():
    """Gateway shutdown cuts the consumer and joins its thread cleanly, leaving none."""
    a, b = socket.socketpair()
    try:
        fake = _FakeResponseQueue()
        handler = _make_handler(Connection(a), fake)
        assert handler._response_thread is not None and handler._response_thread.is_alive()
        handler.stop()
        assert not handler._response_thread.is_alive()
        assert fake.closed
    finally:
        a.close()
        b.close()


def test_no_broker_resources_when_client_disconnects_before_handshake():
    """A client that drops before Hello must exit quietly (no IndexError, #92) and must
    not open any per-client response queue: the factory only runs after a handshake."""
    a, b = socket.socketpair()
    b.close()
    registered: list = []
    factory_calls: list = []
    errors: list = []
    old_hook = threading.excepthook
    threading.excepthook = lambda args: errors.append(args.exc_type)
    try:
        handler = ClientStreamHandler(
            Connection(a),
            register=lambda h: registered.append(h),
            unregister=lambda cid: None,
            responses_rx_factory=lambda cid: factory_calls.append(cid),
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
    assert factory_calls == []
    assert handler._response_thread is None
