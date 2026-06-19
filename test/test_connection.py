import socket

import pytest

from common.comms.transport.connection import Connection


def test_send_without_timeout_still_delivers():
    a, b = socket.socketpair()
    try:
        sender, receiver = Connection(a), Connection(b)
        sender.send(b"hello")
        assert receiver.recv() == b"hello"
    finally:
        a.close()
        b.close()


def test_send_timeout_unblocks_a_stuck_send():
    # A client that crashed leaves a socket whose buffer fills and never drains; a
    # plain sendall would block the gateway's response consumer FOREVER. With a send
    # timeout the stuck send raises OSError instead, so the gateway can drop the dead
    # client's responses (the live bug: responses queue stuck ready=3/unacked=1).
    a, b = socket.socketpair()
    try:
        conn = Connection(a, send_timeout=1)  # b never reads -> a's send buffer fills
        with pytest.raises(OSError):
            for _ in range(100000):
                conn.send(b"x" * 65536)
    finally:
        a.close()
        b.close()
