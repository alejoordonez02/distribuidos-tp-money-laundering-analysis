import threading
from socket import AF_INET, SOCK_STREAM, socket

from common.comms.transport import Connection
from common.comms.supervisor import Heartbeat, Register, decode
from common.heartbeat import HeartbeatClient


def _listening_server():
    srv = socket(AF_INET, SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    return srv, srv.getsockname()[1]


def test_client_registers_then_heartbeats():
    srv, port = _listening_server()
    received = []

    def serve():
        conn_skt, _ = srv.accept()
        conn = Connection(conn_skt)
        while len(received) < 3:
            data = conn.recv()
            if not data:
                break
            received.append(decode(data))
        conn.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()

    client = HeartbeatClient("node_1", "filter", "127.0.0.1", port, interval=0.05)
    client.start()
    t.join(timeout=5)
    client.stop()
    srv.close()

    assert isinstance(received[0], Register)
    assert received[0].node_id == "node_1"
    assert received[0].kind == "filter"
    assert any(isinstance(m, Heartbeat) for m in received[1:])


def test_stop_is_clean_when_supervisor_never_came_up():
    # Nothing listening on this port: the client keeps retrying without raising, and stop() must return promptly.
    client = HeartbeatClient("node_1", "filter", "127.0.0.1", 1, interval=0.05)
    client.start()
    client.stop()
    assert not client._thread.is_alive()
