import logging
import time
from socket import AF_INET, SOCK_STREAM, socket

from common.comms.transport import Connection

from .supervisor_runtime import SupervisorRuntime

# TODO: esto mejor pasarlo por envs pero posta ALTA pj
CONNECTION_RETRIES = 5
RETRY_DELAY = 0.1


class LeaderDownError(Exception): ...


class ReplicaRuntime(SupervisorRuntime):
    def __init__(self, leader_addr: tuple[str, int], ping_delay: float):
        self._leader = Leader(leader_addr)
        self._keep_running = False
        self._ping_delay = ping_delay

    def start(self):
        self._keep_running = True
        self._run()

    def _run(self):
        while self._keep_running:
            time.sleep(self._ping_delay)
            self._leader.ping_pong()
            logging.debug("ping ponged leader")

    def stop(self):
        self._keep_running = False
        self._leader.close()


class Leader:
    def __init__(self, leader_addr: tuple[str, int]):
        self._addr = leader_addr
        self._conn: Connection | None = None
        self._keep_running = True

    def _connect(self):
        for _ in range(CONNECTION_RETRIES):
            time.sleep(RETRY_DELAY)
            skt = socket(AF_INET, SOCK_STREAM)
            skt.settimeout(0.5)
            try:
                if not self._keep_running:
                    return
                skt.connect(self._addr)
                skt.settimeout(None)
                self._conn = Connection(skt)
                return
            except OSError:
                continue
        raise LeaderDownError(f"could not connect to leader {self._addr}")

    def ping_pong(self):
        if not self._conn:
            self._connect()
        conn = self._conn
        if self._keep_running == False:
            return
        assert conn is not None  # pleasing linter

        try:
            conn.send(b"ping")
            pong = conn.recv()
            if not pong:
                raise LeaderDownError("received an empty pong from leader")
        except OSError as e:
            logging.debug("lost connection with leader (%s)", e)
            self.close()
            raise LeaderDownError(e)

    def close(self):
        self._keep_running = False
        if not self._conn:
            return
        try:
            self._conn.close()
        except OSError:
            pass
        self._conn = None
