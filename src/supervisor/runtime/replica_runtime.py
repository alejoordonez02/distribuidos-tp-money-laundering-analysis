import logging
import time
from socket import AF_INET, SOCK_STREAM, socket

from common.comms.transport import Connection

from .supervisor_runtime import SupervisorRuntime

CONNECTION_RETRIES = 10
RETRY_DELAY = 1


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
        assert self._conn is not None  # pleasing linter

        try:
            while self._keep_running:
                self._conn.send(b"ping")
                pong = self._conn.recv()
                if not pong:
                    raise LeaderDownError()
        except OSError as e:
            logging.debug("lost connection with leader (%s)", e)
            raise LeaderDownError(e)
        finally:
            self.close()

    def close(self):
        if not self._conn:
            return
        try:
            self._conn.close()
        except OSError:
            pass
        self._conn = None
