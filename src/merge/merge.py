import logging
from dataclasses import dataclass
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from merge_fns import MergeFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue, QueueRabbitMQ


@dataclass
class _ClientState:
    left_done: bool = False
    right_done: bool = False


class Merge:
    def __init__(
        self,
        left_rx: MOMQueue,
        right_rx: MOMQueue,
        fn: MergeFn,
        mom_host: str,
        tx_name: str,
    ):
        self._left_rx = left_rx
        self._right_rx = right_rx
        self._fn = fn
        self._mom_host = mom_host
        self._tx_name = tx_name
        self._state: dict[UUID, _ClientState] = {}
        self._lock = Lock()

    def start(self):
        # Each thread creates its own TX connection — pika is not thread-safe.
        tx_right = QueueRabbitMQ(self._mom_host, self._tx_name)

        left = Thread(
            target=self._left_worker,
            daemon=True,
        )
        left.start()
        self._right_rx.start_consuming(
            lambda b, ack, nack: self._handle_right(b, ack, nack, tx_right)
        )
        left.join()

    def _left_worker(self):
        tx_left = QueueRabbitMQ(self._mom_host, self._tx_name)
        self._left_rx.start_consuming(
            lambda b, ack, nack: self._handle_left(b, ack, nack, tx_left)
        )

    def _get_state(self, client_id: UUID) -> _ClientState:
        if client_id not in self._state:
            self._state[client_id] = _ClientState()
        return self._state[client_id]

    def _try_emit_result(self, client_id: UUID, tx: MOMQueue):
        s = self._state[client_id]
        if not (s.left_done and s.right_done):
            return
        tx.send(self._fn.get_result(client_id).serialize())
        tx.send(EOF(client_id).serialize())
        logging.info(f"merge complete for client {client_id}")

    def _handle_left(self, bytes2: bytes, ack: Callable, nack: Callable, tx: MOMQueue):
        msg = deserialize_message(bytes2)
        with self._lock:
            if msg.type() == MessageType.EOF:
                self._get_state(msg.client_id).left_done = True
                self._try_emit_result(msg.client_id, tx)
            else:
                self._fn.left(msg)
        ack()

    def _handle_right(self, bytes2: bytes, ack: Callable, nack: Callable, tx: MOMQueue):
        msg = deserialize_message(bytes2)
        with self._lock:
            if msg.type() == MessageType.EOF:
                self._get_state(msg.client_id).right_done = True
                self._try_emit_result(msg.client_id, tx)
            else:
                self._fn.right(msg)
        ack()
