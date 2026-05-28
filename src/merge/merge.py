import logging
from dataclasses import dataclass
from threading import Lock, Thread
from typing import Callable
from uuid import UUID

from merge_fns import MergeFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


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
        tx_factory: Callable[[], MOMQueue],
    ):
        self._left_rx = left_rx
        self._right_rx = right_rx
        self._fn = fn
        self._tx_factory = tx_factory

        self._state: dict[UUID, _ClientState] = {}
        self._lock = Lock()

    def start(self):
        setup_graceful_shutdown(self.stop)
        t = Thread(
            target=self._handle_side,
            args=(self._left_rx, self._handle_left_message),
            daemon=True,
        )
        t.start()
        self._handle_side(self._right_rx, self._handle_right_message)
        t.join()
        self.stop()

    def stop(self):
        self._right_rx.stop_consuming()
        self._left_rx.stop_consuming()
        self._left_rx.close()
        self._right_rx.close()

    def _get_state(self, client_id: UUID) -> _ClientState:
        if client_id not in self._state:
            self._state[client_id] = _ClientState()

        return self._state[client_id]

    def _try_emit_result(self, tx: MOMQueue, client_id: UUID):
        s = self._state[client_id]
        if not (s.left_done and s.right_done):
            return

        tx.send(self._fn.get_result(client_id).serialize())
        tx.send(EOF(client_id).serialize())
        logging.info(f"merge complete for client {client_id}")

    def _handle_side(
        self,
        side_rx: MOMQueue,
        _handle_side_message: Callable[[bytes, Callable, Callable, MOMQueue]],
    ):
        tx = self._tx_factory()
        side_rx.start_consuming(
            lambda bytes2, ack, nack: _handle_side_message(bytes2, ack, nack, tx)
        )
        tx.close()

    def _handle_left_message(
        self, bytes2: bytes, ack: Callable, _: Callable, tx: MOMQueue
    ):
        msg = deserialize_message(bytes2)

        with self._lock:
            if msg.type() == MessageType.EOF:
                self._get_state(msg.client_id).left_done = True
                self._try_emit_result(tx, msg.client_id)

            else:
                self._fn.left(msg)

        ack()

    def _handle_right_message(
        self, bytes2: bytes, ack: Callable, _: Callable, tx: MOMQueue
    ):
        msg = deserialize_message(bytes2)

        with self._lock:
            if msg.type() == MessageType.EOF:
                self._get_state(msg.client_id).right_done = True
                self._try_emit_result(tx, msg.client_id)

            else:
                self._fn.right(msg)

        ack()
