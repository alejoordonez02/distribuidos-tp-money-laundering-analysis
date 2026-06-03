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
    left_processed = 0
    left_expected = -1
    right_processed = 0
    right_expected = -1


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
        self._mtx = Lock()

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
        done = (
            s.left_processed == s.left_expected
            and s.right_processed == s.right_expected
        )
        logging.debug(
            f"left: {s.left_processed}/{s.left_expected},\nright: {s.right_processed}/{s.right_expected}\ndone:{done}"
        )

        if not done:
            return

        # get_result streams the merged output in bounded chunks (the right side
        # is spilled to disk) so we never build one huge message. We forward each
        # chunk and tell the next cluster exactly how many messages to expect.
        # NOTE: merge is single-instance, so this node alone owns its EOF: it
        #       emits once both sides reached their expected counts, and the
        #       downstream waits for `sent` messages.
        sent = 0
        for result in self._fn.get_result(client_id):
            tx.send(result.serialize())
            sent += 1

        eof = EOF(client_id, expected_count=sent)

        logging.info(f"downstreaming eof: {eof.__dict__}")
        tx.send(eof.serialize())

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

        # TODO: seguro se puede manejar mejor este mtx.
        with self._mtx:
            if msg.type() == MessageType.EOF:
                eof: EOF = msg  # type: ignore[reportAssignmentType]
                logging.debug("received left eof: {eof.__dict__}")
                self._get_state(eof.client_id).left_expected = eof.expected_count
            else:
                self._fn.left(msg)
                self._get_state(msg.client_id).left_processed += 1

            self._try_emit_result(tx, msg.client_id)

        ack()

    def _handle_right_message(
        self, bytes2: bytes, ack: Callable, _: Callable, tx: MOMQueue
    ):
        msg = deserialize_message(bytes2)

        with self._mtx:
            if msg.type() == MessageType.EOF:
                eof: EOF = msg  # type: ignore[reportAssignmentType]
                logging.debug("received right eof: {eof.__dict__}")
                self._get_state(eof.client_id).right_expected = eof.expected_count
            else:
                self._fn.right(msg)
                self._get_state(msg.client_id).right_processed += 1

            self._try_emit_result(tx, msg.client_id)

        ack()
