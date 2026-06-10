import logging
from threading import Lock, Thread
from typing import Any, Callable, Optional
from uuid import UUID

from merge_fns import MergeFn

from common.checkpoint import Checkpointer
from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue
from common.graceful_shutdown import setup_graceful_shutdown


class _ClientState:
    def __init__(self):
        self.left_processed = 0
        self.left_expected = -1
        self.right_processed = 0
        self.right_expected = -1


class MergeCounts:
    """Per-client left/right progress, snapshotable so it rides the checkpoint."""

    def __init__(self):
        self._state: dict[UUID, _ClientState] = {}

    def get(self, client_id: UUID) -> _ClientState:
        if client_id not in self._state:
            self._state[client_id] = _ClientState()
        return self._state[client_id]

    def snapshot_state(self) -> dict[str, Any]:
        return {
            str(c): [s.left_processed, s.left_expected, s.right_processed, s.right_expected]
            for c, s in self._state.items()
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self._state = {}
        for cid, (lp, le, rp, re) in snapshot.items():
            s = _ClientState()
            s.left_processed, s.left_expected, s.right_processed, s.right_expected = (
                lp, le, rp, re,
            )
            self._state[UUID(cid)] = s


class Merge:
    def __init__(
        self,
        left_rx: MOMQueue,
        right_rx: MOMQueue,
        fn: MergeFn,
        tx_factory: Callable[[], MOMQueue],
        counts: MergeCounts,
        checkpointer: Optional[Checkpointer] = None,
    ):
        self._left_rx = left_rx
        self._right_rx = right_rx
        self._fn = fn
        self._tx_factory = tx_factory
        self._counts = counts
        self._checkpointer = checkpointer
        # One lock coordinates both side threads. Reuse the checkpointer's lock so
        # the business state, counts and dedup stay consistent under a single lock.
        self._lock = checkpointer.lock if checkpointer else Lock()

    def start(self):
        setup_graceful_shutdown(self.stop)
        if self._checkpointer and self._checkpointer.restore():
            logging.info("restored state from checkpoint")
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

    def _handle_side(self, side_rx: MOMQueue, handler: Callable):
        tx = self._tx_factory()
        side_rx.start_consuming(lambda b, ack, nack: handler(b, ack, nack, tx))
        tx.close()

    def _try_emit_result(self, tx: MOMQueue, client_id: UUID):
        s = self._counts.get(client_id)
        done = (
            s.left_processed == s.left_expected
            and s.right_processed == s.right_expected
        )
        if not done:
            return

        sent = 0
        for result in self._fn.get_result(client_id):
            tx.send(result.serialize())
            sent += 1

        eof = EOF(client_id, expected_count=sent)
        logging.info(f"downstreaming eof: {eof.__dict__}")
        tx.send(eof.serialize())

    def _handle_left_message(self, bytes2, ack, _, tx):
        self._handle(bytes2, ack, tx, self._fn.left, is_left=True)

    def _handle_right_message(self, bytes2, ack, _, tx):
        self._handle(bytes2, ack, tx, self._fn.right, is_left=False)

    def _handle(self, bytes2, ack, tx, apply_fn, is_left: bool):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            if self._checkpointer:
                self._checkpointer.flush()
            with self._lock:
                s = self._counts.get(msg.client_id)
                if is_left:
                    s.left_expected = msg.expected_count  # type: ignore[attr-defined]
                else:
                    s.right_expected = msg.expected_count  # type: ignore[attr-defined]
                self._try_emit_result(tx, msg.client_id)
            ack()
            return

        def apply():
            apply_fn(msg)
            s = self._counts.get(msg.client_id)
            if is_left:
                s.left_processed += 1
            else:
                s.right_processed += 1

        if self._checkpointer:
            self._checkpointer.handle_data(msg, apply, ack)
        else:
            with self._lock:
                apply()
            ack()
