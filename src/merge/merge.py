import logging
from threading import Lock, Thread
from typing import Any, Callable, Optional, Sequence
from uuid import UUID

from merge_fns import MergeFn

from common.checkpoint import Checkpointer
from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOM
from common.fault_injection import maybe_crash
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
        left_rx: MOM,
        right_rx: MOM,
        fn: MergeFn,
        tx_factory: Callable[[], Sequence[MOM]],
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

    def _handle_side(self, side_rx: MOM, handler: Callable):
        txs = self._tx_factory()
        side_rx.start_consuming(lambda b, ack, nack: handler(b, ack, nack, txs))
        for tx in txs:
            tx.close()

    def _try_emit_result(self, txs: Sequence[MOM], client_id: UUID):
        s = self._counts.get(client_id)
        done = (
            s.left_processed == s.left_expected
            and s.right_processed == s.right_expected
        )
        if not done:
            return

        # Round-robin results across the downstream shards (deterministic, so a
        # re-emit after a crash lands each result on the same shard), then send each
        # shard its own per-shard EOF so an affinity downstream peer learns exactly
        # how many messages to expect (a single downstream is just shard 0).
        sent_per_shard: dict[int, int] = {}
        for i, result in enumerate(self._fn.get_result(client_id)):
            shard = i % len(txs)
            txs[shard].send(result.serialize())
            sent_per_shard[shard] = sent_per_shard.get(shard, 0) + 1

        for shard, tx in enumerate(txs):
            eof = EOF(client_id, expected_count=sent_per_shard.get(shard, 0))
            logging.info(f"downstreaming per-shard eof: {eof.__dict__}")
            tx.send(eof.serialize())

    def _handle_left_message(self, bytes2, ack, _, txs):
        self._handle(bytes2, ack, txs, self._fn.left, is_left=True)

    def _handle_right_message(self, bytes2, ack, _, txs):
        self._handle(bytes2, ack, txs, self._fn.right, is_left=False)

    def _handle(self, bytes2, ack, txs, apply_fn, is_left: bool):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            maybe_crash("before_eof_flush")
            if self._checkpointer:
                self._checkpointer.flush()
            maybe_crash("after_eof_flush_before_handle")
            with self._lock:
                s = self._counts.get(msg.client_id)
                if is_left:
                    s.left_expected = msg.expected_count  # type: ignore[attr-defined]
                else:
                    s.right_expected = msg.expected_count  # type: ignore[attr-defined]
                self._try_emit_result(txs, msg.client_id)
            maybe_crash("after_downstream_eof_before_ack")
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
