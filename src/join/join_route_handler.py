import logging
from typing import Any, Callable, Optional
from uuid import UUID

from join_fns import JoinFn

from common.checkpoint import dispatch, make_checkpointer
from common.comms.messages import Message, deserialize_message
from common.comms.middleware import MOM, MOMQueue


class _JoinCounts:
    """Per-client received/expected progress, snapshotable for the checkpoint."""

    def __init__(self):
        self._received: dict[UUID, int] = {}
        self._expected: dict[UUID, int] = {}
        self._finalized: set[UUID] = set()

    def inc_received(self, client_id: UUID):
        self._received[client_id] = self._received.get(client_id, 0) + 1

    def set_expected(self, client_id: UUID, expected: int):
        self._expected[client_id] = expected

    def is_complete(self, client_id: UUID) -> bool:
        expected = self._expected.get(client_id)
        return expected is not None and self._received.get(client_id, 0) >= expected

    def is_finalized(self, client_id: UUID) -> bool:
        return client_id in self._finalized

    def mark_finalized(self, client_id: UUID):
        self._finalized.add(client_id)
        self._expected.pop(client_id, None)
        self._received.pop(client_id, None)

    def drop(self, client_id: UUID):
        self._received.pop(client_id, None)
        self._expected.pop(client_id, None)
        self._finalized.discard(client_id)

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "received": {str(c): v for c, v in self._received.items()},
            "expected": {str(c): v for c, v in self._expected.items()},
            "finalized": [str(c) for c in self._finalized],
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self._received = {UUID(c): v for c, v in snapshot.get("received", {}).items()}
        self._expected = {UUID(c): v for c, v in snapshot.get("expected", {}).items()}
        self._finalized = {UUID(c) for c in snapshot.get("finalized", [])}


class JoinRouteHandler:
    """A *thread-safe* wrapper for handling a join queue.

    Receives factories for its moms rather than the moms themselves to account for
    pika's non-thread-safeness.
    """

    def __init__(
        self,
        responses_tx_factory: Callable[[], MOM],
        mom_factory: Callable[[], MOMQueue],
        join_fn: JoinFn,
        uc_id: int,
        state_dir: Optional[str] = None,
        checkpoint_every: int = 5,
    ):
        """
        Create a new `JoinRouteHandler`.

        # Args
        * reponses_tx_factory: a factory for the responses exchange publisher.
        * mom_factory: a factory for the read half queue.
        * join_fn: the join function to be used.
        * uc_id: stamped on every Response so the client can demultiplex chunks
          of different UCs arriving on its per-client response queue.
        """
        self.responses_tx_factory = responses_tx_factory
        self.responses_tx: MOM
        self.mom_factory = mom_factory
        self.join_fn = join_fn
        self._uc_id = uc_id
        self._mom: Optional[MOMQueue] = None
        self._counts = _JoinCounts()
        self._state_dir = state_dir
        self._checkpoint_every = checkpoint_every
        self._checkpointer = None

    def start(self):
        self.responses_tx = self.responses_tx_factory()
        self._mom = self.mom_factory()
        # Built here (in this handler's own thread) so the checkpoint state and the
        # rx connection share a thread.
        self._checkpointer = make_checkpointer(
            self._state_dir,
            f"join_uc{self._uc_id}",
            [],
            self._checkpoint_every,
            self.join_fn,
            extra_state={"counts": self._counts},
        )
        if self._checkpointer and self._checkpointer.restore():
            logging.info("restored join uc%s from checkpoint", self._uc_id)

        self._mom.start_consuming(self._handle_message)

    def stop(self):
        if self._mom is not None:
            self._mom.stop_consuming()

    def close(self):
        if self._mom is not None:
            self._mom.close()
        self.responses_tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        dispatch(self._checkpointer, msg, ack, self._on_eof, self._on_data, self._on_abort)

    def _on_eof(self, msg: Message):
        self._counts.set_expected(msg.client_id, msg.expected_count)  # type: ignore[attr-defined]
        self._maybe_finalize(msg.client_id)

    def _on_abort(self, msg: Message):
        client_id = msg.client_id
        self.join_fn.discard(client_id)
        self._counts.drop(client_id)
        if self._checkpointer is not None:
            self._checkpointer.mark_aborted(client_id)
            self._checkpointer.flush(force=True)

    def _on_data(self, msg: Message):
        self.join_fn.join(msg)
        self._counts.inc_received(msg.client_id)
        self._maybe_finalize(msg.client_id)

    def _maybe_finalize(self, client_id: UUID):
        if self._counts.is_finalized(client_id):
            return
        if not self._counts.is_complete(client_id):
            return
        for response in self.join_fn.get_responses(client_id):
            response.uc_id = self._uc_id
            self.responses_tx.send(response.serialize(), routing_key=str(client_id))
        self._counts.mark_finalized(client_id)
        # persist the finalized marker before the EOF is acked so a redelivered EOF does not re-emit
        if self._checkpointer is not None:
            self._checkpointer.flush(force=True)
