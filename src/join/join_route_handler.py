import logging
from typing import Callable
from uuid import UUID

from join_fns import JoinFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue


class JoinRouteHandler:
    """
    A *thread-safe* wrapper for handling a join queue.

    This class receives factories for its moms rather than the moms themselves to
    account for 'pika's non-thread-safeness.
    """

    def __init__(
        self,
        responses_tx_factory: Callable[[], MOMQueue],
        mom_factory: Callable[[], MOMQueue],
        join_fn: JoinFn,
        uc_id: int,
    ):
        """
        Create a new `JoinRouteHandler`.

        # Args
        * reponses_tx_factory: a factory for the write half queue.
        * mom_factory: a factory for the read half queue.
        * join_fn: the join function to be used.
        * uc_id: stamped on every Response so the client can demultiplex chunks
          of different UCs from the shared responses queue.
        """
        self.responses_tx_factory = responses_tx_factory
        self.responses_tx: MOMQueue
        self.mom_factory = mom_factory
        self.join_fn = join_fn
        self._uc_id = uc_id
        self._mom: MOMQueue | None = None
        self._received: dict[UUID, int] = {}
        self._expected: dict[UUID, int] = {}

    def start(self):
        """
        Starts consuming from its read half queue and sending the corresponding through
        its write half results queue as they are complete.
        """
        self.responses_tx = self.responses_tx_factory()
        self._mom = self.mom_factory()
        self._mom.start_consuming(lambda b, ack, nack: self._handle_message(b, ack, nack))

    def stop(self):
        if self._mom is not None:
            self._mom.stop_consuming()

    def close(self):
        if self._mom is not None:
            self._mom.close()
        self.responses_tx.close()

    def _finalize(self, client_id: UUID):
        for response in self.join_fn.get_responses(client_id):
            response.uc_id = self._uc_id
            self.responses_tx.send(response.serialize())

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        client_id = msg.client_id
        logging.debug("received message %s", msg.__dict__)

        if msg.type() == MessageType.EOF:
            self._expected[client_id] = msg.expected_count  # type: ignore[reportAttributeAccessIssue]
        else:
            self.join_fn.join(msg)
            self._received[client_id] = self._received.get(client_id, 0) + 1

        expected = self._expected.get(client_id)
        if expected is not None and self._received.get(client_id, 0) >= expected:
            self._finalize(client_id)
            self._expected.pop(client_id, None)
            self._received.pop(client_id, None)

        ack()
