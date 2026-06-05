import logging
from threading import Lock
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
        responses_lock: Lock,
    ):
        """
        Create a new `JoinRouteHandler`.

        # Args
        * reponses_tx_factory: a factory for the write half queue.
        * mom_factory: a factory for the read half queue.
        * join_fn: the join function to be used.
        * responses_lock: shared across all route handlers so a UC's (possibly
          multi-chunk) response is emitted atomically — chunks of different UCs
          never interleave in the responses queue.
        """
        self.responses_tx_factory = responses_tx_factory
        self.responses_tx: MOMQueue
        self.mom_factory = mom_factory
        self.join_fn = join_fn
        self._responses_lock = responses_lock
        self._mom: MOMQueue | None = None
        # Per-client message accounting: the upstream EOF carries `expected_count`
        # (how many data messages were sent to this queue). We only finalize a
        # client once that many have actually arrived — otherwise a data message
        # that gets enqueued *after* the EOF (no publisher confirms upstream, so a
        # late publish from another filter peer can overtake the EOF under load)
        # would be dropped, silently losing rows. The stateful nodes already wait
        # this way; UC1 goes filter->join directly, so the join must too.
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
        # Hold the shared lock for the whole UC so its chunks are contiguous in
        # the responses queue (otherwise UC1/UC3 chunks could interleave and the
        # client would write a corrupted, mixed response file).
        with self._responses_lock:
            for response in self.join_fn.get_responses(client_id):
                self.responses_tx.send(response.serialize())

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        client_id = msg.client_id
        logging.debug("received message %s", msg.__dict__)  # lazy: str() only if DEBUG

        if msg.type() == MessageType.EOF:
            self._expected[client_id] = msg.expected_count  # type: ignore[reportAttributeAccessIssue]
        else:
            self.join_fn.join(msg)
            self._received[client_id] = self._received.get(client_id, 0) + 1

        # Finalize only once the EOF has arrived AND every data message it
        # promised has actually been processed (handles the EOF overtaking late
        # data messages under load).
        expected = self._expected.get(client_id)
        if expected is not None and self._received.get(client_id, 0) >= expected:
            self._finalize(client_id)
            self._expected.pop(client_id, None)
            self._received.pop(client_id, None)

        ack()
