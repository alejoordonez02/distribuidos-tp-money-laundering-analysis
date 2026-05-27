import logging
from typing import Callable

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
    ):
        self.responses_tx_factory = responses_tx_factory
        self.responses_tx: MOMQueue
        self.mom_factory = mom_factory
        self.join_fn = join_fn
        self._mom: MOMQueue | None = None

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

    def _handle_eof(self, eof: EOF):
        response = self.join_fn.get_response(eof.client_id)
        self.responses_tx.send(response.serialize())

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        logging.debug(f"received message {msg.__dict__}")

        if msg.type() == MessageType.EOF:
            logging.info(f"received eof {msg.__dict__}")
            self._handle_eof(msg)  # type: ignore[reportArgumentType]
        else:
            self.join_fn.join(msg)

        ack()
