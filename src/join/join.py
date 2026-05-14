import logging
from typing import Callable

from common.comms.messages import (
    EOF,
    FIN,
    MessageType,
    Transaction,
    deserialize_message,
)
from common.comms.middleware import MessageMiddlewareQueue


class Join:
    def __init__(
        self,
        client_responses_rx: MessageMiddlewareQueue,
        client_responses_tx: MessageMiddlewareQueue,
    ):
        self.client_responses_rx = client_responses_rx
        self.client_responses_tx = client_responses_tx

        self.client_responses: list[Transaction] = []

    def start(self):
        self.client_responses_rx.start_consuming(self._handle_message)

    def _handle_eof(self, eof: EOF):
        logging.info(f"received eof: {eof.__dict__}")
        logging.info(
            f"sending client results: {[r.__dict__ for r in self.client_responses]}"
        )
        for t in self.client_responses:
            # TODO: this should be an actual response message type, containing all
            #       the data the client is expecting for each specific use case.
            self.client_responses_tx.send(t.serialize())

        self.client_responses_tx.send(FIN().serialize())

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        match msg.type().value:
            case MessageType.TRANSACTION.value:
                self.client_responses.append(msg)  # type: ignore
            case MessageType.EOF.value:
                self._handle_eof(msg)  # type: ignore
            case _:
                raise RuntimeError(f"unexpected message {msg.__dict__}")

        ack()
