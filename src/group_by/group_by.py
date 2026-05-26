import logging
from typing import Callable

from group_by_fns import GroupByFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MOMQueue


class GroupBy:
    def __init__(
        self,
        rx: MOMQueue,
        fn: GroupByFn,
        tx: MOMQueue,
    ):
        self.rx = rx
        self.fn = fn
        self.tx = tx

    def start(self):
        self.rx.start_consuming(self._handle_message)

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            self.tx.send(self.fn.get_result(msg.client_id).serialize())
            self.tx.send(EOF(msg.client_id).serialize())
            logging.info(f"forwarded eof for client {msg.client_id}")
        else:
            self.fn.group_by(msg)

        ack()
