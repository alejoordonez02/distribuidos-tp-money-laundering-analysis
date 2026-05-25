import logging
from typing import Callable

from group_by_fns import GroupByFn

from common.comms.messages import EOF, MessageType, deserialize_message
from common.comms.middleware import MessageMiddlewareQueue


class GroupBy:
    def __init__(
        self,
        rx: MessageMiddlewareQueue,
        fn: GroupByFn,
        tx: MessageMiddlewareQueue,
    ):
        self.rx = rx
        self.fn = fn
        self.tx = tx

    def start(self):
        self.rx.start_consuming(self._handle_message)

    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)

        if msg.type() == MessageType.EOF:
            result = self.fn.get_result(msg.client_id)
            if isinstance(result, list):
                for r in result:
                    self.tx.send(r.serialize())
            else:
                self.tx.send(result.serialize())
            self.tx.send(EOF(msg.client_id).serialize())
            logging.info(f"forwarded eof for client {msg.client_id}")
        else:
            self.fn.group_by(msg)

        ack()
