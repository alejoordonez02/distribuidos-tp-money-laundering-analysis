import logging
from typing import Callable, Optional, Sequence

from group_by_fns import GroupByFn

from common.checkpoint import Checkpointer, dispatch
from common.comms.eof_handler import StatelessEOFHandler
from common.comms.messages import Message, deserialize_message
from common.comms.middleware import MOM
from common.graceful_shutdown import setup_graceful_shutdown


class GroupBy:
    def __init__(
        self,
        fn: GroupByFn,
        external_rx: MOM,
        external_txs: Sequence[MOM],
        eof_handler: StatelessEOFHandler,
        checkpointer: Optional[Checkpointer] = None,
        input_ctx=None,
    ):
        self.external_rx = external_rx
        self.external_txs = external_txs
        self.fn = fn
        self.eof_handler = eof_handler
        self.checkpointer = checkpointer
        self.input_ctx = input_ctx

    def start(self):
        setup_graceful_shutdown(self.stop)
        if self.checkpointer and self.checkpointer.restore():
            logging.info("restored state from checkpoint")
        self.eof_handler.start()
        self.external_rx.start_consuming(self._handle_message)
        self.stop()

    def stop(self):
        self.external_rx.stop_consuming()
        self.eof_handler.stop()
        self.external_rx.close()
        for tx in self.external_txs:
            tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        dispatch(
            self.checkpointer, msg, ack, self._on_eof, self._on_data, self.input_ctx
        )

    def _on_eof(self, msg: Message):
        self.eof_handler.handle(msg)  # type: ignore[reportArgumentType]

    def _on_data(self, msg: Message):
        for group, affinity in self.fn.group_by(msg):
            external_tx_idx = affinity % len(self.external_txs)
            self.external_txs[external_tx_idx].send(group.serialize())
            self.eof_handler.add_sent_data_count(msg.client_id)

        self.eof_handler.add_processed_count(msg.client_id)
