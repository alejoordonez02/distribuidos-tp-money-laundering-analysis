import logging
from queue import Queue
from typing import Sequence
from uuid import UUID

from common.comms.messages import EOF
from common.comms.middleware import MOM

from .eof_handler import StatefulEOFHandler, StatelessEOFHandler


class StatelessSingleNodeEOFHandler(StatelessEOFHandler):
    def __init__(self, external_txs: Sequence[MOM]):
        self.external_txs = external_txs

        self.processed_counts: dict[UUID, int] = {}
        self.sent_data: dict[UUID, int] = {}

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        eof.expected_count = self.sent_data[eof.client_id]
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.external_txs:
            tx.send(eof.serialize())


# TODO: esto es tmp hasta definir bien la interfaz
class StatefulSingleNodeEOFHandler(StatefulEOFHandler):
    def __init__(self, external_txs: Sequence[MOM], internal_eofs_tx: Queue[EOF]):
        self.external_txs = external_txs
        self.internal_eofs_tx = internal_eofs_tx

        self.processed_counts: dict[UUID, int] = {}
        self.sent_data: dict[UUID, int] = {}

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        self.internal_eofs_tx.put(eof)

    def downstream(self, eof: EOF):
        eof.expected_count = self.sent_data[eof.client_id]
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.external_txs:
            tx.send(eof.serialize())
