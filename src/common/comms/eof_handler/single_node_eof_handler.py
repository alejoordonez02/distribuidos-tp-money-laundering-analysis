import logging
from queue import Queue
from typing import Iterable
from uuid import UUID

from common.comms.messages import EOF
from common.comms.middleware import MOM

from .eof_handler import StatefulEOFHandler, StatelessEOFHandler


class StatelessSingleNodeEOFHandler(StatelessEOFHandler):
    def __init__(self, txs: Iterable[MOM]):
        self.txs = txs

        self.processed_counts: dict[UUID, int] = {}
        self.next_expected_counts: dict[UUID, int] = {}

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        eof.expected_count = self.next_expected_counts[eof.client_id]
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.txs:
            tx.send(eof.serialize())


# TODO: esto es tmp hasta definir bien la interfaz
class StatefulSingleNodeEOFHandler(StatefulEOFHandler):
    def __init__(self, txs: Iterable[MOM], internal_eofs_tx: Queue[EOF]):
        self.txs = txs
        self.internal_eofs_tx = internal_eofs_tx

        self.processed_counts: dict[UUID, int] = {}
        self.next_expected_counts: dict[UUID, int] = {}

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        self.internal_eofs_tx.put(eof)

    def downstream(self, eof: EOF):
        eof.expected_count = self.next_expected_counts[eof.client_id]
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.txs:
            tx.send(eof.serialize())
