import logging
from uuid import UUID

from common.comms.messages import EOF
from common.comms.middleware import MOMQueue

from .eof_handler import StatelessEOFHandler


class SingleNodeEOFHandler(StatelessEOFHandler):
    def __init__(self, txs: list[MOMQueue]):
        self.txs = txs

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.txs:
            tx.send(eof.serialize())

    def add_processed_count(self, client_id: UUID):
        pass
