import logging
from uuid import UUID

from eof_handler import EOFHandler

from common.comms.messages import EOF
from common.comms.middleware import MOMQueue


class SingleNodeEOFHandler(EOFHandler):
    def __init__(self, txs: list[MOMQueue]):
        self.txs = txs

    def start(self): pass

    def stop_consuming(self): pass

    def stop(self): pass

    def close(self): pass

    def handle(self, eof: EOF):
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.txs:
            tx.send(eof.serialize())

    def add_processed_count(self, client_id: UUID):
        pass
