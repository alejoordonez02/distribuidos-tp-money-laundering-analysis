import logging
from queue import Queue
from uuid import UUID

from common.comms.messages import EOF
from common.comms.middleware import MOMQueue

from .eof_handler import StatefulEOFHandler, StatelessEOFHandler


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


# TODO: esto es tmp hasta definir bien la interfaz
class StatefulSingleNodeEOFHandler(StatefulEOFHandler):
    def __init__(self, txs: list[MOMQueue], internal_eofs_tx: Queue[EOF]):
        self.txs = txs
        self.internal_eofs_tx = internal_eofs_tx

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        self.internal_eofs_tx.put(eof)

    def downstream(self, eof: EOF):
        logging.info(f"downstreaming eof: {eof.__dict__}")

        # NOTE: si soy steteful espero al eof del cliente
        #       para hacer `get_result(client_id)` y
        #       mandar el msj, mando un sólo msj.
        eof.expected_count = 1

        for tx in self.txs:
            tx.send(eof.serialize())

    def add_processed_count(self, client_id: UUID):
        pass
