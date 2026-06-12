import logging
from queue import Queue
from typing import Sequence
from uuid import UUID

from common.comms.messages import EOF
from common.comms.middleware import MOM

from .eof_handler import StatefulEOFHandler, StatelessEOFHandler, total_sent


class StatelessSingleNodeEOFHandler(StatelessEOFHandler):
    def __init__(self, external_txs: Sequence[MOM]):
        self.external_txs = external_txs

        self.processed_counts: dict[UUID, int] = {}
        self.sent_data: dict[UUID, dict[int, int]] = {}

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        # one EOF per downstream shard, each with its own count (a single downstream
        # is just shard 0; the default filter's routes are shards too).
        shards = self.sent_data.get(eof.client_id, {})
        for shard, tx in enumerate(self.external_txs):
            shard_eof = EOF(eof.client_id, expected_count=shards.get(shard, 0))
            logging.info(f"downstreaming per-shard eof: {shard_eof.__dict__}")
            tx.send(shard_eof.serialize())


class StatefulSingleNodeEOFHandler(StatefulEOFHandler):
    def __init__(self, external_txs: Sequence[MOM], internal_eofs_tx: Queue[EOF]):
        self.external_txs = external_txs
        self.internal_eofs_tx = internal_eofs_tx

        self.processed_counts: dict[UUID, int] = {}
        self.sent_data: dict[UUID, dict[int, int]] = {}

    def start(self):
        pass

    def stop(self):
        pass

    def handle(self, eof: EOF):
        self.internal_eofs_tx.put(eof)

    def confirm_sent_data(self, client_id: UUID):
        pass

    def downstream(self, eof: EOF):
        eof.expected_count = total_sent(self.sent_data, eof.client_id)
        logging.info(f"downstreaming eof: {eof.__dict__}")
        for tx in self.external_txs:
            tx.send(eof.serialize())
