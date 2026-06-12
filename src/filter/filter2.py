import logging
from typing import Callable, Optional, Sequence

from filter_fns import FilterFn

from common.checkpoint import Checkpointer, dispatch
from common.comms.eof_handler import StatelessEOFHandler
from common.comms.messages import Message, deserialize_message
from common.comms.middleware import MOM
from common.graceful_shutdown import setup_graceful_shutdown


class Filter:
    def __init__(
        self,
        messages_rx: MOM,
        routes: Sequence[tuple[MOM, FilterFn]],
        eof_handler: StatelessEOFHandler,
        checkpointer: Optional[Checkpointer] = None,
        input_ctx=None,
        sharded_routes: Optional[Sequence[tuple[Sequence[MOM], FilterFn]]] = None,
    ):
        self.messages_rx = messages_rx
        self.routes = routes
        # each sharded route partitions its output across N downstream shards (vs the
        # broadcast `routes`); used to fan a large stream out to N parallel consumers.
        self.sharded_routes = sharded_routes or []
        self.eof_handler = eof_handler
        self.checkpointer = checkpointer
        self.input_ctx = input_ctx

    def start(self):
        setup_graceful_shutdown(self.stop)
        if self.checkpointer and self.checkpointer.restore():
            logging.info("restored state from checkpoint")
        self.eof_handler.start()
        self.messages_rx.start_consuming(self._handle_message)
        self.stop()

    def stop(self):
        self.messages_rx.stop_consuming()
        self.eof_handler.stop()
        self.messages_rx.close()
        for tx, _ in self.routes:
            tx.close()
        for shards, _ in self.sharded_routes:
            for tx in shards:
                tx.close()

    def _handle_message(self, bytes2: bytes, ack: Callable, _: Callable):
        msg = deserialize_message(bytes2)
        dispatch(
            self.checkpointer, msg, ack, self._on_eof, self._on_data, self.input_ctx
        )

    def _on_eof(self, msg: Message):
        self.eof_handler.handle(msg)  # type: ignore[reportArgumentType]

    def _on_data(self, msg: Message):
        # broadcast routes: every input reaches every route, each counted as its own
        # shard so the ring sends each route its own EOF.
        shard = 0
        for destination, filter_fn in self.routes:
            destination.send(filter_fn.filter(msg).serialize())
            self.eof_handler.add_sent_data_count(msg.client_id, shard)
            shard += 1

        # sharded routes: partition the output across N shards, deterministically by
        # message identity so a re-emit after a crash lands on the same shard.
        for shards, filter_fn in self.sharded_routes:
            i = (int.from_bytes(msg.producer_id[-4:], "big") + msg.seq) % len(shards)
            shards[i].send(filter_fn.filter(msg).serialize())
            self.eof_handler.add_sent_data_count(msg.client_id, shard + i)
            shard += len(shards)

        self.eof_handler.add_processed_count(msg.client_id)
