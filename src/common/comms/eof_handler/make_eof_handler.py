import os
from queue import Queue
from typing import Iterable

from common.comms.eof_handler.stateful_ring_eof_handler import StatefulRingEOFHandler
from common.comms.messages.eof import EOF
from common.comms.middleware import MOM, RingRabbitMQ

from .eof_handler import StatefulEOFHandler, StatelessEOFHandler
from .single_node_eof_handler import SingleNodeEOFHandler, StatefulSingleNodeEOFHandler
from .stateless_ring_eof_handler import StatelessRingEOFHandler


def make_stateless_eof_handler(
    mom_host: str, txs: Iterable[MOM]
) -> StatelessEOFHandler:
    """
    Create a stateless end of file message handler.

    # Args
    * `mom_host` - the host of the mom broker.
    * `txs` - the write halves for forwarding end of file messages
      once they have already been processed.

    # Returns
    A new `StatelessEOFHandler`
    """

    NPEERS = int(os.getenv("NPEERS", "1"))

    if NPEERS < 1:
        raise ValueError("NPEERS must be greater or equal than 1")
    if NPEERS == 1:
        return SingleNodeEOFHandler(txs)

    IDX = int(os.environ["IDX"])
    RING_NAME: str = os.environ["RING_NAME"]

    peer_ids = [idx for idx in range(NPEERS) if idx != IDX]
    mom_ring = RingRabbitMQ(mom_host, RING_NAME, IDX, peer_ids)

    return StatelessRingEOFHandler(mom_ring, txs)


def make_stateful_eof_handler(
    mom_host: str, txs: Iterable[MOM], internal_eofs_tx: Queue[EOF]
) -> StatefulEOFHandler:
    """
    Create a stateful end of file message handler.

    # Args
    * `mom_host` - the host of the mom broker.
    * `txs` - the write halves for forwarding end of file messages
      once they have already been processed.
    * `internal_eofs_tx` - the medium through which the handler
      will communicate the receipt of cluster end of file messages.

    # Returns
    A new `StatefulEOFHandler`
    """

    NPEERS = int(os.getenv("NPEERS", "1"))

    if NPEERS < 1:
        raise ValueError("NPEERS must be greater or equal than 1")
    if NPEERS == 1:
        return StatefulSingleNodeEOFHandler(txs, internal_eofs_tx)

    IDX = int(os.environ["IDX"])
    RING_NAME: str = os.environ["RING_NAME"]

    peer_ids = [idx for idx in range(NPEERS) if idx != IDX]
    mom_ring = RingRabbitMQ(mom_host, RING_NAME, IDX, peer_ids)

    return StatefulRingEOFHandler(IDX, mom_ring, txs, internal_eofs_tx)
