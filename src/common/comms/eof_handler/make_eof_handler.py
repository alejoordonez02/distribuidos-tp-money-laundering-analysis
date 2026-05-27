import os

from common.comms.middleware import MOMQueue, RingRabbitMQ

from .eof_handler import StatelessEOFHandler
from .single_node_eof_handler import SingleNodeEOFHandler
from .stateless_ring_eof_handler import StatelessRingEOFHandler


def make_stateless_eof_handler(
    mom_host: str, txs: list[MOMQueue]
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

    if NPEERS == 1:
        return SingleNodeEOFHandler(txs)

    IDX = int(os.environ["IDX"])
    RING_NAME: str = os.environ["RING_NAME"]

    peer_ids = [idx for idx in range(NPEERS) if idx != IDX]
    mom_ring = RingRabbitMQ(mom_host, RING_NAME, IDX, peer_ids)

    return StatelessRingEOFHandler(mom_ring, txs)
