from typing import Callable

from .errors import MOMCloseError
from .exchange_mom import MOMExchange
from .exchange_rabbitmq import ExchangeRabbitMQ
from .ring_mom import MOMRing, MOMRingError


def _get_front_back_ids(self_id: int, peer_ids: list[int]) -> tuple[int, int]:
    if self_id in peer_ids:
        raise MOMRingError("invalid peer ids list, self's id should not be included")

    peer_ids.sort()
    npeers = len(peer_ids)

    if not npeers:
        return self_id, self_id
    elif npeers == 1:
        return peer_ids[0], peer_ids[0]

    front_idx = 0
    for idx, p in enumerate(peer_ids):
        if self_id < p:
            front_idx = idx
            break

    back_idx = (npeers + front_idx - 1) % npeers
    front_id = peer_ids[front_idx]
    back_id = peer_ids[back_idx]

    return front_id, back_id


class RingRabbitMQ(MOMRing):
    def __init__(
        self,
        host: str,
        ring_name: str,
        self_id: int,
        peer_ids: list[int],
        exchange_factory: Callable[
            [str, str, list[str]], MOMExchange
        ] = ExchangeRabbitMQ,
    ):
        """
        Create a RingRabbitMQ middleware.

        # Args
        * `host` - the broker host.
        * `ring_name` - the name for the ring cluster.
        * `self_id` - the id of *this* node.
        * `peer_ids` - the ids of the peer nodes, where `self_id`
          is **not** included.
        * `exchange_factory` - (optional) a method for creating the exchange
          to be used. *This optional parameter could be removed from the
          API*.

        # Returns
        A new `RingRabbitMQ` middleware.
        """
        self.id = self_id
        self.peer_ids = peer_ids
        self.front_id, self.back_id = _get_front_back_ids(self.id, peer_ids)

        self.exchange_front = exchange_factory(host, ring_name, [str(self.front_id)])
        self.exchange_back = exchange_factory(host, ring_name, [str(self.back_id)])

    def start_consuming(
        self, on_message_callback: Callable[[bytes, Callable, Callable], None]
    ):
        """
        Starts consuming messages from the back node through the broker in
        a blocking manner and handles them with the passed message handler
        in handle.

        # Args
        * `on_message_callback(msg_bytes, ack, nack)` - the message handling
          function.

        # Errors
        * `MOMDisconnectedError` if the connection with the broker is lost.
        * `MOMMessageError` if an unexpected error occurs.
        """

        self.exchange_back.start_consuming(on_message_callback)

    def stop_consuming(self):
        """
        Stop consuming messages from back peer through the broker.
        """
        self.exchange_back.stop_consuming()

    def send(self, message: bytes):
        """
        Send a message to front peer through the broker.

        # Args
         * `message` - the bytes of the serialized message to be sent.

        # Errors
         * `MOMDisconnectedError` if the connection with the broker is lost.
         * `MOMMessageError` if an unexpected error occurs.
        """
        self.exchange_front.send(message)

    def close(self):
        try:
            self.exchange_front.close()
        except MOMCloseError as e:
            raise MOMCloseError(
                f"failed to close connection with front peer (id: {self.front_id})",
                str(e),
            ) from e

        try:
            self.exchange_back.close()
        except MOMCloseError as e:
            raise MOMCloseError(
                f"failed to close connection with back peer (id: {self.back_id})",
                str(e),
            ) from e
