from abc import abstractmethod

from .mom import MOM


class MOMExchange(MOM):
    @abstractmethod
    def __init__(
        self,
        host: str,
        exchange_name: str,
        routing_keys: list[str],
        queue_name: str = "",
    ):
        pass

    @abstractmethod
    def send(self, message: bytes, routing_key: str | None = None):
        """Publish a message. `routing_key` targets exactly one bound queue for this
        single message (e.g. a client_id); without it the message fans out to the
        routing keys declared at construction. Routing by key is exchange-only, which
        is why it lives here and not on the plain-queue interface."""
        pass
