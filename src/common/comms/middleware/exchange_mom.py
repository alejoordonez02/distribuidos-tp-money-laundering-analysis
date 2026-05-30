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
