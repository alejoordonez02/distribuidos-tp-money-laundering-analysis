from abc import abstractmethod

from .mom import MOM


class MOMRingError(Exception):
    pass


class MOMRing(MOM):
    """
    A message oriented middleware that provides logical ring
    communication among a cluster of nodes.
    """

    @abstractmethod
    def __init__(self, host: str, ring_name: str, self_id: int, peer_ids: list[int]):
        pass
