from typing import Hashable

from .mom import MOM


class MOMRing(MOM):
    """
    A message oriented middleware that provides logical ring
    communication among a cluster of nodes.
    """

    def __init__(self, self_id: Hashable, peer_ids: list[Hashable]):
        pass
