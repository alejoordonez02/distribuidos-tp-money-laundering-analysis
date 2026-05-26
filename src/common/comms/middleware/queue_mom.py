from abc import abstractmethod

from .mom import MOM


class MOMQueue(MOM):
    @abstractmethod
    def __init__(self, host: str, queue_name: str):
        pass
