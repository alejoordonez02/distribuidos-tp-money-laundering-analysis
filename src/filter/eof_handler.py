from abc import abstractmethod
from uuid import UUID

from common.comms.messages import EOF


class EOFHandler:
    @abstractmethod
    def start(self): ...

    @abstractmethod
    def stop(self): ...

    @abstractmethod
    def handle(self, eof: EOF): ...

    @abstractmethod
    def add_processed_count(self, client_id: UUID): ...
