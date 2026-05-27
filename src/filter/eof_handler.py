from abc import abstractmethod
from uuid import UUID

from common.comms.messages import EOF


class EOFHandler:
    @abstractmethod
    def start(self): pass

    @abstractmethod
    def stop_consuming(self): pass

    @abstractmethod
    def stop(self): pass

    @abstractmethod
    def close(self): pass

    @abstractmethod
    def handle(self, eof: EOF): pass

    @abstractmethod
    def add_processed_count(self, client_id: UUID): pass
