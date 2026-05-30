from abc import abstractmethod
from typing import Iterable
from uuid import UUID

from common.comms.messages import Message


class AggregateFn:
    @abstractmethod
    def aggregate(self, msg: Message):
        pass

    @abstractmethod
    def get_result(self, client_id: UUID) -> Iterable[tuple[Message, int]]:
        pass
