from abc import abstractmethod
from uuid import UUID

from common.comms.messages import Message


class GroupByFn:
    @abstractmethod
    def accumulate(self, msg: Message):
        pass

    @abstractmethod
    def get_result(self, client_id: UUID) -> Message | None:
        pass
