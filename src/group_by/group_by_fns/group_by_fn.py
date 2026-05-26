from abc import abstractmethod
from uuid import UUID

from common.comms.messages import Message


class GroupByFn:
    @abstractmethod
    def group_by(self, msg: Message) -> list[Message] | None:
        pass

    @abstractmethod
    def get_result(self, client_id: UUID) -> list[Message]:
        pass
