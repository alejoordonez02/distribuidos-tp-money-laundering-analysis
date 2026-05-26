from abc import abstractmethod
from collections.abc import Iterator
from uuid import UUID

from common.comms.messages import Message


class GroupByFn:
    @abstractmethod
    def group_by(self, msg: Message) -> Iterator[Message] | list[Message] | None:
        pass

    @abstractmethod
    def get_result(self, client_id: UUID) -> list[Message]:
        pass
