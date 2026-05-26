from abc import abstractmethod
from uuid import UUID

from common.comms.messages import Message


class MergeFn:
    @abstractmethod
    def left(self, msg: Message):
        """Handle a message from the left stream."""
        pass

    @abstractmethod
    def right(self, msg: Message):
        """Handle a message from the right stream."""
        pass

    @abstractmethod
    def get_result(self, client_id: UUID) -> list[Message]:
        pass
