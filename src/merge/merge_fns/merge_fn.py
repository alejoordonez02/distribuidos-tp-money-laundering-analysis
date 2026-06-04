from abc import abstractmethod
from typing import Iterable
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
    def get_result(self, client_id: UUID) -> Iterable[Message]:
        """Yield one or more result messages.

        Implementations stream the result in bounded chunks so a single client
        never materializes its whole output in RAM (and never builds one huge
        message). The controller forwards every yielded message and reports the
        count as the downstream EOF expected_count.
        """
        pass
