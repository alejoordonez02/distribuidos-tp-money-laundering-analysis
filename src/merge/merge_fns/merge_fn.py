from abc import abstractmethod
from typing import Any, Iterable
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

    def snapshot_state(self) -> dict[str, Any]:
        raise NotImplementedError("this merge fn is not checkpointable")

    def restore_state(self, snapshot: dict[str, Any]):
        raise NotImplementedError("this merge fn is not checkpointable")

    @abstractmethod
    def get_result(self, client_id: UUID) -> Iterable[Message]:
        """Yield one or more result messages.

        Implementations stream the result in bounded chunks so a single client
        never materializes its whole output in RAM (and never builds one huge
        message). The controller forwards every yielded message and reports the
        count as the downstream EOF expected_count.
        """
        pass
