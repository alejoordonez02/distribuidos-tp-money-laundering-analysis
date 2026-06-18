from abc import ABC, abstractmethod
from typing import Any, Iterable
from uuid import UUID

from common.comms.messages import Message


class AggregateFn(ABC):
    @abstractmethod
    def aggregate(self, msg: Message): ...

    @abstractmethod
    def get_result(self, client_id: UUID) -> Iterable[tuple[Message, int]]: ...

    @abstractmethod
    def discard(self, client_id: UUID):
        """Drop a client's accumulated state without producing a result (on abort)."""
        ...

    def snapshot_state(self) -> dict[str, Any]:
        raise NotImplementedError("this aggregate fn is not checkpointable")

    def restore_state(self, snapshot: dict[str, Any]):
        raise NotImplementedError("this aggregate fn is not checkpointable")
