from abc import abstractmethod
from typing import Any, Iterator
from uuid import UUID

from common.comms.messages import Message, Response


class JoinFn:
    @abstractmethod
    def join(self, el: Message):
        pass

    def snapshot_state(self) -> dict[str, Any]:
        raise NotImplementedError("this join fn is not checkpointable")

    def restore_state(self, snapshot: dict[str, Any]):
        raise NotImplementedError("this join fn is not checkpointable")

    def get_response(self, client_id: UUID) -> Response:
        """Single-message result. Small UCs implement this; the route handler
        calls `get_responses`, whose default wraps this as one (last) chunk."""
        raise NotImplementedError

    def get_responses(self, client_id: UUID) -> Iterator[Response]:
        """Stream a UC's result as one or more Response chunks (last chunk has
        ``last=True``). Default = one chunk; UCs that can produce huge results
        (UC1, UC3) override this to chunk under RabbitMQ's max_message_size."""
        yield self.get_response(client_id)

    @abstractmethod
    def discard(self, client_id: UUID):
        """Drop a client's accumulated state without producing a result (on abort)."""
        ...
