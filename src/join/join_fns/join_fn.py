from abc import abstractmethod
from typing import Callable
from uuid import UUID

from common.comms.messages import MessageType, Response, deserialize_message


class JoinFn[El]:
    def _handle_message(self, bytes2: bytes, ack: Callable, nack: Callable):
        msg = deserialize_message(bytes2)
        match msg.type().value:
            case MessageType.TRANSACTION.value:
                self.client_responses.append(msg)  # type: ignore
            case MessageType.EOF.value:
                self._handle_eof(msg)  # type: ignore
            case _:
                raise RuntimeError(f"unexpected message {msg.__dict__}")

        ack()

    @abstractmethod
    def join(self, el: El):
        pass

    @abstractmethod
    def get_response(self, client_id: UUID) -> Response:
        pass
