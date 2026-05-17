from abc import abstractmethod
from uuid import UUID

from common.comms.messages import Message, Response


class JoinFn:
    @abstractmethod
    def join(self, el: Message):
        pass

    @abstractmethod
    def get_response(self, client_id: UUID) -> Response:
        pass
