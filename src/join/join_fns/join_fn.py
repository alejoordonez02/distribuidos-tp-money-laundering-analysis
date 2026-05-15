from abc import abstractmethod
from uuid import UUID

from common.comms.messages import Response


class JoinFn[El]:
    @abstractmethod
    def join(self, el: El):
        pass

    @abstractmethod
    def get_response(self, client_id: UUID) -> Response:
        pass
