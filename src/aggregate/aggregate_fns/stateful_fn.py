from abc import ABC, abstractmethod
from typing import Iterable
from uuid import UUID

from common.comms.messages import Message


class StatefulFn(ABC):
    @abstractmethod
    def transform(self, msg: Message): ...

    @abstractmethod
    def get_result(self, client_id: UUID) -> Iterable[tuple[Message, int]]: ...
