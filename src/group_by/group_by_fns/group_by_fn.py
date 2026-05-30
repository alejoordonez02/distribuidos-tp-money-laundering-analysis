from abc import ABC, abstractmethod
from typing import Iterable

from common.comms.messages import Message


class GroupByFn(ABC):
    @abstractmethod
    def group_by(self, msg: Message) -> Iterable[tuple[Message, int]]: ...
