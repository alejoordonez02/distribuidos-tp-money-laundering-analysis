from abc import abstractmethod
from typing import Iterable

from common.comms.messages import Message


class GroupByFn:
    @abstractmethod
    def group_by(self, msg: Message) -> Iterable[tuple[Message, int]]:
        pass
