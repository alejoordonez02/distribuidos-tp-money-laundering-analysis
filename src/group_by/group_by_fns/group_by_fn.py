from abc import abstractmethod

from common.comms.messages import Message


class GroupByFn:
    @abstractmethod
    def group_by(self, msg: Message) -> Message:
        pass
