from abc import abstractmethod

from common.comms.messages import Message


class FilterFn:
    @abstractmethod
    def filter(self, el: Message) -> Message:
        """
        Filter an element.

        Returns the filtered element.
        """
        pass
