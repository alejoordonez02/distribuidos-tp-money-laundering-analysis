from abc import abstractmethod

from common.comms.messages import Message


# TODO: esto quizás quedó semánticamente un poco raro con los nuevos
#       msjs de transaction*s* y account*s*.
class FilterFn:
    @abstractmethod
    def filter(self, el: Message) -> Message:
        """
        Filter an element.

        Returns the filtered element.
        """
        pass
