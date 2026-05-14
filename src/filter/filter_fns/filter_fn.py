from abc import abstractmethod


class FilterFn[El]:
    @abstractmethod
    def filter(self, el: El) -> bool:
        """
        Filter an element.

        Returns true if the element should be filtered (dropped).
        """
        pass
