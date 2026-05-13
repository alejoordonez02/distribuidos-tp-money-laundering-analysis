from abc import abstractmethod


class FilterFn[El]:
    @abstractmethod
    def filter(self, el: El) -> bool:
        pass
