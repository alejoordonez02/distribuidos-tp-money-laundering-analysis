from abc import abstractmethod


class Parser[El]:
    @abstractmethod
    def parse(self, line: str) -> El:
        pass
