from abc import abstractmethod
from typing import Generic, TypeVar

El = TypeVar("El")


class Parser(Generic[El]):
    @abstractmethod
    def parse(self, line: str) -> El:
        pass
