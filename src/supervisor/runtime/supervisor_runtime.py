from abc import ABC, abstractmethod


class SupervisorRuntime(ABC):
    @abstractmethod
    def start(self): ...
