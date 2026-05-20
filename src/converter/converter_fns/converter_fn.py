from abc import abstractmethod

from common.comms.messages import Transactions


class ConverterFn:
    @abstractmethod
    def convert(self, msg: Transactions) -> Transactions:
        pass
