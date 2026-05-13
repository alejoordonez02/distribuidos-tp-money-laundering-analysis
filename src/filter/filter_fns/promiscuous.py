from common.comms.messages import Transaction

from .filter_fn import FilterFn


class Promiscuous(FilterFn[Transaction]):
    def filter(self, el: Transaction) -> bool:
        return True
