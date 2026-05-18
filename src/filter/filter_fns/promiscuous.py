from common.comms.messages import Transactions

from .filter_fn import FilterFn


class Promiscuous(FilterFn):
    def filter(self, el: Transactions) -> Transactions:  # type: ignore[reportIncompatibleMethodOverride]
        return el
