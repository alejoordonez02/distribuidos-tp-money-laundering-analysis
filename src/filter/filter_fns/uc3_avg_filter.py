from datetime import datetime

from common.comms.messages import FilteredByAverage

from .filter_fn import FilterFn

PERCENTAGE = 0.01

class UC3AvgFilter(FilterFn):
    def filter(self, el: MergedTransactions) -> FilteredByAverage:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = []
        for (account, amount, average) in el.entries:
            if amount < PERCENTAGE * average:
                filtered.append((account, amount))
        
        return FilteredByAverage(el.client_id, filtered)
