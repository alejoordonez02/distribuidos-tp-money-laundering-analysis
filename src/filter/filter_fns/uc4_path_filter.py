from common.comms.messages import PathCounts

from .filter_fn import FilterFn

MIN_PATH_COUNT = 5


class UC4PathFilter(FilterFn):
    def filter(self, el: PathCounts) -> PathCounts:  # type: ignore[reportIncompatibleMethodOverride]
        filtered = {
            path: count for path, count in el.counts.items() if count >= MIN_PATH_COUNT
        }

        return PathCounts(el.client_id, filtered)
