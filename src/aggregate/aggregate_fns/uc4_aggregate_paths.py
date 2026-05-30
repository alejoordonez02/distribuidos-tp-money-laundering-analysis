from uuid import UUID

from common.comms.messages import PathCounts

from .aggregate_fn import AggregateFn


class UC4AggregatePaths(AggregateFn):
    def __init__(self):
        self.counts: dict[UUID, PathCounts] = {}

    def aggregate(self, msg: PathCounts):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.counts:
            self.counts[msg.client_id] = PathCounts(msg.client_id, {})

        for path, count in msg.counts.items():
            self.counts[msg.client_id].add(path, count)

    def get_result(self, client_id: UUID) -> PathCounts: # type: ignore[reportIncompatibleMethodOverride]
        return self.counts.pop(client_id, PathCounts(client_id, {}))
