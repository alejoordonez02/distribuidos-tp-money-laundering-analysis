from uuid import UUID

from common.comms.messages import PathCounts

from .aggregate_fn import AggregateFn

_BATCH_SIZE = 50_000


class UC4AggregatePaths(AggregateFn):
    def __init__(self):
        self.counts: dict[UUID, PathCounts] = {}

    def aggregate(self, msg: PathCounts):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.counts:
            self.counts[msg.client_id] = PathCounts(msg.client_id, {})

        for path, count in msg.counts.items():
            self.counts[msg.client_id].add(path, count)

    def get_result(self, client_id: UUID) -> list[PathCounts]:
        if client_id not in self.counts:
            return [PathCounts(client_id, {})]

        full = self.counts.pop(client_id)

        if not full.counts:
            return [PathCounts(client_id, {})]

        batches: list[PathCounts] = []
        batch = PathCounts(client_id, {})
        batch_size = 0

        for path, count in full.counts.items():
            batch.add(path, count)
            batch_size += 1
            if batch_size >= _BATCH_SIZE:
                batches.append(batch)
                batch = PathCounts(client_id, {})
                batch_size = 0

        if batch_size > 0:
            batches.append(batch)

        return batches
