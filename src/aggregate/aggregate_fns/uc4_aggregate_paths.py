from typing import Iterable
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

    def get_result(self, client_id: UUID) -> Iterable[tuple[PathCounts, int]]:
        if client_id not in self.counts:
            return ()

        path_counts = self.counts.pop(client_id)

        for path, count in path_counts.counts.items():
            # TODO: acá va u otro msj, o quizás podríamos
            #       agrupar según la afinidad y mandar todos
            #       los q la comparten en un sólo msj.
            path_count = PathCounts(client_id, {path: count})
            yield path_count, hash(path)
