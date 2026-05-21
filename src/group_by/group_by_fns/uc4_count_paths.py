from uuid import UUID

from common.comms.messages import Graph, Path, PathCounts

from .group_by_fn import GroupByFn


class UC4CountPaths(GroupByFn):
    def __init__(self):
        self.client_counts: dict[UUID, PathCounts] = {}

    def aggregate(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        for predecessors, successors in msg.nodes.values():
            for p in predecessors:
                for s in successors:
                    path = Path(p, s)
                    self.client_counts[msg.client_id].add(path)

    def get_result(self, client_id: UUID) -> PathCounts:
        if client_id not in self.client_counts:
            return PathCounts(client_id, {})

        return self.client_counts.pop(client_id)
