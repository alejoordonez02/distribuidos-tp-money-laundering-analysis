from collections import defaultdict
from uuid import UUID

from common.comms.messages import Graph, Node, Path, PathCounts
from common.pipeline_config import UC4_PATHS_BATCH_SIZE

from .group_by_fn import GroupByFn


class UC4CountPaths(GroupByFn):
    def group_by(self, msg: Graph) -> list[PathCounts]:  # type: ignore[reportIncompatibleMethodOverride]
        batch_counts: dict[tuple[Node, Node], int] = defaultdict(int)

        for _node_b, (preds, succs) in msg.nodes.items():
            for a in preds:
                for c in succs:
                    if a != c:
                        batch_counts[(a, c)] += 1

        if not batch_counts:
            return []

        results: list[PathCounts] = []
        batch = PathCounts(msg.client_id, {})
        size = 0

        for (a, c), count in batch_counts.items():
            batch.add(Path(a, c), count)
            size += 1
            if size >= UC4_PATHS_BATCH_SIZE:
                results.append(batch)
                batch = PathCounts(msg.client_id, {})
                size = 0

        if size > 0:
            results.append(batch)

        return results

    def get_result(self, client_id: UUID) -> list[PathCounts]:  # type: ignore[reportIncompatibleMethodOverride]
        return []
