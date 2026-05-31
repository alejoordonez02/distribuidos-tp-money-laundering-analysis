from collections import defaultdict
from typing import Iterator

from common.comms.messages import Graph, Path, PathCounts

from .group_by_fn import GroupByFn

AFFINITY_SHARDS = 100


class UC4CountPaths(GroupByFn):
    def group_by(self, msg: Graph) -> Iterator[tuple[PathCounts, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        affinities: dict[int, PathCounts] = defaultdict(
            lambda: PathCounts(msg.client_id, {})
        )

        for _, (preds, succs) in msg.nodes.items():
            for a in preds:
                for c in succs:
                    if a != c:
                        path = Path(a, c)
                        idx = hash(path) % AFFINITY_SHARDS
                        affinity_shard = affinities[idx]

                        affinity_shard.add(path, 1)

        for affinity, path_counts in affinities.items():
            yield path_counts, affinity
