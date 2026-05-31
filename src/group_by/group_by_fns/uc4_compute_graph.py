from collections import defaultdict
from typing import Iterable

from common.comms.messages import Graph, Node, Transactions

from .group_by_fn import GroupByFn

AFFINITY_SHARDS = 100


class UC4ComputeGraph(GroupByFn):
    def group_by(self, msg: Transactions) -> Iterable[tuple[Graph, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        graph = Graph(msg.client_id, {})

        for t in msg.transactions:
            a = Node(t.from_bank, t.from_account)
            b = Node(t.to_bank, t.to_account)

            graph.add_origin(b, a)
            graph.add_destination(a, b)

        affinities: dict[int, Graph] = defaultdict(lambda: Graph(msg.client_id, {}))

        for b, (a, c) in graph.nodes.items():
            affinity_shard_idx = hash(b) % AFFINITY_SHARDS
            affinity_shard = affinities[affinity_shard_idx]

            if b not in affinity_shard.nodes:
                affinity_shard.nodes[b] = (a, c)
                continue

            affinity_shard.nodes[b][0].update(a)
            affinity_shard.nodes[b][1].update(c)

        for affinity, graph in affinities.items():
            yield graph, affinity
