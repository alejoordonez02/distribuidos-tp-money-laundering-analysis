from typing import Iterable
from uuid import UUID

from common.comms.messages import Graph, HighDegree, Node

from .stateful_fn import StatefulFn

MIN_DEGREE = 5


class UC4Degree(StatefulFn):
    def __init__(self):
        self._out: dict[UUID, dict[str, set[str]]] = {}
        self._in: dict[UUID, dict[str, set[str]]] = {}

    def _capped_update(
        self, store: dict[str, set[str]], node: str, neighbors: set[Node]
    ):
        bucket = store.setdefault(node, set())
        if len(bucket) >= MIN_DEGREE:
            return
        for neighbor in neighbors:
            bucket.add(str(neighbor))
            if len(bucket) >= MIN_DEGREE:
                break

    def transform(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._out:
            self._out[msg.client_id] = {}
            self._in[msg.client_id] = {}

        for node, (predecessors, successors) in msg.nodes.items():
            node = str(node)
            self._capped_update(self._in[msg.client_id], node, predecessors)
            self._capped_update(self._out[msg.client_id], node, successors)

    def get_result(self, client_id: UUID) -> Iterable[tuple[HighDegree, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        if client_id not in self._out:
            return ()

        out = self._out.pop(client_id)
        in_ = self._in.pop(client_id)

        hi_out = {
            Node.from_str(n)
            for n, neighbors in out.items()
            if len(neighbors) >= MIN_DEGREE
        }
        hi_in = {
            Node.from_str(n)
            for n, neighbors in in_.items()
            if len(neighbors) >= MIN_DEGREE
        }

        yield HighDegree(client_id, hi_out, hi_in), 0
