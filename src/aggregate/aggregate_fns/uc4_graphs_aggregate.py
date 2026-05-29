from collections import defaultdict
from uuid import UUID

from common.comms.messages import Graph, Node, Transactions

from .aggregate_fn import AggregateFn


class UC4ComputeGraph(AggregateFn):
    def __init__(self):
        self._succs: dict[UUID, dict[Node, set[Node]]] = {}
        self._preds: dict[UUID, dict[Node, set[Node]]] = {}

    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._succs:
            self._succs[msg.client_id] = defaultdict(set)
            self._preds[msg.client_id] = defaultdict(set)

        # TODO: agarrar los diccionarios q ya tenemos, y
        #       hacerles update con los q vienen

    def get_result(self, client_id: UUID) -> Graph:
        # TODO: agarrar el los cosos q tenemos computados
        #       y mandarlos

        return Graph(client_id, {})
