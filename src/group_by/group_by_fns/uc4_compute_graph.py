import json
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from uuid import UUID

from common.comms.messages import Graph, Node, Transactions

from .group_by_fn import GroupByFn


def _serialize(succs: dict[Node, set[Node]], preds: dict[Node, set[Node]]) -> str:
    return json.dumps(
        [
            {
                str(node): [str(n) for n in succs_set]
                for node, succs_set in succs.items()
            },
            {
                str(node): [str(n) for n in preds_set]
                for node, preds_set in preds.items()
            },
        ]
    )


def _deserialize(line: str) -> tuple[dict[Node, set[Node]], dict[Node, set[Node]]]:
    succs_raw, preds_raw = json.loads(line)
    return (
        {
            _node_from_str(node): {_node_from_str(n) for n in succs_set}
            for node, succs_set in succs_raw.items()
        },
        {
            _node_from_str(node): {_node_from_str(n) for n in preds_set}
            for node, preds_set in preds_raw.items()
        },
    )


def _node_from_str(s: str) -> Node:
    bank, account = s.split(",")
    return Node(bank, account)


class UC4ComputeGraph(GroupByFn):
    def __init__(self):
        self._files: dict[UUID, Path] = {}
        # self._succs: dict[UUID, dict[Node, set[Node]]] = {}
        # self._preds: dict[UUID, dict[Node, set[Node]]] = {}

    def _file_for(self, client_id: UUID) -> Path:
        if client_id not in self._files:
            fd, path = tempfile.mkstemp(prefix=f"uc4_{client_id}_", suffix=".jsonl")
            os.close(fd)
            self._files[client_id] = Path(path)
        return self._files[client_id]

    def group_by(self, msg: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        # if msg.client_id not in self._succs:
        #     self._succs[msg.client_id] = defaultdict(set)
        #     self._preds[msg.client_id] = defaultdict(set)

        # succs = self._succs[msg.client_id]
        # preds = self._preds[msg.client_id]

        succs: dict[Node, set[Node]] = defaultdict(set)
        preds: dict[Node, set[Node]] = defaultdict(set)

        for t in msg.transactions:
            origin = Node(t.from_bank, t.from_account)
            dest = Node(t.to_bank, t.to_account)
            succs[origin].add(dest)
            preds[dest].add(origin)

        path = self._file_for(msg.client_id)
        with open(path, "a") as f:
            f.write(_serialize(succs, preds) + "\n")

    def get_result(self, client_id: UUID) -> Graph:
        # succs = self._succs.pop(client_id, {})
        # preds = self._preds.pop(client_id, {})

        succs: dict[Node, set[Node]] = defaultdict(set)
        preds: dict[Node, set[Node]] = defaultdict(set)
        path = self._files.pop(client_id, None)

        if path and path.exists():
            with open(path) as f:
                for line in f:
                    line_succs, line_preds = _deserialize(line)
                    for k, v in line_succs.items():
                        succs[k].update(v)
                    for k, v in line_preds.items():
                        preds[k].update(v)
            path.unlink()

        nodes = {
            node: (preds.get(node, set()), succs.get(node, set()))
            for node in succs.keys() | preds.keys()
        }

        return Graph(client_id, nodes)
