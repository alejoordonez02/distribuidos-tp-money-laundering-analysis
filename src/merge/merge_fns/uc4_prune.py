import json
import math
from typing import Any, Iterator
from uuid import UUID

from common.checkpoint import PersistentSpill
from common.comms.messages import Graph, HighDegree, Node

from .merge_fn import MergeFn

SPLIT_THRESHOLD = 250_000
_CHUNK_SIZE = 50_000


def _serialize(node: Node, preds: set[Node], succs: set[Node]) -> str:
    return json.dumps([str(node), [str(n) for n in preds], [str(n) for n in succs]])


def _deserialize(line: str) -> tuple[Node, set[Node], set[Node]]:
    raw = json.loads(line)
    return (
        Node.from_str(raw[0]),
        {Node.from_str(n) for n in raw[1]},
        {Node.from_str(n) for n in raw[2]},
    )


class UC4PruneMergeFn(MergeFn):
    def __init__(self, spill: PersistentSpill):
        self._hi_out: dict[UUID, set[Node]] = {}
        self._hi_in: dict[UUID, set[Node]] = {}
        self._spill = spill

    def left(self, msg: HighDegree):  # type: ignore[reportIncompatibleMethodOverride]
        self._hi_out.setdefault(msg.client_id, set()).update(msg.hi_out)
        self._hi_in.setdefault(msg.client_id, set()).update(msg.hi_in)

    def right(self, msg: Graph):  # type: ignore[reportIncompatibleMethodOverride]
        for node, (preds, succs) in msg.nodes.items():
            self._spill.append(msg.client_id, _serialize(node, preds, succs) + "\n")

    def get_result(self, client_id: UUID) -> Iterator[Graph]:  # type: ignore[reportIncompatibleMethodOverride]
        hi_out = self._hi_out.pop(client_id, set())
        hi_in = self._hi_in.pop(client_id, set())

        batch: dict[Node, tuple[set[Node], set[Node]]] = {}
        batch_size = 0
        for line in self._spill.iter_lines(client_id):
            node, preds, succs = _deserialize(line.rstrip("\n"))
            preds = preds & hi_out
            succs = succs & hi_in
            if not preds or not succs:
                continue
            if len(preds) * len(succs) > SPLIT_THRESHOLD:
                yield from self._salted_tiles(client_id, node, preds, succs)
                continue
            batch[node] = (preds, succs)
            batch_size += len(preds) + len(succs)
            if batch_size >= _CHUNK_SIZE:
                yield Graph(client_id, batch)
                batch = {}
                batch_size = 0

        if batch:
            yield Graph(client_id, batch)

    def discard(self, client_id: UUID):
        self._hi_out.pop(client_id, None)
        self._hi_in.pop(client_id, None)
        self._spill.clear(client_id)

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "hi_out": {str(c): [str(n) for n in s] for c, s in self._hi_out.items()},
            "hi_in": {str(c): [str(n) for n in s] for c, s in self._hi_in.items()},
            "spill": self._spill.snapshot_state(),
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self._hi_out = {
            UUID(c): {Node.from_str(n) for n in lst}
            for c, lst in snapshot.get("hi_out", {}).items()
        }
        self._hi_in = {
            UUID(c): {Node.from_str(n) for n in lst}
            for c, lst in snapshot.get("hi_in", {}).items()
        }
        self._spill.restore_state(snapshot.get("spill", {}))

    def clear_stale_spill(self):
        self._spill.clear_all()

    def _salted_tiles(
        self, client_id: UUID, node: Node, preds: set[Node], succs: set[Node]
    ) -> Iterator[Graph]:
        p_count, s_count = len(preds), len(succs)
        chunk_p = max(1, int(math.sqrt(SPLIT_THRESHOLD * p_count / s_count)))
        chunk_s = max(1, int(math.sqrt(SPLIT_THRESHOLD * s_count / p_count)))
        preds_list = list(preds)
        succs_list = list(succs)

        for pi in range(0, p_count, chunk_p):
            pchunk = set(preds_list[pi : pi + chunk_p])
            for si in range(0, s_count, chunk_s):
                schunk = set(succs_list[si : si + chunk_s])
                yield Graph(client_id, {node: (pchunk, schunk)})
