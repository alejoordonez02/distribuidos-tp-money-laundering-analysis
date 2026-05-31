from typing import Iterator

from common.comms.messages import NodeMsg, PathMsg
from common.comms.messages.graph_src.path import Path

from .group_by_fn import GroupByFn


class UC4CountPaths(GroupByFn):
    def group_by(self, msg: NodeMsg) -> Iterator[tuple[PathMsg, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        preds = msg.predecesors
        succs = msg.succesors
        paths: set[PathMsg] = set()
        for a in preds:
            for c in succs:
                if a != c:
                    paths.add(PathMsg(msg.client_id, Path(a, c), 1))

        for path in paths:
            yield path, hash(path)

