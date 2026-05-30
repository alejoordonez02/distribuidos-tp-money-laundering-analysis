from uuid import UUID

from common.comms.messages import NodeMsg, Node, PathMsg
from common.comms.messages.graph_src.path import Path

from .group_by_fn import GroupByFn


class UC4CountPaths(GroupByFn):

    def group_by(self, msg: NodeMsg) -> Iterator[PathMsg]:  # type: ignore[reportIncompatibleMethodOverride]
        preds = msg.predecesors
        succs = msg.succesors
        result = set()
        for a in preds:
            for c in succs:
                if a != c:
                    result.add(PathMsg(msg.client_id, Path(a, c), 1))
        for r in result:
            yield r