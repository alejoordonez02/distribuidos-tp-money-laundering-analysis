from typing import Any, Self
from uuid import UUID

from .graph_src import Node, Path
from .message import Message
from .message_types import MessageType


class PathCounts(Message):
    def __init__(self, client_id: UUID, counts: dict[Path, int]):
        self.client_id = client_id
        self.counts = counts

    def add(self, path: Path, count: int):
        if path not in self.counts:
            self.counts[path] = 0

        self.counts[path] += count

    @classmethod
    def _type(cls):
        return MessageType.PATH_COUNTS

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            [(path.fields(), count) for (path, count) in self.counts.items()],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        counts = {
            Path(Node(obank, oaccount), Node(dbank, daccount)): count
            for (((obank, oaccount), (dbank, daccount)), count) in fields[1]
        }

        return cls(client_id, counts)

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, PathCounts)
            and other.client_id == self.client_id
            and other.counts == self.counts
        )
