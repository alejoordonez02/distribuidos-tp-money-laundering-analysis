from typing import Any, Self
from uuid import UUID

from common.comms.messages import Message, MessageType

from .node import Node
from .path import Path


class PathCounts(Message):
    def __init__(self, client_id: UUID, counts: dict[Path, int]):
        self.client_id = client_id
        self.counts = counts

    def add(self, path: Path):
        if path not in self.counts:
            self.counts[path] = 0

        self.counts[path] += 1

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
            for (((obank, oaccount), (dbank, daccount)), count) in fields[1:]
        }

        return cls(client_id, counts)
