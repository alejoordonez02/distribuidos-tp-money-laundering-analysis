from typing import Any, Self
from uuid import UUID

from .graph_src import Node, Path
from .message import Message
from .message_types import MessageType


class PathMsg(Message):
    def __init__(self, client_id: UUID, path: Path, count: int):
        self.client_id = client_id
        self.path = path
        self.count = count
        
    def add(self, count: int):
        self.count += count
        
    @classmethod
    def _type(cls):
        return MessageType.PATH_MSG

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            str(self.path.origin),
            str(self.path.destination),
            str(self.count)
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        return cls(
            client_id=UUID(fields[0]),
            path=Path(Node.from_str(fields[1]), Node.from_str(fields[2])),
            count=int(fields[3])
        )
