import logging
from typing import Any, Self
from uuid import UUID

from .graph_src import Node, Path
from .message import Message
from .message_types import MessageType


class PathMsg(Message):
    def __init__(self, client_id: UUID, path: Path, count: int):
        self.client_id = client_id
        self.path = path
        self.counts = count
        
    def add(self, count: int):
        self.counts += count
        
    @classmethod
    def _type(cls):
        return MessageType.PATH_MSG

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            self.path.fields(),
            self.counts
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        cli_id = UUID(fields[0])
        f = fields[1]
        onode = Node(f[0][0], f[0][1])
        dnode = Node(f[1][0], f[1][1])
        p = Path(onode, dnode)
        return cls(
            client_id=cli_id,
            path= p,
            count=int(fields[2])
        )
