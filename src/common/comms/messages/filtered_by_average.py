from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class FilteredByAverage(Message):
    def __init__(self, client_id: UUID, entries: list[tuple[str, float]]):
        self.client_id = client_id
        self.entries = entries  # Lista de (account, amount)

    @classmethod
    def _type(cls):
        return MessageType.FILTERED_BY_AVERAGE

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[[account, amount] for account, amount in self.entries],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        entries = [(e[0], float(e[1])) for e in fields[1:]]
        return cls(client_id, entries)
