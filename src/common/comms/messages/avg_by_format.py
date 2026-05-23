from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class AvgByFormat(Message):
   
    def __init__(self, client_id: UUID, averages: dict[str, float]):
        self.client_id = client_id
        self.averages = averages  # payment_format → amount_average

    @classmethod
    def _type(cls):
        return MessageType.AVG_BY_FORMAT

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[[payment_format, amount_average] for payment_format, amount_average in self.averages.items()],
        ]
        

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        data = {e[0]: float(e[1]) for e in fields[1:]}
        return cls(client_id, data)