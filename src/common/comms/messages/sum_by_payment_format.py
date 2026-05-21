from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class SumByPaymentFormat(Message):

    def __init__(self, client_id: UUID, data: dict[str, tuple[float, int]]):
        self.client_id = client_id
        self.data = data  # payment_format → (total_sum, transaction_amounts)
        

    @classmethod
    def _type(cls):
        return MessageType.SUM_BY_PAYMENT_FORMAT

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[[payment_format, total_sum, transaction_amounts] for payment_format, (total_sum, transaction_amounts) in self.data.items()],
        ]
        

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        data = {e[0]: (float(e[1]), int(e[2])) for e in fields[1:]}
        return cls(client_id, data)
        
