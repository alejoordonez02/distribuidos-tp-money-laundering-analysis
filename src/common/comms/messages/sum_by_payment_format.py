from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class SumByPaymentFormat(Message):
    def __init__(self, client_id: UUID, sum_counts: dict[str, tuple[float, int]]):
        self.client_id = client_id
        self.sum_counts = sum_counts

    @classmethod
    def _type(cls):
        return MessageType.SUM_BY_PAYMENT_FORMAT

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            *[
                [payment_format, total_sum, transactions_count]
                for payment_format, (
                    total_sum,
                    transactions_count,
                ) in self.sum_counts.items()
            ],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        sum_counts = {
            payment_format: (total_sum, transactions_count)
            for payment_format, total_sum, transactions_count in fields[1:]
        }

        return cls(client_id, sum_counts)
