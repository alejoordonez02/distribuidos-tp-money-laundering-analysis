from typing import Any, Self
from uuid import UUID

from common.data.transaction import Transaction

from .message import Message
from .message_types import MessageType


class MergedTransactions(Message):
    def __init__(
        self,
        client_id: UUID,
        transactions: list[Transaction],
        averages: dict[str, float],
    ):
        self.client_id = client_id
        self.transactions = transactions
        self.averages = averages

    @classmethod
    def _type(cls):
        return MessageType.MERGED_TRANSACTIONS

    def _fields(self) -> list[Any]:
        return [
            self.client_id,
            len(self.transactions),
            *[
                [
                    t.timestamp,
                    t.from_bank,
                    t.from_account,
                    t.to_bank,
                    t.to_account,
                    t.amount_received,
                    t.receiving_currency,
                    t.amount_paid,
                    t.payment_currency,
                    t.payment_format,
                ]
                for t in self.transactions
            ],
            *[
                [payment_format, average]
                for payment_format, average in self.averages.items()
            ],
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        transactions_amount = int(fields[1])
        transaction_fields = fields[2 : 2 + transactions_amount]
        transactions = [
            Transaction(
                timestamp=fields[0],
                from_bank=fields[1],
                from_account=fields[2],
                to_bank=fields[3],
                to_account=fields[4],
                amount_received=float(fields[5]),
                receiving_currency=fields[6],
                amount_paid=float(fields[7]),
                payment_currency=fields[8],
                payment_format=fields[9],
            )
            for fields in transaction_fields
        ]

        data_fields = fields[2 + transactions_amount :]
        data = {entry[0]: float(entry[1]) for entry in data_fields}

        return cls(client_id, transactions, data)
