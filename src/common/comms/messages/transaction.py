from datetime import datetime
from typing import Any, Self

from .message import Message
from .message_types import MessageType


class Transaction(Message):
    def __init__(
        self,
        timestamp: datetime,
        from_bank: str,
        from_account: str,
        to_bank: str,
        to_account: str,
        amount_received: float,
        receiving_currency: str,
        amount_paid: float,
        payment_currency: str,
        payment_format: str,
    ):
        self.timestamp = timestamp
        self.from_bank = from_bank
        self.from_account = from_account
        self.to_bank = to_bank
        self.to_account = to_account
        self.amount_received = amount_received
        self.receiving_currency = receiving_currency
        self.amount_paid = amount_paid
        self.payment_currency = payment_currency
        self.payment_format = payment_format

    @classmethod
    def deserialize(cls, bytes2: bytes) -> Self:
        return cls._deserialize(bytes2)  # type: ignore

    @classmethod
    def _type(cls):
        return MessageType.TRANSACTION

    def _fields(self) -> list[Any]:
        return [
            MessageType.TRANSACTION.value,
            self.timestamp,
            self.from_bank,
            self.from_account,
            self.to_bank,
            self.to_account,
            self.amount_received,
            self.receiving_currency,
            self.amount_paid,
            self.payment_currency,
            self.payment_format,
        ]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        (
            timestamp,
            from_bank,
            from_account,
            to_bank,
            to_account,
            amount_received,
            receiving_currency,
            amount_paid,
            payment_currency,
            payment_format,
        ) = fields

        return cls(
            timestamp,
            from_bank,
            from_account,
            to_bank,
            to_account,
            amount_received,
            receiving_currency,
            amount_paid,
            payment_currency,
            payment_format,
        )
