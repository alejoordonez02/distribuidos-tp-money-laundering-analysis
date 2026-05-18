from datetime import datetime

from common.data import Transaction

from .parser import Parser

DATETIME_FORMAT = "%Y/%m/%d %H:%M"


class TransactionParser(Parser[Transaction]):
    def parse(self, line: str) -> Transaction:
        (
            _,  # idx
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
            _,  # label
        ) = line.rstrip("\n").split(",")

        transaction = Transaction(
            datetime.strptime(timestamp, DATETIME_FORMAT),
            from_bank,
            from_account,
            to_bank,
            to_account,
            float(amount_received),
            receiving_currency,
            float(amount_paid),
            payment_currency,
            payment_format,
        )

        return transaction
