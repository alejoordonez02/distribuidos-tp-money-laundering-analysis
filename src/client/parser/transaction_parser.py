from common.data import Transaction, fast_datetime

from .parser import Parser


class TransactionParser(Parser[Transaction]):
    def parse(self, line: str) -> Transaction:
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
            _,  # label
        ) = line.rstrip("\n").split(",")

        transaction = Transaction(
            fast_datetime(timestamp),
            str(int(from_bank)),
            from_account,
            str(int(to_bank)),
            to_account,
            float(amount_received),
            receiving_currency,
            float(amount_paid),
            payment_currency,
            payment_format,
        )

        return transaction
