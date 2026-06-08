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

        # Normalize bank ids to their integer form (drop leading zeros: "020" ->
        # "20"). The raw dataset writes banks with leading zeros, but the oracle
        # reads them as int32, so the client must match or every bank-bearing
        # result (UC1/UC2/UC3) mismatches. Doing it here lets the client read the
        # ORIGINAL dataset directly — no normalized 15GB copy needed.
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
