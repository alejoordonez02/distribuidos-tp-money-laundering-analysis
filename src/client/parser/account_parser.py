from common.data import Account

from .parser import Parser


class AccountParser(Parser[Account]):
    def parse(self, line: str) -> Account:
        (
            bank_name,
            bank_id,
            account_number,
            entity_id,
            entity_name,
        ) = line.rstrip("\n").split(",")

        account = Account(
            bank_name,
            str(int(bank_id)),
            account_number,
            entity_id,
            entity_name,
        )

        return account
