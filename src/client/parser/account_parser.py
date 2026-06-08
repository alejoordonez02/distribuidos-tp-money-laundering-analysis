from common.data import Account

from .parser import Parser


class AccountParser(Parser[Account]):
    def parse(self, line: str) -> Account:
        (
            # _,  # row idx NOTE: se ve q cuando el dataset viene de un
            #                     .to_csv(..) se guarda con idx pero los
            #                     originales no lo tienen
            bank_name,
            bank_id,
            account_number,
            entity_id,
            entity_name,
        ) = line.rstrip("\n").split(",")

        account = Account(
            bank_name,
            str(int(bank_id)),  # normalize to int form, consistent with the oracle
            account_number,
            entity_id,
            entity_name,
        )

        return account
