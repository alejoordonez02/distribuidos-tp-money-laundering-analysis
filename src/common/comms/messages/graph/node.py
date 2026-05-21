class Node:
    def __init__(
        self,
        bank: str,
        account: str,
    ):
        self.key = self.bank + self.account
        self.bank = bank
        self.account = account

    def fields(self) -> tuple[str, str]:
        return (self.bank, self.account)

    def __hash__(self):
        return hash(self.key)
