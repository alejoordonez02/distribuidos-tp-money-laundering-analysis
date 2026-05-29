class Node:
    def __init__(
        self,
        bank: str,
        account: str,
    ):
        self.key = bank + account
        self.bank = bank
        self.account = account

    def fields(self) -> tuple[str, str]:
        return (self.bank, self.account)

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other) -> bool:
        return other.key == self.key

    def __str__(self) -> str:
        return f"{self.bank},{self.account}"