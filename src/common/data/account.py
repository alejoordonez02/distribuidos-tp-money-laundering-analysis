from dataclasses import dataclass


@dataclass
class Account:
    bank_name: str
    bank_id: str
    account_number: str
    entity_id: str
    entity_name: str
