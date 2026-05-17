from dataclasses import dataclass


@dataclass
class Account:
    bank_name: str
    bank_id: str
    account_number: str
    entity_id: str
    entity_name: str

    # @classmethod
    # def _type(cls):
    #     return MessageType.ACCOUNT
    #
    # def _fields(self) -> list[Any]:
    #     return [
    #         self.bank_name,
    #         self.bank_id,
    #         self.account_number,
    #         self.entity_id,
    #         self.entity_name,
    #     ]
    #
    # @classmethod
    # def _from_fields(cls, fields: list[Any]) -> Self:
    #     bank_name, bank_id, account_number, entity_id, entity_name = fields
    #     return cls(bank_name, bank_id, account_number, entity_id, entity_name)
