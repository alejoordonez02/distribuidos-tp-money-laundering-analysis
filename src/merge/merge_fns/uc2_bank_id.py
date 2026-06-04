import logging
from typing import Iterator
from uuid import UUID

from common.comms.messages import BankNames, MaxByBank, MergedBankData

from .merge_fn import MergeFn


class UC2BankIdMergeFn(MergeFn):
    def __init__(self):
        self._max_amounts: dict[UUID, MaxByBank] = {}
        self._bank_names: dict[UUID, BankNames] = {}

    def left(self, msg: MaxByBank):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._max_amounts:
            self._max_amounts[msg.client_id] = MaxByBank(msg.client_id, {})

        state = self._max_amounts[msg.client_id].data

        for bank_id, (account, amount) in msg.data.items():
            curr = state.get(bank_id)
            if not curr or amount > curr[1]:
                state[bank_id] = (account, amount)

    def right(self, msg: BankNames):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._bank_names:
            self._bank_names[msg.client_id] = BankNames(msg.client_id, {})

        self._bank_names[msg.client_id].data.update(msg.data)

    def get_result(self, client_id: UUID) -> Iterator[MergedBankData]:  # type: ignore[reportIncompatibleMethodOverride]
        max_amounts = self._max_amounts.pop(client_id, MaxByBank(client_id, {})).data
        bank_names = self._bank_names.pop(client_id, BankNames(client_id, {})).data
        logging.debug(f"max_amounts:\n{max_amounts}")
        logging.debug(f"bank_names:\n{bank_names}")

        entries = [
            (bank_id, account, amount, bank_names[bank_id])
            for bank_id, (account, amount) in max_amounts.items()
            if bank_id in bank_names
        ]

        # UC2 output is bounded by the number of banks (small), so a single
        # message is fine; yield to satisfy the streaming MergeFn contract.
        merged = MergedBankData(client_id, entries)
        logging.debug(f"merged:\n{merged.__dict__}")
        yield merged
