import logging
from typing import Any, Iterator
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
        logging.debug("max_amounts:\n%s", max_amounts)
        logging.debug("bank_names:\n%s", bank_names)

        entries = [
            (bank_id, account, amount, bank_names[bank_id])
            for bank_id, (account, amount) in max_amounts.items()
            if bank_id in bank_names
        ]

        # UC2 output is bounded by the number of banks (small), so one message is fine; yield to satisfy the streaming MergeFn contract
        merged = MergedBankData(client_id, entries)
        logging.debug("merged:\n%s", merged.__dict__)
        yield merged

    def discard(self, client_id: UUID):
        self._max_amounts.pop(client_id, None)
        self._bank_names.pop(client_id, None)

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "max": {str(c): mbb.data for c, mbb in self._max_amounts.items()},
            "names": {str(c): bn.data for c, bn in self._bank_names.items()},
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self._max_amounts = {
            UUID(c): MaxByBank(
                UUID(c), {b: (a, float(am)) for b, (a, am) in data.items()}
            )
            for c, data in snapshot.get("max", {}).items()
        }
        self._bank_names = {
            UUID(c): BankNames(UUID(c), dict(data))
            for c, data in snapshot.get("names", {}).items()
        }
