from uuid import UUID

from common.comms.messages import Accounts, BankNames

from .group_by_fn import GroupByFn


class UC2BankNamesGroupByFn(GroupByFn):
    """Groups accounts by bank_id, extracting the bank_name for each."""

    def __init__(self):
        self._state: dict[UUID, BankNames] = {}

    def group_by(self, msg: Accounts):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = BankNames(msg.client_id, {})
        state = self._state[msg.client_id]
        for a in msg.accounts:
            state.data[a.bank_id] = a.bank_name

    def get_result(self, client_id: UUID) -> BankNames:  # type: ignore[reportIncompatibleMethodOverride]
        return self._state.pop(client_id, BankNames(client_id, {}))
