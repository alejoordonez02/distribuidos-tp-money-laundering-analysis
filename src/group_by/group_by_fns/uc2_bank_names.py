from uuid import UUID

from common.comms.messages import Accounts, BankNames

from .group_by_fn import GroupByFn


class UC2BankNamesGroupByFn(GroupByFn):
    """Groups accounts by bank_id, extracting the bank_name for each."""

    def __init__(self):
        # client_id → {bank_id → bank_name}
        self._state: dict[UUID, dict[str, str]] = {}

    def accumulate(self, msg: Accounts):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = {}
        state = self._state[msg.client_id]
        for a in msg.accounts:
            state[a.bank_id] = a.bank_name

    def get_result(self, client_id: UUID) -> BankNames | None:
        return BankNames(client_id, self._state.get(client_id, {}))
