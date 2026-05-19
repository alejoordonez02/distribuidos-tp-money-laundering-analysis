from uuid import UUID

from common.comms.messages import BankNames

from .aggregate_fn import AggregateFn


class UC2BankNamesAggregateFn(AggregateFn):
    """Consolidates partial BankNames results, merging all bank_id → bank_name mappings."""

    def __init__(self):
        # client_id → {bank_id → bank_name}
        self._state: dict[UUID, dict[str, str]] = {}

    def accumulate(self, msg: BankNames):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = {}
        self._state[msg.client_id].update(msg.data)

    def get_result(self, client_id: UUID) -> BankNames | None:
        return BankNames(client_id, self._state.get(client_id, {}))
