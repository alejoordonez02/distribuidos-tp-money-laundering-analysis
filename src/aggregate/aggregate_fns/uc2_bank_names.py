from typing import Any, Iterable
from uuid import UUID

from common.comms.messages import BankNames

from .aggregate_fn import AggregateFn

# NOTE: no puede ser más chico que la cantidad de controladores de adelante (se desperdicia la diferencia)
AFFINITY_SHARDS = 10


class UC2BankNamesAggregateFn(AggregateFn):
    def __init__(self):
        self._state: dict[UUID, BankNames] = {}

    def aggregate(self, msg: BankNames):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._state:
            self._state[msg.client_id] = BankNames(msg.client_id, {})

        self._state[msg.client_id].data.update(msg.data)

    def get_result(self, client_id: UUID) -> Iterable[tuple[BankNames, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        if client_id not in self._state:
            return ()

        bank_names = self._state.pop(client_id)

        for bank_id, bank_name in bank_names.data.items():
            bank_id_name = BankNames(client_id, {bank_id: bank_name})
            yield bank_id_name, hash(bank_id)

    def discard(self, client_id: UUID):
        self._state.pop(client_id, None)

    def snapshot_state(self) -> dict[str, Any]:
        return {str(cid): bn.data for cid, bn in self._state.items()}

    def restore_state(self, snapshot: dict[str, Any]):
        self._state = {
            UUID(cid): BankNames(UUID(cid), dict(data))
            for cid, data in snapshot.items()
        }
