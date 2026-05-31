from typing import Iterable
from uuid import UUID

from common.comms.messages import BankNames

from .aggregate_fn import AggregateFn

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
        # affinities: dict[int, tuple[str, str]] = {}
        #
        # for bank_id, bank_name in bank_names.data.items():
        #     affinity_shard = hash(bank_id) % AFFINITY_SHARDS
        #     affinities[affinity_shard] = bank_id, bank_name
        #
        # for affinity, (bank_id, bank_name) in affinities.items():
        #     bank_id_name = BankNames(client_id, {bank_id: bank_name})
        #     yield bank_id_name, affinity

        for bank_id, bank_name in bank_names.data.items():
            bank_id_name = BankNames(client_id, {bank_id: bank_name})
            yield bank_id_name, hash(bank_id)
