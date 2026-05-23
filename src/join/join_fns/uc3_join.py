import logging
from uuid import UUID

from common.comms.messages import FilteredByAverage, Response

from .join_fn import JoinFn


class UC3Join(JoinFn):
    def __init__(self):
        self._state: dict[UUID, FilteredByAverage] = {}

    def join(self, el: FilteredByAverage):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self._state:
            self._state[el.client_id] = FilteredByAverage(el.client_id, [])
        self._state[el.client_id].entries.extend(el.entries)
        

    def get_response(self, client_id: UUID) -> Response:
        body = "--- UC3 ---"
        for bank_id , origin_account, payment_format, amount in self._state.get(client_id, FilteredByAverage(client_id, [])).entries:
            body += f"\nbank_id: {bank_id:<20} account: {origin_account:<20} payment_format: {payment_format:<20} amount: {amount}"
        body += "\n"
        return Response(client_id, body)
        
