import logging
from uuid import UUID

from common.comms.messages import Transactions, Response

from .join_fn import JoinFn


class UC3Join(JoinFn):
    def __init__(self):
        self._state: dict[UUID, Transactions] = {}

    def join(self, el: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self._state:
            self._state[el.client_id] = Transactions(el.client_id, [])
        self._state[el.client_id].transactions.extend(el.transactions)
        

    def get_response(self, client_id: UUID) -> Response:
        body = "--- UC3 ---"
        for t in self._state.get(client_id, Transactions(client_id, [])).transactions:
            body += f"\nbank_id: {t.from_bank:<20} account: {t.from_account:<20} payment_format: {t.payment_format:<20} amount: {t.amount_paid}"
        body += "\n"
        return Response(client_id, body)
        
