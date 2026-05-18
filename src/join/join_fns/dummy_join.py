from uuid import UUID

from common.comms.messages import Response, Transactions

from .join_fn import JoinFn


class DummyJoin(JoinFn):
    def __init__(self):
        self.client_transactions: dict[UUID, Transactions] = {}

    def join(self, el: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self.client_transactions:
            self.client_transactions[el.client_id] = Transactions(el.client_id, [])

        self.client_transactions[el.client_id].transactions.extend(el.transactions)

    def get_response(self, client_id: UUID) -> Response:
        if client_id not in self.client_transactions:
            self.client_transactions[client_id] = Transactions(client_id, [])

        body = (
            f"{[t.__dict__ for t in self.client_transactions[client_id].transactions]}"
        )
        response = Response(client_id, body)

        return response
