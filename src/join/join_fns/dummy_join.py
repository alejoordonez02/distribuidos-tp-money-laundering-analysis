from uuid import UUID

from common.comms.messages import Response, Transactions

from .join_fn import JoinFn


class DummyJoin(JoinFn):
    def __init__(self):
        self.transactions: Transactions = Transactions([])

    def join(self, el: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        self.transactions.transactions.extend(el.transactions)

    def get_response(self, client_id: UUID) -> Response:
        body = f"{[t.__dict__ for t in self.transactions.transactions]}"
        response = Response(client_id, body)

        return response
