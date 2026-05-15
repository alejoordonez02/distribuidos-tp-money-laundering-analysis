from uuid import UUID

from common.comms.messages import Response, Transaction

from .join_fn import JoinFn


class DummyJoin(JoinFn[Transaction]):
    def __init__(self):
        self.transactions: list[Transaction] = []

    def join(self, el: Transaction):
        self.transactions.append(el)

    def get_response(self, client_id: UUID) -> Response:
        body = f"{[t.__dict__ for t in self.transactions]}"
        response = Response(client_id, body)

        return response
