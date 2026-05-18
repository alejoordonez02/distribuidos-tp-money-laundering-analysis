from uuid import UUID

from common.comms.messages import Response, Transactions

from .join_fn import JoinFn


class UC1Join(JoinFn):
    def __init__(self):
        self.client_responses: dict[UUID, Transactions] = {}

    def join(self, el: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self.client_responses:
            self.client_responses[el.client_id] = Transactions(el.client_id, [])

        self.client_responses[el.client_id].transactions.extend(el.transactions)

    def get_response(self, client_id: UUID) -> Response:  # type: ignore[reportIncompatibleMethodOverride]
        body = "--- UC1 ---"
        for t in self.client_responses[client_id].transactions:
            origin = f"{t.from_bank}-{t.from_account}"
            destination = f"{t.to_bank}-{t.to_account}"
            amount = f"{t.amount_paid}"

            body += f"\norigin: {origin:<20} destination: {destination:<20} amount: {amount}"

        response = Response(client_id, body)
        return response
