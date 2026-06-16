from typing import Any
from uuid import UUID

from common.comms.messages import PathCounts, Response

from .join_fn import JoinFn


class UC4Join(JoinFn):
    def __init__(self):
        self.client_responses: dict[UUID, PathCounts] = {}

    def snapshot_state(self) -> dict[str, Any]:
        return {str(c): p.serialize() for c, p in self.client_responses.items()}

    def restore_state(self, snapshot: dict[str, Any]):
        self.client_responses = {
            UUID(c): PathCounts.deserialize(b) for c, b in snapshot.items()
        }

    def join(self, el: PathCounts):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self.client_responses:
            self.client_responses[el.client_id] = PathCounts(el.client_id, {})

        # TODO: esto estaría mejor en `PathCounts`
        self.client_responses[el.client_id].counts.update(el.counts)

    def get_response(self, client_id: UUID) -> Response:  # type: ignore[reportIncompatibleMethodOverride]
        body = "--- UC4 ---"
        for path in self.client_responses.pop(
            client_id, PathCounts(client_id, {})
        ).counts.keys():
            obank = path.origin.bank
            oaccount = path.origin.account
            dbank = path.destination.bank
            daccount = path.destination.account

            body += f"\nbank: {obank:<20} account: {oaccount}"
            body += f"\nbank: {dbank:<20} account: {daccount}"

        body += "\n"
        response = Response(client_id, body)

        return response
