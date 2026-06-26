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

        self.client_responses[el.client_id].counts.update(el.counts)

    def get_response(self, client_id: UUID) -> Response:  # type: ignore[reportIncompatibleMethodOverride]
        accounts: set[tuple[Any, Any]] = set()
        for path in self.client_responses.pop(
            client_id, PathCounts(client_id, {})
        ).counts.keys():
            accounts.add((path.origin.bank, path.origin.account))
            accounts.add((path.destination.bank, path.destination.account))

        body = "--- UC4 ---"
        for bank, account in accounts:
            body += f"\nbank: {bank:<20} account: {account}"

        body += "\n"
        response = Response(client_id, body)

        return response

    def discard(self, client_id: UUID):
        self.client_responses.pop(client_id, None)
