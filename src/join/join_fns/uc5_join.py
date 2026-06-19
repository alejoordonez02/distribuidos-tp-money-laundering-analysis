from typing import Any
from uuid import UUID

from common.comms.messages import Response, TransactionCount

from .join_fn import JoinFn


class UC5Join(JoinFn):
    def __init__(self):
        self._state: dict[UUID, TransactionCount] = {}

    def snapshot_state(self) -> dict[str, Any]:
        return {str(c): t.serialize() for c, t in self._state.items()}

    def restore_state(self, snapshot: dict[str, Any]):
        self._state = {
            UUID(c): TransactionCount.deserialize(b) for c, b in snapshot.items()
        }

    def join(self, el: TransactionCount):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self._state:
            self._state[el.client_id] = TransactionCount(el.client_id, 0)
        self._state[el.client_id].count += el.count

    def get_response(self, client_id: UUID) -> Response:
        count = self._state.pop(client_id, TransactionCount(client_id, 0)).count

        body = f"--- UC5 ---\ncount: {count}\n"
        response = Response(client_id, body)

        return response

    def discard(self, client_id: UUID):
        self._state.pop(client_id, None)
