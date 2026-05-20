from uuid import UUID

from common.comms.messages import Count, Response

from .join_fn import JoinFn


class UC5Join(JoinFn):
    def __init__(self):
        self._state: dict[UUID, Count] = {}

    def join(self, el: Count):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self._state:
            self._state[el.client_id] = Count(el.client_id, 0)
        self._state[el.client_id].count += el.count

    def get_response(self, client_id: UUID) -> Response:
        count = self._state.get(client_id, Count(client_id, 0)).count
        body = f"--- UC5 ---\ncount: {count}\n"
        return Response(client_id, body)
