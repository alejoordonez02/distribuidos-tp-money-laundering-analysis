from uuid import UUID

from common.comms.messages import MergedBankData, Response

from .join_fn import JoinFn


class UC2Join(JoinFn):
    def __init__(self):
        self._entries: dict[UUID, list[tuple[str, str, float, str]]] = {}

    def join(self, el: MergedBankData):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self._entries:
            self._entries[el.client_id] = []
        self._entries[el.client_id].extend(el.entries)

    def get_response(self, client_id: UUID) -> Response:
        body = "--- UC2 ---"
        for bank_id, account, max_amount, bank_name in self._entries.get(client_id, []):
            body += f"\nbank_id: {bank_id:<20} account: {account:<20} bank_name: {bank_name:<30} amount: {max_amount}"
        body += "\n"
        return Response(client_id, body)
