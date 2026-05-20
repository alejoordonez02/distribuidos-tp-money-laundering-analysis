from uuid import UUID

from common.comms.messages import MergedBankData, Response

from .join_fn import JoinFn


class UC2Join(JoinFn):
    def __init__(self):
        self._state: dict[UUID, MergedBankData] = {}

    def join(self, el: MergedBankData):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self._state:
            self._state[el.client_id] = MergedBankData(el.client_id, [])
        self._state[el.client_id].entries.extend(el.entries)

    def get_response(self, client_id: UUID) -> Response:
        body = "--- UC2 ---"
        for bank_id, account, max_amount, bank_name in self._state.get(client_id, MergedBankData(client_id, [])).entries:
            body += f"\nbank_id: {bank_id:<20} account: {account:<20} bank_name: {bank_name:<30} amount: {max_amount}"
        body += "\n"
        return Response(client_id, body)
