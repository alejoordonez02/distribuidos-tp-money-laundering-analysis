from typing import Any
from uuid import UUID

from common.comms.messages import MergedBankData, Response

from .join_fn import JoinFn


class UC2Join(JoinFn):
    def __init__(self):
        self.bank_data: dict[UUID, MergedBankData] = {}

    def snapshot_state(self) -> dict[str, Any]:
        return {str(c): m.serialize() for c, m in self.bank_data.items()}

    def restore_state(self, snapshot: dict[str, Any]):
        self.bank_data = {
            UUID(c): MergedBankData.deserialize(b) for c, b in snapshot.items()
        }

    def join(self, el: MergedBankData):  # type: ignore[reportIncompatibleMethodOverride]
        if el.client_id not in self.bank_data:
            self.bank_data[el.client_id] = MergedBankData(el.client_id, [])
        self.bank_data[el.client_id].entries.extend(el.entries)

    def get_response(self, client_id: UUID) -> Response:
        body = "--- UC2 ---"
        for bank_id, account, max_amount, bank_name in self.bank_data.pop(
            client_id, MergedBankData(client_id, [])
        ).entries:
            body += f"\nbank_id: {bank_id:<20} account: {account:<20} bank_name: {bank_name:<30} amount: {max_amount}"

        body += "\n"
        response = Response(client_id, body)

        return response

    def discard(self, client_id: UUID):
        self.bank_data.pop(client_id, None)
