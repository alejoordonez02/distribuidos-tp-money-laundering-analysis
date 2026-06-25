from typing import Any, Iterator
from uuid import UUID

from common.comms.messages import Response, Transactions

from .join_fn import JoinFn
from .line_spill import Spill, stream_responses


class UC3Join(JoinFn):
    def __init__(self, spill: Spill):
        self._spill = spill

    def snapshot_state(self) -> dict[str, Any]:
        return {"spill": self._spill.snapshot_state()}  # type: ignore[attr-defined]

    def restore_state(self, snapshot: dict[str, Any]):
        self._spill.restore_state(snapshot.get("spill", {}))  # type: ignore[attr-defined]

    def clear_stale_spill(self):
        self._spill.clear_all()  # type: ignore[attr-defined]

    def join(self, el: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        for t in el.transactions:
            self._spill.append(
                el.client_id,
                f"\nbank_id: {t.from_bank:<20} account: {t.from_account:<20} payment_format: {t.payment_format:<20} amount: {t.amount_paid}",
            )

    def get_responses(self, client_id: UUID) -> Iterator[Response]:
        yield from stream_responses(self._spill, client_id, "--- UC3 ---")

    def discard(self, client_id: UUID):
        self._spill.clear(client_id)
