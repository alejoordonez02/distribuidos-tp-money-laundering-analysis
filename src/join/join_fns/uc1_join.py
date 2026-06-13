from typing import Any, Iterator
from uuid import UUID

from common.comms.messages import Response, Transactions

from .join_fn import JoinFn
from .line_spill import Spill, stream_responses


class UC1Join(JoinFn):
    def __init__(self, spill: Spill):
        self._spill = spill

    def snapshot_state(self) -> dict[str, Any]:
        return {"spill": self._spill.snapshot_state()}  # type: ignore[attr-defined]

    def restore_state(self, snapshot: dict[str, Any]):
        self._spill.restore_state(snapshot.get("spill", {}))  # type: ignore[attr-defined]

    def join(self, el: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        for t in el.transactions:
            origin = f"{t.from_bank}-{t.from_account}"
            destination = f"{t.to_bank}-{t.to_account}"
            amount = f"{t.amount_paid}"
            self._spill.append(
                el.client_id,
                f"\norigin: {origin:<20} destination: {destination:<20} amount: {amount}",
            )

    def get_responses(self, client_id: UUID) -> Iterator[Response]:
        yield from stream_responses(self._spill, client_id, "--- UC1 ---")
