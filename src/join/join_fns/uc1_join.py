from typing import Iterator
from uuid import UUID

from common.comms.messages import Response, Transactions

from .join_fn import JoinFn
from .line_spill import LineSpill, stream_responses


class UC1Join(JoinFn):
    def __init__(self):
        # Lines are spilled to disk as they arrive instead of buffering the full
        # Transaction objects in RAM — on Large UC1 is ~7.7M rows, which OOMs the
        # node if held in memory until EOF.
        self._spill = LineSpill("UC1Join")

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
        # Stream UC1 in chunks: on Large it's ~7.7M lines (~560MB), which exceeds
        # RabbitMQ's max_message_size as a single message — chunking keeps every
        # response message small and the client reassembles them in order.
        yield from stream_responses(self._spill, client_id, "--- UC1 ---")
