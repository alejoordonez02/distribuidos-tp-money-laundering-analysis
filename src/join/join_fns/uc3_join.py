from typing import Iterator
from uuid import UUID

from common.comms.messages import Response, Transactions

from .join_fn import JoinFn
from .line_spill import LineSpill, stream_responses


class UC3Join(JoinFn):
    def __init__(self):
        # Same disk-spill as UC1Join: on Large UC3 is ~3.5M rows, too many to hold
        # the Transaction objects in RAM until the client EOF.
        self._spill = LineSpill("UC3Join")

    def join(self, el: Transactions):  # type: ignore[reportIncompatibleMethodOverride]
        for t in el.transactions:
            self._spill.append(
                el.client_id,
                f"\nbank_id: {t.from_bank:<20} account: {t.from_account:<20} payment_format: {t.payment_format:<20} amount: {t.amount_paid}",
            )

    def get_responses(self, client_id: UUID) -> Iterator[Response]:
        # Stream UC3 in chunks (on Large ~3.5M lines) so no single response
        # message exceeds RabbitMQ's max_message_size.
        yield from stream_responses(self._spill, client_id, "--- UC3 ---")
