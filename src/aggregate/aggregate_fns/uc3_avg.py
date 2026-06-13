from typing import Any, Iterable
from uuid import UUID

from common.comms.messages import AvgByFormat, SumByPaymentFormat

from .aggregate_fn import AggregateFn


class UC3AvgAggregateFn(AggregateFn):
    def __init__(self):
        self.sum_counts: dict[UUID, SumByPaymentFormat] = {}

    def aggregate(self, msg: SumByPaymentFormat):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self.sum_counts:
            self.sum_counts[msg.client_id] = SumByPaymentFormat(msg.client_id, {})

        client_sum_counts = self.sum_counts[msg.client_id].sum_counts
        for format2, (sum2, count) in msg.sum_counts.items():
            curr = client_sum_counts.get(format2, (0, 0))
            new = (curr[0] + sum2, curr[1] + count)

            client_sum_counts[format2] = new

    def get_result(self, client_id: UUID) -> Iterable[tuple[AvgByFormat, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        if client_id not in self.sum_counts:
            return ()

        sum_counts = self.sum_counts.pop(client_id)
        averages = {fmt: s / c for fmt, (s, c) in sum_counts.sum_counts.items()}

        for format2, average in averages.items():
            format_average = AvgByFormat(client_id, {format2: average})
            yield format_average, hash(format2)

    def snapshot_state(self) -> dict[str, Any]:
        return {
            str(cid): list(sc.sum_counts.items())
            for cid, sc in self.sum_counts.items()
        }

    def restore_state(self, snapshot: dict[str, Any]):
        self.sum_counts = {
            UUID(cid): SumByPaymentFormat(
                UUID(cid), {fmt: (float(s), int(c)) for fmt, (s, c) in entries}
            )
            for cid, entries in snapshot.items()
        }
