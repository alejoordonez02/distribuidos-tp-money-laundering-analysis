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

    def get_result(self, client_id: UUID) -> Iteratior[AvgByFormat]:  # type: ignore[reportIncompatibleMethodOverride]
        sc = self.sum_counts.pop(client_id, None)
        if not sc:
            return AvgByFormat(client_id, {})

        averages = {fmt: s / c for fmt, (s, c) in sc.sum_counts.items()}
        return [AvgByFormat(client_id, averages),]
