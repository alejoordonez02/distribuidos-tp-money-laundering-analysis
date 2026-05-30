from typing import Iterable

from common.comms.messages import Accounts, BankNames

from .group_by_fn import GroupByFn


class UC2BankNamesGroupByFn(GroupByFn):
    """Groups accounts by bank_id, extracting the bank_name for each."""

    def group_by(self, msg: Accounts) -> Iterable[tuple[BankNames, int]]:  # type: ignore[reportIncompatibleMethodOverride]
        bank_id_bank_names = BankNames(
            msg.client_id,
            {a.bank_id: a.bank_name for a in msg.accounts},
        )

        for bank_id, bank_name in bank_id_bank_names.data.items():
            bank_id_name = BankNames(msg.client_id, {bank_id: bank_name})
            yield bank_id_name, hash(bank_id)
