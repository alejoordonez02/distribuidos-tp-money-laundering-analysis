from common.comms.messages import Accounts, BankNames

from .group_by_fn import GroupByFn


class UC2BankNamesGroupByFn(GroupByFn):
    """Groups accounts by bank_id, extracting the bank_name for each."""

    def group_by(self, msg: Accounts) -> Iterator[BankNames]:  # type: ignore[reportIncompatibleMethodOverride]
        bank_id_bank_names = BankNames(
            msg.client_id,
            {a.bank_id: a.bank_name for a in msg.accounts},
        )

        return [bank_id_bank_names,]
