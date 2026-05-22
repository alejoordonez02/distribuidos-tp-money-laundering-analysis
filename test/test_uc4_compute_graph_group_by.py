from common.comms.messages import Graph, Node, Transactions
from common.data import Transaction
from group_by.group_by_fns import UC4ComputeGraph


def _transaction(from_bank: str, from_account: str, to_bank: str, to_account: str):
    return Transaction(
        timestamp=None,  # type: ignore[reportArgumentType]
        from_bank=from_bank,
        from_account=from_account,
        to_bank=to_bank,
        to_account=to_account,
        amount_received=None,  # type: ignore[reportArgumentType]
        receiving_currency=None,  # type: ignore[reportArgumentType]
        amount_paid=None,  # type: ignore[reportArgumentType]
        payment_currency=None,  # type: ignore[reportArgumentType]
        payment_format=None,  # type: ignore[reportArgumentType]
    )


def test_single_path_graph():
    some_uuid = "some_uuid"
    origin_bank = "origin_bank"
    origin_account = "origin_account"
    middle_bank = "middle_bank"
    middle_account = "middle_account"
    destination_bank = "destination_bank"
    destination_account = "destination_account"

    transactions = Transactions(
        some_uuid,  # type: ignore[reportArgumentType]
        [
            # scatter transactions
            _transaction(
                from_bank=origin_bank,
                from_account=origin_account,
                to_bank=middle_bank,
                to_account=middle_account,
            ),
            # gather transactions
            _transaction(
                from_bank=middle_bank,
                from_account=middle_account,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
        ],
    )

    origin_node = Node(origin_bank, origin_account)
    middle_node = Node(middle_bank, middle_account)
    destination_node = Node(destination_bank, destination_account)

    expected_graph = Graph(
        some_uuid,  # type: ignore[reportArgumentType]
        {
            origin_node: (set(), set([middle_node])),
            destination_node: (set([middle_node]), set()),
            middle_node: (set([origin_node]), set([destination_node])),
        },
    )

    fn = UC4ComputeGraph()
    fn.group_by(transactions)

    graph = fn.get_result(some_uuid)  # type: ignore[reportArgumentType]

    assert graph == expected_graph
