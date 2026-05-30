import pytest

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


def _all_edges(graph: Graph) -> set[tuple[Node, Node]]:
    result = set()
    for node, (preds, succs) in graph.nodes.items():
        for p in preds:
            result.add((p, node))
        for s in succs:
            result.add((node, s))
    return result


@pytest.mark.skip(reason="este test hay que reescribirlo porque cambió la interfaz")
def test_single_path_graph():
    some_uuid = "some_uuid"
    origin_node = Node("origin_bank", "origin_account")
    middle_node = Node("middle_bank", "middle_account")
    destination_node = Node("destination_bank", "destination_account")

    transactions = Transactions(
        some_uuid,  # type: ignore[reportArgumentType]
        [
            _transaction(
                "origin_bank", "origin_account", "middle_bank", "middle_account"
            ),
            _transaction(
                "middle_bank",
                "middle_account",
                "destination_bank",
                "destination_account",
            ),
        ],
    )

    expected = {(origin_node, middle_node), (middle_node, destination_node)}

    fn = UC4ComputeGraph()
    graph = fn.group_by(transactions)

    assert _all_edges(graph) == expected  # type: ignore[reportArgumentType]


@pytest.mark.skip(reason="este test hay que reescribirlo porque cambió la interfaz")
def test_five_path_graph():
    some_uuid = "some_uuid"
    origin_node = Node("origin_bank", "origin_account")
    destination_node = Node("destination_bank", "destination_account")
    middles = [Node(f"middle_bank{i}", f"middle_account{i}") for i in range(1, 6)]

    transactions = Transactions(
        some_uuid,  # type: ignore[reportArgumentType]
        [
            _transaction(
                "origin_bank", "origin_account", f"middle_bank{i}", f"middle_account{i}"
            )
            for i in range(1, 6)
        ]
        + [
            _transaction(
                f"middle_bank{i}",
                f"middle_account{i}",
                "destination_bank",
                "destination_account",
            )
            for i in range(1, 6)
        ],
    )

    expected = {(origin_node, m) for m in middles} | {
        (m, destination_node) for m in middles
    }

    fn = UC4ComputeGraph()
    graph = fn.group_by(transactions)

    assert _all_edges(graph) == expected  # type: ignore[reportArgumentType]
