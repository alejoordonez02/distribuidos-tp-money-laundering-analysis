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


def _print_nodes(graph: Graph) -> str:
    nodes_str = ""
    for n, (predecessors, successors) in graph.nodes.items():
        nodes_str += f"--- node: {n.bank}-{n.account} ---\n"
        nodes_str += "\t--- predecessors: ---\n"
        for p in predecessors:
            nodes_str += f"\t- p: {p.bank}-{p.account}\n"

        nodes_str += "\t--- successors: ---\n"
        for s in successors:
            nodes_str += f"\t- p: {s.bank}-{s.account}\n"

    return nodes_str


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

    assert graph == expected_graph, (
        f"expected:\n{_print_nodes(expected_graph)}\ngot:\n{_print_nodes(graph)}"
    )


def test_two_path_graph():
    some_uuid = "some_uuid"
    origin_bank = "origin_bank"
    origin_account = "origin_account"
    middle_bank = "middle_bank"
    middle_account = "middle_account"
    middle_bank2 = "middle_bank2"
    middle_account2 = "middle_account2"
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
            _transaction(
                from_bank=origin_bank,
                from_account=origin_account,
                to_bank=middle_bank2,
                to_account=middle_account2,
            ),
            # gather transactions
            _transaction(
                from_bank=middle_bank,
                from_account=middle_account,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
            _transaction(
                from_bank=middle_bank2,
                from_account=middle_account2,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
        ],
    )

    origin_node = Node(origin_bank, origin_account)
    middle_node = Node(middle_bank, middle_account)
    middle_node2 = Node(middle_bank2, middle_account2)
    destination_node = Node(destination_bank, destination_account)

    expected_graph = Graph(
        some_uuid,  # type: ignore[reportArgumentType]
        {
            origin_node: (set(), set([middle_node, middle_node2])),
            destination_node: (set([middle_node, middle_node2]), set()),
            middle_node: (set([origin_node]), set([destination_node])),
            middle_node2: (set([origin_node]), set([destination_node])),
        },
    )

    fn = UC4ComputeGraph()
    fn.group_by(transactions)

    graph = fn.get_result(some_uuid)  # type: ignore[reportArgumentType]

    assert graph == expected_graph, (
        f"expected:\n{_print_nodes(expected_graph)}\ngot:\n{_print_nodes(graph)}"
    )


def test_five_path_graph():
    some_uuid = "some_uuid"
    origin_bank = "origin_bank"
    origin_account = "origin_account"
    middle_bank = "middle_bank"
    middle_account = "middle_account"
    middle_bank2 = "middle_bank2"
    middle_account2 = "middle_account2"
    middle_bank3 = "middle_bank3"
    middle_account3 = "middle_account3"
    middle_bank4 = "middle_bank4"
    middle_account4 = "middle_account4"
    middle_bank5 = "middle_bank5"
    middle_account5 = "middle_account5"
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
            _transaction(
                from_bank=origin_bank,
                from_account=origin_account,
                to_bank=middle_bank2,
                to_account=middle_account2,
            ),
            _transaction(
                from_bank=origin_bank,
                from_account=origin_account,
                to_bank=middle_bank3,
                to_account=middle_account3,
            ),
            _transaction(
                from_bank=origin_bank,
                from_account=origin_account,
                to_bank=middle_bank4,
                to_account=middle_account4,
            ),
            _transaction(
                from_bank=origin_bank,
                from_account=origin_account,
                to_bank=middle_bank5,
                to_account=middle_account5,
            ),
            # gather transactions
            _transaction(
                from_bank=middle_bank,
                from_account=middle_account,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
            _transaction(
                from_bank=middle_bank2,
                from_account=middle_account2,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
            _transaction(
                from_bank=middle_bank3,
                from_account=middle_account3,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
            _transaction(
                from_bank=middle_bank4,
                from_account=middle_account4,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
            _transaction(
                from_bank=middle_bank5,
                from_account=middle_account5,
                to_bank=destination_bank,
                to_account=destination_account,
            ),
        ],
    )

    origin_node = Node(origin_bank, origin_account)
    middle_node = Node(middle_bank, middle_account)
    middle_node2 = Node(middle_bank2, middle_account2)
    middle_node3 = Node(middle_bank3, middle_account3)
    middle_node4 = Node(middle_bank4, middle_account4)
    middle_node5 = Node(middle_bank5, middle_account5)
    destination_node = Node(destination_bank, destination_account)

    expected_graph = Graph(
        some_uuid,  # type: ignore[reportArgumentType]
        {
            origin_node: (
                set(),
                set(
                    [
                        middle_node,
                        middle_node2,
                        middle_node3,
                        middle_node4,
                        middle_node5,
                    ]
                ),
            ),
            destination_node: (
                set(
                    [
                        middle_node,
                        middle_node2,
                        middle_node3,
                        middle_node4,
                        middle_node5,
                    ]
                ),
                set(),
            ),
            middle_node: (set([origin_node]), set([destination_node])),
            middle_node2: (set([origin_node]), set([destination_node])),
            middle_node3: (set([origin_node]), set([destination_node])),
            middle_node4: (set([origin_node]), set([destination_node])),
            middle_node5: (set([origin_node]), set([destination_node])),
        },
    )

    fn = UC4ComputeGraph()
    fn.group_by(transactions)

    graph = fn.get_result(some_uuid)  # type: ignore[reportArgumentType]

    assert graph == expected_graph, (
        f"expected:\n{_print_nodes(expected_graph)}\ngot:\n{_print_nodes(graph)}"
    )
