from common.comms.messages import Graph, Node, Path, PathCounts
from group_by.group_by_fns import UC4CountPaths


def test_single_path_graph():
    some_uuid = "some_uuid"
    origin_bank = "origin_bank"
    origin_account = "origin_account"
    middle_bank = "middle_bank"
    middle_account = "middle_account"
    destination_bank = "destination_bank"
    destination_account = "destination_account"

    origin_node = Node(origin_bank, origin_account)
    middle_node = Node(middle_bank, middle_account)
    destination_node = Node(destination_bank, destination_account)

    graph = Graph(
        some_uuid,  # type: ignore[reportArgumentType]
        {
            origin_node: (set(), set([middle_node])),
            destination_node: (set([middle_node]), set()),
            middle_node: (set([origin_node]), set([destination_node])),
        },
    )

    expected_path_count = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 1},
    )

    fn = UC4CountPaths()
    fn.group_by(graph)

    path_count = fn.get_result(some_uuid)  # type: ignore[reportArgumentType]

    assert path_count == expected_path_count


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

    origin_node = Node(origin_bank, origin_account)
    middle_node = Node(middle_bank, middle_account)
    middle_node2 = Node(middle_bank2, middle_account2)
    destination_node = Node(destination_bank, destination_account)

    graph = Graph(
        some_uuid,  # type: ignore[reportArgumentType]
        {
            origin_node: (set(), set([middle_node, middle_node2])),
            destination_node: (set([middle_node, middle_node2]), set()),
            middle_node: (set([origin_node]), set([destination_node])),
            middle_node2: (set([origin_node]), set([destination_node])),
        },
    )

    expected_path_count = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 2},
    )

    fn = UC4CountPaths()
    fn.group_by(graph)

    path_count = fn.get_result(some_uuid)  # type: ignore[reportArgumentType]

    assert path_count == expected_path_count


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

    origin_node = Node(origin_bank, origin_account)
    middle_node = Node(middle_bank, middle_account)
    middle_node2 = Node(middle_bank2, middle_account2)
    middle_node3 = Node(middle_bank3, middle_account3)
    middle_node4 = Node(middle_bank4, middle_account4)
    middle_node5 = Node(middle_bank5, middle_account5)
    destination_node = Node(destination_bank, destination_account)

    graph = Graph(
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

    expected_path_count = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 5},
    )

    fn = UC4CountPaths()
    fn.group_by(graph)

    path_count = fn.get_result(some_uuid)  # type: ignore[reportArgumentType]

    assert path_count == expected_path_count
