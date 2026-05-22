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
