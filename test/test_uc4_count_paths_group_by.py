from common.comms.messages import Graph, Node, Path, PathCounts
from group_by.group_by_fns import UC4CountPaths


def _combine_batches(batches: list[PathCounts]) -> PathCounts | None:
    if not batches:
        return None
    combined = PathCounts(batches[0].client_id, {})
    for batch in batches:
        for path, count in batch.counts.items():
            combined.add(path, count)
    return combined


def test_single_path_graph():
    some_uuid = "some_uuid"
    origin_node = Node("origin_bank", "origin_account")
    middle_node = Node("middle_bank", "middle_account")
    destination_node = Node("destination_bank", "destination_account")

    graph = Graph(
        some_uuid,  # type: ignore[reportArgumentType]
        {
            origin_node: (set(), {middle_node}),
            middle_node: ({origin_node}, {destination_node}),
            destination_node: ({middle_node}, set()),
        },
    )

    expected = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 1},
    )

    fn = UC4CountPaths()
    result = fn.group_by(graph)

    assert _combine_batches(result) == expected  # type: ignore[reportArgumentType]


def test_five_path_graph():
    some_uuid = "some_uuid"
    origin_node = Node("origin_bank", "origin_account")
    destination_node = Node("destination_bank", "destination_account")
    middles = [Node(f"middle_bank{i}", f"middle_account{i}") for i in range(5)]

    nodes = {
        origin_node: (set(), set(middles)),
        destination_node: (set(middles), set()),
    }
    for m in middles:
        nodes[m] = ({origin_node}, {destination_node})

    graph = Graph(some_uuid, nodes)  # type: ignore[reportArgumentType]

    expected = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 5},
    )

    fn = UC4CountPaths()
    result = fn.group_by(graph)

    assert _combine_batches(result) == expected  # type: ignore[reportArgumentType]
