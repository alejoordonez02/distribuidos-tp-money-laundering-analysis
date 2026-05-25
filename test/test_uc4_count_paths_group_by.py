from common.comms.messages import Edges, Node, Path, PathCounts
from group_by.group_by_fns import UC4CountPaths


def _combine_batches(batches: list[PathCounts]) -> PathCounts:
    if not batches:
        raise ValueError("empty batch list")
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

    edges = Edges(
        some_uuid,  # type: ignore[reportArgumentType]
        [(origin_node, middle_node), (middle_node, destination_node)],
    )

    expected = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 1},
    )

    fn = UC4CountPaths()
    fn.group_by(edges)

    assert _combine_batches(fn.get_result(some_uuid)) == expected  # type: ignore[reportArgumentType]


def test_five_path_graph():
    some_uuid = "some_uuid"
    origin_node = Node("origin_bank", "origin_account")
    destination_node = Node("destination_bank", "destination_account")
    middles = [Node(f"middle_bank{i}", f"middle_account{i}") for i in range(5)]

    edge_list = []
    for m in middles:
        edge_list.append((origin_node, m))
        edge_list.append((m, destination_node))

    edges = Edges(some_uuid, edge_list)  # type: ignore[reportArgumentType]

    expected = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 5},
    )

    fn = UC4CountPaths()
    fn.group_by(edges)

    assert _combine_batches(fn.get_result(some_uuid)) == expected  # type: ignore[reportArgumentType]
