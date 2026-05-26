from aggregate.aggregate_fns import UC4AggregatePaths
from common.comms.messages import Node, Path, PathCounts


def test_two_single_path_graphs():
    some_uuid = "some_uuid"
    origin_bank = "origin_bank"
    origin_account = "origin_account"
    destination_bank = "destination_bank"
    destination_account = "destination_account"

    origin_node = Node(origin_bank, origin_account)
    destination_node = Node(destination_bank, destination_account)

    path_count1 = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 1},
    )

    path_count2 = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 1},
    )

    expected = PathCounts(
        some_uuid,  # type: ignore[reportArgumentType]
        {Path(origin_node, destination_node): 2},
    )

    fn = UC4AggregatePaths()
    fn.aggregate(path_count1)
    fn.aggregate(path_count2)

    aggregated = fn.get_result(some_uuid)  # type: ignore[reportArgumentType]

    assert aggregated == expected, (
        f"expected:\n{expected.__dict__}\ngot:\n{aggregated.__dict__}"
    )
