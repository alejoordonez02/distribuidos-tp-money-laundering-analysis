from typing import Iterable


class ResultYielder:
    def get_result(self, client) -> Iterable[tuple[Message, int]]: ...

    """
    Returns an iterable of the pieces of the computed result for
    a client along with an integer that represents the affinity
    of that specific result.
    """
