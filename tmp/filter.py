from typing import Callable


class Transaction:
    pass

class Filter:
    def __init__(self, filter_fn: Callable[[list[Transaction], list[Transaction]]]):
        pass
