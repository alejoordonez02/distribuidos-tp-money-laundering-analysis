from .node import Node


class Path:
    def __init__(self, origin: Node, destination: Node):
        self.key = self.origin.key + self.destination.key
        self.origin = origin
        self.destination = destination

    def fields(
        self,
    ) -> tuple[tuple[str, str], tuple[str, str]]:
        return (self.origin.fields(), self.destination.fields())

    def __hash__(self) -> int:
        return hash(self.origin.key + self.destination.key)
