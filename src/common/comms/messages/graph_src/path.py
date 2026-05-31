from .node import Node
from typing import Self

class Path:
    def __init__(self, origin: Node, destination: Node):
        self.key = origin.key + destination.key
        self.origin = origin
        self.destination = destination

    def fields(
        self,
    ) -> tuple[tuple[str, str], tuple[str, str]]:
        return (self.origin.fields(), self.destination.fields())

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other) -> bool:
        return other.key == self.key

    def __str__(self) -> str:
        return f"{self.origin}_{self.destination}"
    
    @classmethod
    def from_string(cls, s: str) -> Self:
        fields = s.split("_")
        return cls(Node.from_str(fields[0]), Node.from_str(fields[1]))