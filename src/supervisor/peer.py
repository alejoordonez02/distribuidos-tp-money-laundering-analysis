from dataclasses import dataclass
from typing import Self


@dataclass
class Peer:
    idx: int
    host: str

    def __gt__(self, other: Self | int) -> bool:
        if self.idx is None or (isinstance(other, Peer) and other.idx is None):
            raise RuntimeError("cannot compare peer without an assigned idx")
        return self.idx > other.idx if isinstance(other, Peer) else self.idx > other  # type: ignore[reportOperatorIssue]

    def __lt__(self, other: Self | int) -> bool:
        if self.idx is None or (isinstance(other, Peer) and not other.idx):
            raise RuntimeError("cannot compare peer without an assigned idx")
        return self.idx < other.idx if isinstance(other, Peer) else self.idx < other  # type: ignore[reportOperatorIssue]
