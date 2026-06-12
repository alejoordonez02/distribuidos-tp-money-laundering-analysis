from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class RingBarrier(Message):
    """Wire form of the RingCompletion barrier token. Circulates the ring once all
    peers have emitted, collecting each peer's per-shard sent counts so the leader
    can forward one downstream EOF per shard with the cluster total."""

    def __init__(
        self,
        client_id: UUID,
        origin: int,
        sent_by: dict[int, dict[int, int]] | None = None,
    ):
        self.client_id = client_id
        self.origin = origin
        self.sent_by = sent_by or {}

    @classmethod
    def _type(cls):
        return MessageType.RING_BARRIER

    def _fields(self) -> list[Any]:
        return [self.client_id, self.origin, self.sent_by]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        return cls(client_id, *fields[1:])
