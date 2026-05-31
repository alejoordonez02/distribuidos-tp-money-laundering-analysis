from typing import Any, Self
from uuid import UUID

from .message import Message
from .message_types import MessageType


class RingSentData(Message):
    """
    The amount of data that was sent to the next clúster
    in the pipeline.

    This message is currently only used by stateful end
    of file handlers, as sent data is only known by a
    stateful controller when getting the result, at which
    time both end of file and ring done rounds are done,
    thus a last one is needed to figure out what to put
    in the `expected_count` field of the `EOF` that's
    to be downstreamed.
    """

    def __init__(self, client_id: UUID, origin: int, sent_data: int):
        self.client_id = client_id
        self.origin = origin
        self.sent_data = sent_data

    @classmethod
    def _type(cls):
        return MessageType.RING_SENT_DATA

    def _fields(self) -> list[Any]:
        return [self.client_id, self.origin, self.sent_data]

    @classmethod
    def _from_fields(cls, fields: list[Any]) -> Self:
        client_id = UUID(fields[0])
        return cls(client_id, *fields[1:])
