import json
from abc import abstractmethod
from datetime import datetime
from typing import Any
from uuid import UUID

import msgpack

from .errors import UnexpectedMessageError
from .message_types import MessageType


class MessageJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, UUID):
            return str(o)
        elif isinstance(o, datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)


def _mp_default(o):
    """msgpack hook mirroring MessageJSONEncoder: UUID/datetime -> str so the
    wire format matches what the `_from_fields` reconstructors expect."""
    if isinstance(o, UUID):
        return str(o)
    if isinstance(o, datetime):
        return str(o)
    raise TypeError(f"Object of type {type(o).__name__} is not msgpack-serializable")


# Wire-format offsets, so gateway and StampingMOM can stamp headers without unpacking the payload.
# Layout: [type:1][client_id:16][producer_id:16][seq:8][msgpack payload]
TYPE_RANGE = slice(0, 1)
PREFIX_RANGE = slice(1, 17)
PRODUCER_RANGE = slice(17, 33)
SEQ_RANGE = slice(33, 41)
MSG_RANGE = slice(41, None)
SEQ_BYTES = SEQ_RANGE.stop - SEQ_RANGE.start
DEFAULT_PREFIX = b"\x00" * 16
DEFAULT_PRODUCER = b"\x00" * (PRODUCER_RANGE.stop - PRODUCER_RANGE.start)
DEFAULT_SEQ = 0


class Message:
    client_id: UUID
    # Null until a producer (StampingMOM/gateway) stamps the message on the wire.
    producer_id: bytes = DEFAULT_PRODUCER
    seq: int = DEFAULT_SEQ

    def type(self) -> MessageType:
        """
        Get the type of a message instance.

        # Returns
        The `MessageType` variant of the message.
        """
        return self._type()

    def serialize(self) -> bytes:
        """
        Serializes a `Message` into `bytes`.

        Layout: ``[1-byte type][16-byte client_id][16-byte producer_id]``
        ``[8-byte seq][msgpack payload]``. The header fields sit at fixed
        positions so the gateway and the middleware can stamp them without
        unpacking the payload. client_id is the first field of `_fields()` by
        convention; messages without one (Hello) get a null prefix.

        # Returns
        The `bytes` of the serialized message.
        """
        fields = self._fields()
        client_id = getattr(self, "client_id", None)
        if client_id:
            prefix = client_id.bytes
            fields = fields[1:]
        else:
            prefix = DEFAULT_PREFIX
        return (
            bytes([int(self._type())])
            + prefix
            + self.producer_id
            + self.seq.to_bytes(SEQ_BYTES, "big")
            + msgpack.packb(fields, default=_mp_default)
        )

    @classmethod
    def deserialize(cls, bytes2: bytes) -> "Message":
        """
        Deserializes `bytes` into a specific `Message` variant.

        This method is useful so that matching the message type after
        deserializing is not necessary when there there is only one
        expected variant.

        # Args
        * `bytes2` - the `bytes` of the serialized message.

        # Returns
        A new `Message` instance.

        # Example
        ```python
        eof: EOF = EOF.deserialize(bytes2)
        ```

        # Errors
        * `UnexpectedMessageError` if the the type field does not match
          the expected one.
        """
        if bytes2[0] != cls._type():
            raise UnexpectedMessageError(
                f"wrong message type\n\texpected: {cls._type()}\n\tgot: {bytes2[0]}"
            )

        client_id = UUID(bytes=bytes2[PREFIX_RANGE])
        fields = msgpack.unpackb(
            bytes2[MSG_RANGE], raw=False, use_list=True, strict_map_key=False
        )
        msg = cls._from_fields([str(client_id), *fields])
        msg.producer_id = bytes2[PRODUCER_RANGE]
        msg.seq = int.from_bytes(bytes2[SEQ_RANGE], "big")
        return msg

    @classmethod
    @abstractmethod
    def _type(cls) -> MessageType:
        pass

    @abstractmethod
    def _fields(self) -> list[Any]:
        """
        Returns the fields of the `Message`.

        `Message` subclasses must return the fields that are to be
        serialized/deserialized when sending/receiveing messages
        corresponding their attributes.

        # Returns
        The list of *fields* for the `Message` instance.
        """
        pass

    @classmethod
    @abstractmethod
    def _from_fields(cls, fields: list[Any]) -> "Message":
        """
        Create a `Message` instance from a field list.

        These fields will match those returned in the `_fields()`
        method.

        # Args
        * fields: the liest of fields for creating the `Message`
          variant.

        # Returns
        A new `Message` instance.
        """
        pass
