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


class Message:
    client_id: UUID

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

        # Returns
        The `bytes` of the serialized message.
        """
        # 1-byte message-type prefix + msgpack of the fields. The prefix lets the
        # gateway route a message by peeking byte 0 without unpacking the payload;
        # msgpack replaces JSON (far cheaper encode/decode in the hot path, and
        # works on both CPython (C ext) and PyPy (pure-Python fallback)).
        return bytes([int(self._type())]) + msgpack.packb(
            self._fields(), default=_mp_default
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

        return cls._from_fields(
            msgpack.unpackb(bytes2[1:], raw=False, use_list=True, strict_map_key=False)
        )

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
