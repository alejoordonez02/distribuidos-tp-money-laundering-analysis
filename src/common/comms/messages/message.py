import json
from abc import abstractmethod
from datetime import datetime
from typing import Any
from uuid import UUID

from .errors import UnexpectedMessageError
from .message_types import MessageType


class MessageJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, UUID):
            return str(o)
        elif isinstance(o, datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)


class Message:
    def type(self) -> MessageType:
        """
        Get the type of a message instance.

        # Returns
        A `MessageType` variant for the message.
        """
        return self._type()

    def serialize(self) -> bytes:
        """
        Serializes a `Message` into `bytes`.

        # Returns
        The `bytes` of the serialized message.
        """
        return json.dumps(self._fields(), cls=MessageJSONEncoder).encode("utf-8")

    @classmethod
    @abstractmethod
    def deserialize(cls, bytes2: bytes) -> "Message":
        """
        Deserializes `bytes` into a `Message` variant.

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
        pass

    @classmethod
    def _deserialize(cls, bytes2: bytes):
        fields = json.loads(bytes2.decode("utf-8"))
        if fields[0] != cls._type():
            raise UnexpectedMessageError(
                f"wrong message type\n\texpected: {cls._type()}\n\tgot: {fields[0]}"
            )

        return cls._from_fields(fields[1:])

    @classmethod
    @abstractmethod
    def _type(cls) -> MessageType:
        pass

    @abstractmethod
    def _fields(self) -> list[Any]:
        pass

    @classmethod
    @abstractmethod
    def _from_fields(cls, fields: list[Any]) -> "Message":
        pass
