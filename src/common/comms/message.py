import json
from abc import abstractmethod
from enum import Enum
from typing import Any
from uuid import UUID

from .errors import UnexpectedMessageError, UnknownMessageError


# this is used for serializing uuids, otherwise the defult json encoder is used
class UUIDEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, UUID):
            return str(o)
        return json.JSONEncoder.default(self, o)


class MessageType(Enum):
    pass


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
        return json.dumps(self._fields(), cls=UUIDEncoder).encode("utf-8")

    @classmethod
    def deserialize(cls, bytes2: bytes) -> "Message":
        """
        Deserializes `bytes` into a `Message`.

        This method can be used for deserializing bytes into an expected type
        of message instance if called directly from the specific subclass.

        # Args
        * `bytes2` - the `bytes` of the serialized message.

        # Returns
        A new `Message` instance.

        # Errors
        * `UnknownMessageError` if the type field is unknown.
        * `UnexpectedMessageError` if the method is called into a specific
          subclass and the type field does not match the expected one.
        """
        fields = json.loads(bytes2.decode("utf-8"))
        match fields[0]:
            case _:
                raise UnknownMessageError(
                    f"unknown message type {fields[0]} with contents {fields[1:]}"
                )

    @classmethod
    def _deserialize(cls, bytes2: bytes):
        fields = json.loads(bytes2.decode("utf-8"))
        if fields[0] != cls._type().value:
            raise UnexpectedMessageError(
                f"wrong message type\n\texpected: {cls._type().value}\n\tgot: {fields[0]}"
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
