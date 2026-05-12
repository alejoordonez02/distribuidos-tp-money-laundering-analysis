import json

from .eof import EOF
from .errors import UnknownMessageError
from .message import Message
from .message_types import MessageType


def deserialize_message(bytes2: bytes) -> Message:
    """
    Deserializes `bytes` into a `Message`.

    # Args
    * `bytes2` - the `bytes` of the serialized message.

    # Returns
    A new `Message` instance.

    # Errors
    * `UnknownMessageError` if the type field is unknown.
    """
    fields = json.loads(bytes2.decode("utf-8"))
    match fields[0]:
        case MessageType.EOF.value:
            return EOF.deserialize(bytes2)
        case _:
            raise UnknownMessageError(
                f"unknown message type {fields[0]} with contents {fields[1:]}"
            )
