import json

from .account import Account
from .eof import EOF
from .errors import UnknownMessageError
from .fin import FIN
from .message import Message
from .message_types import MessageType
from .response import Response
from .transaction import Transaction


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
        case MessageType.EOF:
            return EOF.deserialize(bytes2)
        case MessageType.TRANSACTION:
            return Transaction.deserialize(bytes2)
        case MessageType.ACCOUNT:
            return Account.deserialize(bytes2)
        case MessageType.FIN:
            return FIN.deserialize(bytes2)
        case MessageType.RESPONSE:
            return Response.deserialize(bytes2)
        case _:
            raise UnknownMessageError(
                f"unknown message type {fields[0]} with contents {fields[1:]}"
            )
