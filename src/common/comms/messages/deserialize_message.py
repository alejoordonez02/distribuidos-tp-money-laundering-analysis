import json

from .accounts import Accounts
from .avg_by_format import AvgByFormat
from .bank_names import BankNames
from .eof import EOF
from .errors import UnknownMessageError
from .fin import FIN
from .graph import Graph
from .max_by_bank import MaxByBank
from .merged_bank_data import MergedBankData
from .merged_transactions import MergedTransactions
from .message import Message
from .message_types import MessageType
from .path_count import PathCounts
from .response import Response
from .ring_done import RingDone
from .sum_by_payment_format import SumByPaymentFormat
from .transaction_count import TransactionCount
from .transactions import Transactions
from .node_msg import NodeMsg

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
        case MessageType.TRANSACTIONS:
            return Transactions.deserialize(bytes2)
        case MessageType.ACCOUNTS:
            return Accounts.deserialize(bytes2)
        case MessageType.FIN:
            return FIN.deserialize(bytes2)
        case MessageType.RESPONSE:
            return Response.deserialize(bytes2)
        case MessageType.MAX_BY_BANK:
            return MaxByBank.deserialize(bytes2)
        case MessageType.BANK_NAMES:
            return BankNames.deserialize(bytes2)
        case MessageType.MERGED_BANK_DATA:
            return MergedBankData.deserialize(bytes2)
        case MessageType.GRAPH:
            return Graph.deserialize(bytes2)
        case MessageType.PATH_COUNTS:
            return PathCounts.deserialize(bytes2)
        case MessageType.SUM_BY_PAYMENT_FORMAT:
            return SumByPaymentFormat.deserialize(bytes2)
        case MessageType.AVG_BY_FORMAT:
            return AvgByFormat.deserialize(bytes2)
        case MessageType.MERGED_TRANSACTIONS:
            return MergedTransactions.deserialize(bytes2)
        case MessageType.COUNT:
            return TransactionCount.deserialize(bytes2)
        case MessageType.RING_DONE:
            return RingDone.deserialize(bytes2)
        case MessageType.NODEMSG:
            return NodeMsg.
        case _:
            raise UnknownMessageError(
                f"unknown message type {fields[0]} with contents {fields[1:]}"
            )
