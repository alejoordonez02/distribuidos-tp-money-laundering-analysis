from enum import IntEnum


class MessageType(IntEnum):
    EOF = 0
    TRANSACTIONS = 1
    ACCOUNTS = 2
    FIN = 3
    RESPONSE = 4
