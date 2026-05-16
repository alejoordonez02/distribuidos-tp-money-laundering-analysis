from enum import IntEnum


class MessageType(IntEnum):
    EOF = 0
    TRANSACTION = 1
    ACCOUNT = 2
    FIN = 3
    RESPONSE = 4
