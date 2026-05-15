from enum import Enum


class MessageType(Enum):
    EOF = 0
    TRANSACTION = 1
    ACCOUNT = 2
    FIN = 3
    RESPONSE = 4
