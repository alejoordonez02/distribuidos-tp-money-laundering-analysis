from enum import IntEnum


class MessageType(IntEnum):
    EOF = 0
    TRANSACTIONS = 1
    ACCOUNTS = 2
    FIN = 3
    RESPONSE = 4
    MAX_BY_BANK = 5
    BANK_NAMES = 6
    MERGED_BANK_DATA = 7
    COUNT = 8
