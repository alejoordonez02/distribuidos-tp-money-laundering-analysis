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
    GRAPH = 8
    PATH_COUNTS = 9
    COUNT = 10
    SUM_BY_PAYMENT_FORMAT = 11
    AVG_BY_FORMAT = 12
    MERGED_TRANSACTIONS = 13
    RING_DONE = 14
