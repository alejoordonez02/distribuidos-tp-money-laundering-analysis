"""Phase 1: lightweight message identity (producer_id + seq) wire-format tests."""

from datetime import datetime
from uuid import uuid4

from common.comms.messages import (
    DEFAULT_PRODUCER,
    EOF,
    Abort,
    TransactionCount,
    Transactions,
    deserialize_message,
)
from common.data import Transaction


def _a_transaction() -> Transaction:
    return Transaction(
        datetime(2022, 1, 1, 12, 0, 0),
        "0001",
        "ACC_A",
        "0002",
        "ACC_B",
        100.0,
        "USD",
        100.0,
        "USD",
        "ACH",
    )


def test_data_message_defaults_to_null_identity():
    msg = TransactionCount(uuid4(), 42)

    got = deserialize_message(msg.serialize())

    assert isinstance(got, TransactionCount)
    assert got.count == 42
    assert got.client_id == msg.client_id
    # Built-in-code messages carry a null identity until a producer stamps them.
    assert got.producer_id == DEFAULT_PRODUCER
    assert got.seq == 0


def test_identity_survives_round_trip_when_stamped():
    msg = TransactionCount(uuid4(), 7)
    msg.producer_id = b"\x11" * 16
    msg.seq = 1234567890

    got = deserialize_message(msg.serialize())

    assert got.producer_id == b"\x11" * 16
    assert got.seq == 1234567890
    assert got.count == 7


def test_transactions_round_trip_preserves_payload_and_identity():
    cid = uuid4()
    msg = Transactions(cid, [_a_transaction(), _a_transaction()])
    msg.producer_id = b"\xab" * 16
    msg.seq = 5

    got = deserialize_message(msg.serialize())

    assert isinstance(got, Transactions)
    assert got.client_id == cid
    assert len(got.transactions) == 2
    assert got.transactions[0].from_account == "ACC_A"
    assert got.transactions[0].amount_received == 100.0
    assert got.producer_id == b"\xab" * 16
    assert got.seq == 5


def test_eof_still_round_trips_with_identity_header():
    cid = uuid4()
    eof = EOF(cid, processed_count=10, expected_count=10, next_expected_count=3, origin=2)

    got = deserialize_message(eof.serialize())

    assert isinstance(got, EOF)
    assert got.client_id == cid
    assert got.processed_count == 10
    assert got.expected_count == 10
    assert got.next_expected_count == 3
    assert got.origin == 2
    # EOF is not stamped by producers; it round-trips with a null identity header.
    assert got.producer_id == DEFAULT_PRODUCER
    assert got.seq == 0


def test_abort_round_trips_with_client_id():
    cid = uuid4()

    got = deserialize_message(Abort(cid).serialize())

    assert isinstance(got, Abort)
    assert got.client_id == cid
    # Abort is a control message; like EOF it carries a null identity header.
    assert got.producer_id == DEFAULT_PRODUCER
    assert got.seq == 0
