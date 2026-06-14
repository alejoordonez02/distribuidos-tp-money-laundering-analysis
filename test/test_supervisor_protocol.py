import pytest

from common.comms.supervisor import Heartbeat, Register, decode, encode


def test_register_roundtrip():
    msg = Register("default_filter_0", "filter")
    assert decode(encode(msg)) == msg


def test_heartbeat_roundtrip():
    msg = Heartbeat("default_filter_0")
    assert decode(encode(msg)) == msg


def test_register_and_heartbeat_are_distinguishable():
    assert isinstance(decode(encode(Register("n", "k"))), Register)
    assert isinstance(decode(encode(Heartbeat("n"))), Heartbeat)


def test_unknown_tag_rejected():
    import msgpack

    with pytest.raises(ValueError):
        decode(msgpack.packb([99, "n"]))
