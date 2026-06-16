"""Supervisor protocol: the wire vocabulary between each node and the
supervisor. Deliberately separate from the internal pipeline protocol — a node
only announces who it is and that it is still alive. Tags are append-only so the
protocol stays backward compatible as it grows."""

from dataclasses import dataclass
from typing import Union

import msgpack

_REGISTER = 0
_HEARTBEAT = 1


@dataclass
class Register:
    """First message after connecting: announces the node's identity and kind."""

    node_id: str
    kind: str


@dataclass
class Heartbeat:
    """Periodic liveness signal from an already-registered node."""

    node_id: str


Message = Union[Register, Heartbeat]


def encode(msg: Message) -> bytes:
    if isinstance(msg, Register):
        return msgpack.packb([_REGISTER, msg.node_id, msg.kind])
    if isinstance(msg, Heartbeat):
        return msgpack.packb([_HEARTBEAT, msg.node_id])
    raise ValueError(f"unknown supervisor message: {type(msg).__name__}")


def decode(data: bytes) -> Message:
    tag, *rest = msgpack.unpackb(data, raw=False, use_list=True)
    if tag == _REGISTER:
        return Register(*rest)
    if tag == _HEARTBEAT:
        return Heartbeat(*rest)
    raise ValueError(f"unknown supervisor message tag: {tag}")
