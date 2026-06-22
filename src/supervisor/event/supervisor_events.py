from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum

from common.comms.transport import Connection


class EventType(IntEnum):
    LEADER_DOWN = 0
    PEER_CONNECTION = 1
    NEW_LEADER = 2


class SupervisorEvent(ABC):
    @abstractmethod
    def type(self) -> EventType: ...


class LeaderDown(SupervisorEvent):
    def type(self) -> EventType:
        return EventType.LEADER_DOWN


@dataclass
class PeerConnection(SupervisorEvent):
    conn: Connection

    def type(self) -> EventType:
        return EventType.PEER_CONNECTION


@dataclass
class NewLeader(SupervisorEvent):
    idx: int

    def type(self) -> EventType:
        return EventType.NEW_LEADER
