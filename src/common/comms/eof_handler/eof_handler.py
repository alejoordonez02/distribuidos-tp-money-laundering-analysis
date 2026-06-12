from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID

from common.comms.messages import EOF


def snapshot_counts(
    processed: dict[UUID, int], sent: dict[UUID, dict[int, int]]
) -> dict[str, Any]:
    return {
        "processed": {str(k): v for k, v in processed.items()},
        "sent": {
            str(k): {str(s): c for s, c in shards.items()} for k, shards in sent.items()
        },
    }


def restore_counts(
    snapshot: dict[str, Any],
) -> tuple[dict[UUID, int], dict[UUID, dict[int, int]]]:
    processed = {UUID(k): v for k, v in snapshot.get("processed", {}).items()}
    sent = {
        UUID(k): {int(s): c for s, c in shards.items()}
        for k, shards in snapshot.get("sent", {}).items()
    }
    return processed, sent


def total_sent(sent: dict[UUID, dict[int, int]], client_id: UUID) -> int:
    return sum(sent.get(client_id, {}).values())


class EOFHandler(ABC):
    """A component for handling end of file messages."""

    processed_counts: dict[UUID, int]
    sent_data: dict[UUID, dict[int, int]]

    def snapshot_state(self) -> dict[str, Any]:
        return snapshot_counts(self.processed_counts, self.sent_data)

    def restore_state(self, snapshot: dict[str, Any]):
        self.processed_counts, self.sent_data = restore_counts(snapshot)

    @abstractmethod
    def start(self):
        """Starts the EOF handler."""
        ...

    @abstractmethod
    def stop(self):
        """Stops the EOF handler."""
        ...

    @abstractmethod
    def handle(self, eof: EOF):
        """
        Handles an end of file message.

        # Args
        * `eof` - the end of file message.
        """
        ...

    def add_processed_count(self, client_id: UUID):
        """
        Increments the count of processed data for a client.

        This method must be called whenever the controller that's
        using the component processes client data.

        # Args
        * `client_id` - the id of the client that owns the
          processed data.
        """
        if client_id not in self.processed_counts:
            self.processed_counts[client_id] = 0

        self.processed_counts[client_id] += 1

    def add_sent_data_count(self, client_id: UUID, shard: int = 0):
        # FIXME: esto tiene q estar con el mtx en
        #        en los q van en dos threads?
        shards = self.sent_data.setdefault(client_id, {})
        shards[shard] = shards.get(shard, 0) + 1


class StatelessEOFHandler(EOFHandler):
    """
    A component for handling end of file messages in
    stateless controllers.
    """


class StatefulEOFHandler(EOFHandler):
    """
    A component for handling end of file messages in
    stateful controllers.
    """

    @abstractmethod
    def downstream(self, eof: EOF): ...

    @abstractmethod
    def confirm_sent_data(self, client_id: UUID): ...
