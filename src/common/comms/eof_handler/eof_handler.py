from abc import ABC, abstractmethod
from uuid import UUID

from common.comms.messages import EOF


class EOFHandler(ABC):
    """A component for handling end of file messages."""

    processed_counts: dict[UUID, int]
    next_expected_counts: dict[UUID, int]

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

    def add_next_expected_processed_counts(self, client_id: UUID):
        # FIXME: esto tiene q estar con el mtx en
        #        en los q van en dos threads?
        if client_id not in self.next_expected_counts:
            self.next_expected_counts[client_id] = 0

        self.next_expected_counts[client_id] += 1


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
