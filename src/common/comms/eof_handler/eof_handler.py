from abc import ABC, abstractmethod
from uuid import UUID

from common.comms.messages import EOF


class EOFHandler(ABC):
    """
    A component for handling end of file messages.
    """

    @abstractmethod
    def start(self):
        """
        Starts the EOF handler.
        """
        pass

    @abstractmethod
    def stop(self):
        """
        Stops the EOF handler.
        """
        pass

    @abstractmethod
    def handle(self, eof: EOF):
        """
        Handles an end of file message.

        # Args
        * `eof` - the end of file message.
        """
        pass

    @abstractmethod
    def add_processed_count(self, client_id: UUID):
        """
        Increments the count of processed data for a client.

        This method must be called whenever the controller that's
        using the component processes client data.

        # Args
        * `client_id` - the id of the client that owns the
          processed data.
        """
        pass


class StatelessEOFHandler(EOFHandler):
    pass


class StatefulEOFHandler(EOFHandler):
    @abstractmethod
    def downstream(self, eof: EOF):
        pass
