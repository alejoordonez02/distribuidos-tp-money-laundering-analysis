from threading import Lock
from uuid import UUID

from client_stream_handler import ClientStreamHandler


class ClientNotFoundError(Exception):
    pass


class ClientStreamMonitor:
    """
    A thread-safe wrapper for the clients dict.
    """

    def __init__(self):
        self.mtx = Lock()
        self.clients: dict[UUID, ClientStreamHandler] = {}

    def add(self, client: ClientStreamHandler):
        """
        Add a client to the list.
        """
        with self.mtx:
            self.clients[client.id] = client

    def remove(self, client_id: UUID):
        """Drop a client's session (on crash) so its responses are no longer routed."""
        with self.mtx:
            self.clients.pop(client_id, None)

    def get(self, client_id: UUID):
        """
        Get a client from the list.

        This method will not remove the client from the list.
        """
        with self.mtx:
            if client_id not in self.clients:
                raise ClientNotFoundError(
                    f"client {client_id} not found, current client list is {[c for c in self.clients.keys()]}"
                )

            return self.clients.get(client_id)
