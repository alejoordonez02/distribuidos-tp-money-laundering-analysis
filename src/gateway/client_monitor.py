from threading import Lock
from uuid import UUID

from client_handler import ClientHandler


class ClientNotFoundError(Exception):
    pass


class ClientMonitor:
    """
    A thread-safe wrapper for the clients dict.
    """

    def __init__(self):
        self.mtx = Lock()
        self.clients: dict[UUID, ClientHandler] = {}

    def add(self, client: ClientHandler):
        """
        Add a client to the list.
        """
        with self.mtx:
            self.clients[client.id] = client

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
