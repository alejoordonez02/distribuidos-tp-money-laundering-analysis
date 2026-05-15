from threading import Lock
from uuid import UUID

from client_handler import ClientHandler


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
            return self.clients[client_id]
