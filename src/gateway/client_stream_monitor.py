from threading import Lock
from uuid import UUID

from client_stream_handler import ClientStreamHandler


class ClientStreamMonitor:
    """A thread-safe registry of live client handlers.

    Responses no longer flow through here (each handler owns its own broker queue), so
    this is purely a lifecycle set: it tracks who is connected and tears every handler
    down on shutdown. No central lookup-by-id is needed any more.
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
        """Drop a client's session (on crash) so it is no longer tracked or stopped."""
        with self.mtx:
            self.clients.pop(client_id, None)

    def stop_all(self):
        """Stop every client's session and close its socket (on gateway shutdown).
        `stop` already waits for the handler's own thread, so no separate join here."""
        with self.mtx:
            clients = list(self.clients.values())
            self.clients.clear()
        for client in clients:
            client.stop()