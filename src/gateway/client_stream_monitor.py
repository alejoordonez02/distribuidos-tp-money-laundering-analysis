import logging
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
            client = self.clients.pop(client_id, None)

    def stop_all(self):
        """Stop every client's writer and close its socket (on gateway shutdown)."""
        logging.warning("Monitor stop all")
        with self.mtx:
            clients = list(self.clients.values())
            self.clients.clear()
        for client in clients:
            logging.warning("Prestop")
            client.stop()
            logging.warning("Postop")
        logging.warning("Out of stop")
        for client in clients:
            logging.warning("Prejoin")
            client.join()
            logging.warning("Posjoin")

        logging.warning("Stop all ended")