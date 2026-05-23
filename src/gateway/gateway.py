import logging
from socket import socket
from threading import Thread
from typing import Callable

from client_handler import ClientHandler
from client_monitor import ClientMonitor, ClientNotFoundError

from common.comms.connection import Connection
from common.comms.messages import (
    Response,
    UnknownMessageError,
)
from common.comms.messages.errors import UnexpectedMessageError
from common.comms.middleware import (
    MessageMiddlewareQueue,
)


class Gateway:
    def __init__(
        self,
        listener: socket,
        addr: tuple[str, int],
        server_rx: MessageMiddlewareQueue,
        trans_tx_factory: Callable[[], MessageMiddlewareQueue],
        accs_tx_factory: Callable[[], MessageMiddlewareQueue],
    ):
        """
        Create a new `Gateway`.

        This method will bind the passed listener to the passed address.

        # Args
        * `listener`: A new connections stream.
        * `addr`: the address to which the listener is to be bound.

        # Returns
        A new `Gateway` instance.
        """
        self._keep_running = False
        self.listener = listener
        self.listener.bind(addr)
        self.server_rx = server_rx
        self.trans_tx_factory = trans_tx_factory
        self.accs_tx_factory = accs_tx_factory

        self.server_handle: Thread
        self.clients = ClientMonitor()

    def start(self):
        """
        Starts listening for new client requests and server responses.
        """
        self._keep_running = True
        self.listener.listen()

        self._run()

    def _handle_server_response(self, bytes2: bytes, ack: Callable, nack: Callable):
        try:
            response = Response.deserialize(bytes2)
        except UnexpectedMessageError as e:
            logging.error(f"received unexpected from server: {e}")
            nack()
            return
        except UnknownMessageError as e:
            logging.error(f"received unknown from server: {e}")
            nack()
            return

        try:
            self.clients.get(response.client_id).send(response)  # type: ignore[reportAttributeAccessIssue]
        except ClientNotFoundError as e:
            logging.error(f"failed to get client response: {e}")
            nack()
            return

        ack()

    def _run(self):
        self.server_handle = Thread(
            target=self.server_rx.start_consuming, args=[self._handle_server_response]
        )
        self.server_handle.start()

        while self._keep_running:
            skt, _ = self.listener.accept()
            conn = Connection(skt)

            client = ClientHandler(conn, self.trans_tx_factory, self.accs_tx_factory)
            client.start()

            self.clients.add(client)

        self.server_handle.join()

    def stop(self):
        self._keep_running = False
