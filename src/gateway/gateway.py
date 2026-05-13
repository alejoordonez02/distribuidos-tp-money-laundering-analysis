from socket import socket
from threading import Thread
from typing import Callable

from client_handler import ClientHandler

from common.comms.connection import Connection
from common.comms.messages import (
    Message,
)
from common.comms.messages.errors import UnknownMessageError
from common.comms.middleware import (
    MessageMiddlewareQueue,
)

BUF_SIZE = 1024


class Gateway:
    def __init__(
        self,
        listener: socket,
        addr: tuple[str, int],
        server_rx: MessageMiddlewareQueue,
        transactions_tx: MessageMiddlewareQueue,
        accounts_tx: MessageMiddlewareQueue,
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
        self.transactions_tx = transactions_tx
        self.accounts_tx = accounts_tx

        self.server_handle: Thread
        self.clients: list[ClientHandler] = []

    def start(self):
        """
        Starts listening for new client requests and server responses.
        """
        self._keep_running = True
        self.listener.listen()

        self._run()

    def _handle_server_responses(self, bytes2: bytes, ack: Callable, nack: Callable):
        try:
            msg = Message.deserialize(bytes2)
        except UnknownMessageError:
            print(f"received unknown {msg} from server")
            return
            # TODO

        match msg.type():  # type: ignore
            case _:
                print("received (msg) from server")
                # TODO

        # send it to client
        # TODO: we are only handling one client
        self.clients[0].send(msg)
        pass

    def _run(self):
        self.server_handle = Thread(
            target=self.server_rx.start_consuming, args=[self._handle_server_responses]
        )

        while self._keep_running:
            # accept new connections
            skt, _ = self.listener.accept()
            conn = Connection(skt)

            # handle those connections
            client = ClientHandler(
                conn, self.transactions_tx, self.accounts_tx
            )  # TODO: concurrency - ~tasks
            self.clients.append(client)
            client.start()

    def stop(self):
        self._keep_running = False
