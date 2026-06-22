import logging
import os
from socket import SHUT_RDWR, socket
from threading import Thread
from typing import Callable

from client_stream_handler import ClientStreamHandler
from client_stream_monitor import ClientStreamMonitor, ClientNotFoundError

from common.comms.transport import Connection
from common.comms.messages import (
    Response,
    UnknownMessageError,
)
from common.comms.messages.errors import UnexpectedMessageError
from common.comms.middleware import (
    MOMQueue,
)
from common.graceful_shutdown import setup_graceful_shutdown

# Cap how long a send to a client may block before its per-client writer treats it as
# gone: long enough for a slow-but-alive client, a safety net behind the per-client
# writer threads that already isolate a stuck send from the shared response consumer.
CLIENT_SEND_TIMEOUT_S = int(os.getenv("GATEWAY_CLIENT_SEND_TIMEOUT_S", "10"))


class Gateway:
    def __init__(
        self,
        listener: socket,
        addr: tuple[str, int],
        server_rx: MOMQueue,
        trans_tx_factory: Callable[[], MOMQueue],
        accs_tx_factory: Callable[[], MOMQueue],
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
        self.clients = ClientStreamMonitor()

    def start(self):
        """
        Starts listening for new client requests and server responses.
        """
        self._keep_running = True
        self.listener.listen()
        setup_graceful_shutdown(self.stop)
        self._run()

    def stop(self):
        self._keep_running = False
        try:
            self.listener.shutdown(SHUT_RDWR)
            self.listener.close()
        except OSError as e:
            # TODO: handle specific OSError cases (e.g. already closed)
            logging.error("!!! UNHANDLED OSError in gateway stop: %s", e, exc_info=True)
        self.server_rx.stop_consuming()

    def _handle_server_response(self, bytes2: bytes, ack: Callable, nack: Callable):
        try:
            response = Response.deserialize(bytes2)
        except UnexpectedMessageError as e:
            logging.error("received unexpected from server: %s", e)
            nack()
            return
        except UnknownMessageError as e:
            logging.error("received unknown from server: %s", e)
            nack()
            return

        try:
            self.clients.get(response.client_id).send(response)  # type: ignore[reportAttributeAccessIssue]
        except ClientNotFoundError:
            pass

        ack()

    def _run(self):
        self.server_handle = Thread(
            target=self.server_rx.start_consuming, args=[self._handle_server_response]
        )
        self.server_handle.start()

        while self._keep_running:
            try:
                skt, _ = self.listener.accept()
            except OSError as e:
                # TODO: handle specific OSError cases (e.g. socket closed on shutdown vs real error)
                logging.error("!!! UNHANDLED OSError in gateway accept loop: %s", e, exc_info=True)
                break
            conn = Connection(skt, send_timeout=CLIENT_SEND_TIMEOUT_S)
            client = ClientStreamHandler(
                conn, self.clients.add, self.clients.remove,
                self.trans_tx_factory, self.accs_tx_factory,
            )
            client.start()

        self.clients.stop_all()
        self.server_handle.join()
        self.server_rx.close()
