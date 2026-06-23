import logging
import os
from socket import SHUT_RDWR, socket
from threading import Thread
from typing import Callable, Sequence
from uuid import UUID

from client_stream_handler import ClientStreamHandler
from client_stream_monitor import ClientStreamMonitor

from common.comms.transport import Connection
from common.comms.middleware import MOM
from common.graceful_shutdown import setup_graceful_shutdown

# Max seconds a send to a slow client may block before its response thread gives up.
CLIENT_SEND_TIMEOUT_S = int(os.getenv("GATEWAY_CLIENT_SEND_TIMEOUT_S", "10"))


class Gateway:
    def __init__(
        self,
        listener: socket,
        addr: tuple[str, int],
        responses_rx_factory: Callable[[UUID], MOM],
        trans_tx_factory: Callable[[], Sequence[MOM]],
        accs_tx_factory: Callable[[], Sequence[MOM]],
    ):
        """
        Create a new `Gateway`.

        This method will bind the passed listener to the passed address.

        # Args
        * `listener`: A new connections stream.
        * `addr`: the address to which the listener is to be bound.
        * `responses_rx_factory`: builds a client's own response consumer, bound by its
          `client_id` so the broker (not the gateway) routes each response to it.

        # Returns
        A new `Gateway` instance.
        """
        self._keep_running = False
        self.listener = listener
        self.listener.bind(addr)
        self.responses_rx_factory = responses_rx_factory
        self.trans_tx_factory = trans_tx_factory
        self.accs_tx_factory = accs_tx_factory

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
<<<<<<< HEAD
        
        self.clients.stop()
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
        except OSError as e:
            logging.warning(
                "client %s unreachable; dropping response (%s)", response.client_id, e
            )
            self.clients.remove(response.client_id)

        ack()
=======
>>>>>>> a3d3652c08c9a01c1a37b10d3589f26c1eaf1dcb

    def _run(self):
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
                self.responses_rx_factory,
                self.trans_tx_factory, self.accs_tx_factory,
            )
            client.start()
<<<<<<< HEAD
            
        self.server_handle.join()
        self.server_rx.close()
=======

        self.clients.stop_all()
>>>>>>> a3d3652c08c9a01c1a37b10d3589f26c1eaf1dcb
