from abc import ABC, abstractmethod
from typing import Callable


class MOM(ABC):
    """
    A message oriented middleware.
    """

    @abstractmethod
    def start_consuming(
        self, on_message_callback: Callable[[bytes, Callable, Callable], None]
    ):
        """
        Starts consuming messages from the broker in a blocking manner and
        handles them with the passed message handler in handle.

        # Args
        * `on_message_callback(msg_bytes, ack, nack)` - the message handling
          function.

        # Errors
        * `MOMDisconnectedError` if the connection with the broker is lost.
        * `MOMMessageError` if an unexpected error occurs.
        """
        pass

    @abstractmethod
    def stop_consuming(self):
        """
        Stop consuming messages from the broker.
        """
        pass

    @abstractmethod
    def send(self, message: bytes):
        """
        Send a message to the broker.

        # Args
        * `message` - the bytes of the serialized message to be sent.

        # Errors
        * `MOMDisconnectedError` if the connection with the broker is lost.
        * `MOMMessageError` if an unexpected error occurs.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the connection with the broker.

        # Errors
        * `MOMClosedError` if the connection with the broker is lost.
        """
        pass
