import struct
from socket import SHUT_RDWR, SO_SNDTIMEO, SOL_SOCKET, socket

# TODO: mepa que vamos a tener q partir los msjs q mandamos entre
#       controladores en el server... bajar esto cuando eso esté.
LEN_SIZE = 4
"""
The amount of bytes for the length of the (byte) message.
"""
BYTE_ORDER = "big"


class Connection:
    """
    A socket wrapper for sending/receiveing *single* messages.
    """

    def __init__(self, skt: socket, send_timeout: "int | None" = None):
        self._keep_running = True
        self.skt = skt
        if send_timeout:
            # SO_SNDTIMEO makes sendall raise instead of blocking forever on a dead
            # peer; only the send side is bounded (recv stays blocking).
            self.skt.setsockopt(
                SOL_SOCKET, SO_SNDTIMEO, struct.pack("ll", send_timeout, 0)
            )

    def send(self, bytes2: bytes):
        """
        Send bytes to peer.

        This method will append the amount of bytes that are being sent
        so that the receiver knows what to expect.
        """
        len_bytes = len(bytes2).to_bytes(LEN_SIZE, byteorder=BYTE_ORDER)
        self.skt.sendall(len_bytes + bytes2)

    def recv(self) -> bytes:
        """
        Receive a *single* message from peer.

        If `close()` is called while blocked on receiveing it returns
        empty bytes.
        """
        try:
            len_bytes = int.from_bytes(
                self.__recv_exact(LEN_SIZE), byteorder=BYTE_ORDER
            )
            bytes2 = self.__recv_exact(len_bytes)
        except OSError as e:
            if self._keep_running:
                raise e
            return b""

        return bytes2

    def __recv_exact(self, amount: int) -> bytes:
        buf = b""
        missing = amount
        while missing:
            received = self.skt.recv(missing)
            if not received:
                # Peer closed cleanly mid-read; surface it so recv() can report
                # the closed connection instead of spinning on empty reads.
                raise OSError("connection closed by peer")
            buf += received
            missing -= len(received)

        return buf

    def close(self):
        """
        Close the connection with peer.
        """
        self._keep_running = False
        self.skt.shutdown(SHUT_RDWR)
        self.skt.close()
