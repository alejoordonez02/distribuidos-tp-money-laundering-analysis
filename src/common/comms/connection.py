from socket import SHUT_RDWR, socket

LEN_SIZE = 2
"""
The amount of bytes for the length of the (byte) message.
"""
BYTE_ORDER = "big"


class Connection:
    """
    A socket wrapper for sending/receiveing *single* messages.
    """

    def __init__(self, skt: socket):
        self._keep_running = True
        self.skt = skt

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
            if not self._keep_running:
                raise e
            else:
                return b""

        return bytes2

    def __recv_exact(self, amount: int) -> bytes:
        buf = b""
        missing = amount
        while missing:
            received = self.skt.recv(missing)
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
