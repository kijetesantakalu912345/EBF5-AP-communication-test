from socket import socket as Socket, AF_INET, SOCK_STREAM, SHUT_RDWR
# bytes 1-4 are an unsigned 32 bit length. maybe a bit overkill, but i think it's fine.

HEADER_SIZE = 4  # n bytes for the size (int) of the payload to receive / send


class SocketConnectionBrokenError(RuntimeError):
    def __init__(self, *args):
        super().__init__("socket connection broken", *args)


class BasicSocket:
    """A socket wrapper class with basic functions"""

    def __init__(self, sock: Socket = None, *, timeout: float = -1):
        self.sock: Socket = Socket(AF_INET, SOCK_STREAM) if sock is None else sock
        if timeout >= 0:
            self.sock.settimeout(timeout)

    def connect(self, host, port: int):
        self.sock.connect((host, port))

    def close(self):
        # https://docs.python.org/3/howto/sockets.html#disconnecting
        self.sock.shutdown(SHUT_RDWR)
        self.sock.close()

    # ----- helpers -----

    def _send_all(self, data: bytes):
        """Sends bytes using the socket connection

        :param data: the bytes of the message to send
        :type data: bytes
        :raises SocketConnectionBrokenError: socket connection is broken/interrupted
        """
        view = memoryview(data)  # zero-copy when slicing bellow
        total = 0
        while total < len(view):
            sent = self.sock.send(view[total:])
            if sent == 0:
                raise SocketConnectionBrokenError()
            total += sent

    def _recv_exact(self, n: int) -> bytes:
        """Attempt to receive an exact number of bytes

        :param n: the number of bytes awaited
        :type n: int
        :raises SocketConnectionBrokenError: socket connection is broken/interrupted
        :return: the bytes received
        :rtype: bytes
        """
        buf = bytearray(n)
        view = memoryview(buf)  # zero-copy when slicing bellow
        total = 0
        while total < n:
            chunk = self.sock.recv_into(view[total:])
            if chunk == 0:
                raise SocketConnectionBrokenError()
            total += chunk
        return bytes(buf)

    # ----- protocol -----

    def send_utf8(self, msg: str) -> None:
        """Sends a message with utf-8 encoding

        :param msg: the messgae to send
        :type msg: str
        :raises SocketConnectionBrokenError: socket connection is broken/interrupted while writing the message
        """
        data = msg.encode("utf-8")
        header = len(data).to_bytes(HEADER_SIZE, "big", signed=False)
        self._send_all(header)
        self._send_all(data)

    def receive_utf8(self) -> str:
        """Returns the next received utf-8 message

        :raises SocketConnectionBrokenError: socket connection is broken/interrupted while reading the message
        :return: The received message
        :rtype: str
        """
        header = self._recv_exact(HEADER_SIZE)
        length = int.from_bytes(header, "big", signed=False)
        data = self._recv_exact(length)
        return data.decode("utf-8")
