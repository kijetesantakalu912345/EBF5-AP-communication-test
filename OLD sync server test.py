from datetime import datetime
from basic_socket import BasicSocket, SocketConnectionBrokenError
from socket import socket as Socket

HOST = "localhost"
PORT = 4999

# messages with client
PING_MSG = "[Server] ping"
RECEIVED_MSG = "[Server] message received"

# internal status logging
CLIENT_CONNECTION_FAILED = "client attempted to connect but failed."
CLIENT_CONNECTED = "client connected"
CLIENT_DISCONNECTED = "client disconnected"
SERVER_AWAKE = "Server is awake"
SERVER_SHUT_DOWN = "server shutting down"
SERVER_SHUTED_DOWN = "Server shutted down"


class ServerSocket(BasicSocket):
    def __init__(self, host: str, port: int, backlog: int = 5, sock: Socket = None):
        super().__init__(sock)
        self.sock.bind((host, port))
        self.sock.listen(backlog)


class ClientSocket(BasicSocket):
    def __init__(self, server: ServerSocket):
        socket, ret_address = server.sock.accept()
        super().__init__(socket)


def log(message: str):
    now = datetime.now()
    print(f"[{now.isoformat(' ')}] {message}")


def test_server():

    # start server
    server = ServerSocket(HOST, PORT, 5)
    log(SERVER_AWAKE)

    # only 1 client allowed at the same time
    client: ClientSocket = None

    # todo: find when we should turn it off
    is_running = True

    while is_running:
        # connect / reconnect client to the server
        if client is None:
            client = ClientSocket(server)
            log(CLIENT_CONNECTED)
            log(PING_MSG)
            try:
                client.send_utf8(PING_MSG)
            except SocketConnectionBrokenError:
                log(CLIENT_CONNECTION_FAILED)
                client.close()
                client = None
                continue

        # attempt to receive data
        try:
            recv_data = client.receive_utf8()
            log(f"[Client] {recv_data}")
            client.send_utf8(RECEIVED_MSG)
        except SocketConnectionBrokenError:
            # closes the client that was disconnected
            # next loop iteration will wait for a client to reconnect
            log(CLIENT_DISCONNECTED)
            client.close()
            client = None

    # cleanly shut down everything
    log(SERVER_SHUT_DOWN)

    if client is not None:
        client.close()
    server.close()

    log(SERVER_SHUTED_DOWN)


if __name__ == "__main__":
    test_server()
