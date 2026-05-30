import asyncio
from socket import socket as Socket, AF_INET, SOCK_STREAM, SHUT_RDWR
import selectors
from datetime import datetime

HOST = "localhost"
PORT = 4999
# I HAVE MADE UP MY MIND: I'm gonna try doing it with select() in an asyncio task.
# I would do it the way archipelago does it but archipelago's way only sends messages in order due to an
# implementation detail with create_task().
# Apparently for archipelago the order that messages are sent in doesn't matter so it's fine.
# Personally I don't really want to rely on that assumption.
# So I'm doing it differently.

class SocketConnectionBrokenError(RuntimeError):
    def __init__(self, *args):
        super().__init__("socket connection broken", *args)

def log(message: str):
    now = datetime.now()
    print(f"[{now.isoformat(' ')}] {message}")

# https://docs.python.org/3/library/selectors.html#examples
class TestAsyncCommunicationAndOtherWorkAtTheSameTime:
    def __init__(self, host: str, port: int, timeout: float = -1):
        # Supporting IPV6 would probably be nice.
        # But it'll probably just be running on localhost or a local network with IPV4 LAN addresses anyway.
        self.server_sock: Socket = Socket(AF_INET, SOCK_STREAM)
        self.server_sock.bind((host, port))
        self.server_sock.listen(1) # CHECK THIS
        self.server_sock.setblocking(False)

        self.select_task: asyncio.Task | None = None
        # self.sync_message_sending_task: asyncio.Task | None = None
        self.other_work_task: asyncio.Task | None = None
        self.exit_event: asyncio.Event = asyncio.Event()
        
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.server_sock, selectors.EVENT_READ, data=self.accept_connection_callback)
        
        self.client_sock: None | Socket = None

        self.message_bytes_to_send = bytearray()
        self.received_message_bytes = bytearray()
        self.incoming_message_final_length: int = 0
        self.is_waiting_for_new_message: bool = True

        if timeout >= 0:
            self.server_sock.settimeout(timeout)

    def add_UTF8_message_to_send_queue(self, UTF8_message: str):
        log(f"sending: {UTF8_message}")
        length_bytes = len(UTF8_message).to_bytes(4, "big", signed=False)
        message_bytes = UTF8_message.encode("utf-8")
        self.message_bytes_to_send += length_bytes
        self.message_bytes_to_send += message_bytes

    def accept_connection_callback(self, sock: Socket, mask):
        if self.client_sock is Socket and self.client_sock.fileno() != -1:
            log("client attempted to connect before the previous client socket connection was broken, ignoring.")
            return
        self.client_sock, address = sock.accept()
        log(f"accepting connection from {address}")
        self.client_sock.setblocking(False)
        self.selector.register(self.client_sock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=self.client_readwrite_callback)

    def client_readwrite_callback(self, client: Socket, mask):
        if mask & selectors.EVENT_READ:
            log("reading...")

            new_bytes = client.recv(2 ** 16)
            self.received_message_bytes += new_bytes
            done_reading = False

            if len(new_bytes) == 0:
                self.disconnect_client()
                raise SocketConnectionBrokenError

            while not done_reading:
                log(f"start: {self.received_message_bytes}")
                if self.is_waiting_for_new_message:
                    if len(self.received_message_bytes) >= 4:
                        self.incoming_message_final_length = int.from_bytes(self.received_message_bytes[:4], "big", signed=False)
                        self.is_waiting_for_new_message = False
                    else:
                        done_reading = True

                if not self.is_waiting_for_new_message:
                    if len(self.received_message_bytes) >= self.incoming_message_final_length:
                        log("message fully received!")
                        if len(self.received_message_bytes) > self.incoming_message_final_length:
                            log("len(self.received_message_fragments) > self.incoming_message_final_length.")
                        message_text: str = self.received_message_bytes[4:4 + self.incoming_message_final_length].decode("utf-8")
                        self.on_message_received(message_text)

                        #self.received_message_fragments.clear()
                        self.received_message_bytes = self.received_message_bytes[4 + self.incoming_message_final_length:]
                        self.incoming_message_final_length = 0
                        self.is_waiting_for_new_message = True
                    else:
                        done_reading = True
            
                log(f"loop/end: {self.received_message_bytes}")

        if mask & selectors.EVENT_WRITE:
            if len(self.message_bytes_to_send) > 0:
                log("writing...")
                log(f"len(self.message_bytes_to_send): {len(self.message_bytes_to_send)}")
                bytes_sent = client.send(self.message_bytes_to_send)
                self.message_bytes_to_send = self.message_bytes_to_send[bytes_sent:]
                log(f"bytes_sent: {bytes_sent}, len(self.message_bytes_to_send): {len(self.message_bytes_to_send)}")
            else:
                log("socket is writable but we have nothing to send right now.")

    def on_message_received(self, message: str):
        log(f"received message: {message}")
        reply = "message received successfully from a sync callback called from an asyncio `Task` polling the socket with `select()`!"
        self.add_UTF8_message_to_send_queue(reply)
        self.disconnect_client()

    def disconnect_client(self):
        log("closing client...")
        if self.client_sock is not None or (self.client_sock is Socket and self.client_sock.fileno() != -1):
            self.client_sock.shutdown(SHUT_RDWR)
            self.client_sock.close()

    def close(self):
        log("closing everything...")
        # https://docs.python.org/3/howto/sockets.html#disconnecting
        self.exit_event.set()
        self.selector.unregister(self.server_sock)
        self.server_sock.shutdown(SHUT_RDWR)
        self.server_sock.close()

        if self.select_task is not None:
            self.select_task.cancel()
        # if self.sync_message_sending_task is not None:
        #     self.sync_message_sending_task.cancel()
        if self.other_work_task is not None:
            self.other_work_task.cancel()

        if self.client_sock is not None:
            try:
                self.selector.unregister(self.client_sock)
            except Exception as e:
                print(f"error unregistering client socket (it was probably already unregistered/not registered). Error: {e}")
            self.client_sock.shutdown(SHUT_RDWR)
            self.client_sock.close()

    async def start(self):
        log("starting server and other work task.")
        self.select_task = asyncio.create_task(self.select_loop(), name="EBF5AP socket select loop ")
        self.other_work_task = asyncio.create_task(self.other_work(), name="EBF5AP asyncio test other work")

        await self.exit_event.wait()

    async def select_loop(self):
        while True:
            events = self.selector.select(timeout=0)
            for selector_key, mask in events:
                # It doesn't very clear from this code but this callback comes from the data parameter of
                # self.selector.register(<whatever>, <whatever>, data=callback_function_in_our_case).
                # This is a kinda clever design but it's also confusing until you know that's what it is doing.
                # I don't know why the library was made like this but whatever sure fine I guess.
                # I'll just do it like this with this comment explaining it because it is admittedly convenient.
                callback = selector_key.data
                if callable(callback):
                    callback(selector_key.fileobj, mask)
            
            await asyncio.sleep(0)

    async def other_work(self):
        while True:
            log("other work (+ sleep)... ")
            await asyncio.sleep(3) # ALSO TRY THIS WITH A REALLY LONG SLEEP DURATION


serverTest = TestAsyncCommunicationAndOtherWorkAtTheSameTime("localhost", 4999)
asyncio.run(serverTest.start())