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
        """Note: the select task will be created immediately when this function is called"""
        # Supporting IPV6 would probably be nice.
        # But it'll probably just be running on localhost or a local network with IPV4 LAN addresses anyway.
        self.server_sock: Socket = Socket(AF_INET, SOCK_STREAM)
        self.server_sock.bind((host, port))
        self.server_sock.listen(1) # CHECK THIS
        self.server_sock.setblocking(False)

        self.select_task: asyncio.Task | None = None
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.server_sock, selectors.EVENT_READ, data=self.accept_connection_callback)
        
        self.client_sock: None | Socket = None

        self.received_message_fragments = bytearray()
        self.incoming_message_final_length: int = 0
        self.message_bytes_to_send = bytearray()

        if timeout >= 0:
            self.server_sock.settimeout(timeout)

    def start(self): # CONTINUE WITH STUFF HERE!!!!
        self.activate_select_loop()
    
    def activate_select_loop(self):
        self.select_task = asyncio.create_task(self.select_loop(), name="EBF5AP socket select loop")

    def add_UTF8_message_to_send_queue(self, UTF8_message: str):
        length_bytes = len(UTF8_message).to_bytes(4, "big", signed=False)
        message_bytes = UTF8_message.encode("utf-8")
        self.message_bytes_to_send += length_bytes
        self.message_bytes_to_send += message_bytes

    def client_readwrite_callback(self, client: Socket, mask):
        # TODO: handle receiving 0 bytes (ie, a disconnect)
        if mask & selectors.EVENT_READ:
            log("reading...")
            if len(self.received_message_fragments) < 4: # ADD self.isWaitingForNewMessage!!!!!
                new_bytes = client.recv(4 - len(self.received_message_fragments))

                # TODO: probably move into a function, replace with just closing the client socket and keeping the rest of the class alive.
                # also fix in the other copy of this code obviously.
                self.received_message_fragments += new_bytes
                if len(new_bytes) == 0:
                    self.close()
                    raise SocketConnectionBrokenError
                
                
                if len(self.received_message_fragments) >= 4: # could get away with == but >= is safer
                    self.incoming_message_final_length = int.from_bytes(self.received_message_fragments[:4], "big", signed=False)
                    if len(self.received_message_fragments) > 4:
                        log("somehow reached `len(self.received_message_fragments) > 4` while receiving message length.")
            # TODO/TO THINK ABOUT LATER: we could select again here because there were probably more than just 4 (or less) bytes sent.
            else:
                new_bytes = client.recv(self.incoming_message_final_length - len(self.received_message_fragments))
                self.received_message_fragments += new_bytes
                if len(new_bytes) == 0:
                    self.close()
                    raise SocketConnectionBrokenError
                if len(self.received_message_fragments) >= self.incoming_message_final_length: # again == is more correct but >= is safer
                    log("message fully received!")
                    if len(self.received_message_fragments) > self.incoming_message_final_length:
                        log("len(self.received_message_fragments) > self.incoming_message_final_length.")
                    message_text: str = self.received_message_fragments[4:4 + self.incoming_message_final_length].decode("utf-8")
                    self.on_message_received(message_text)

                    #self.received_message_fragments.clear()
                    self.received_message_fragments = self.received_message_fragments[4 + self.incoming_message_final_length:]
                    self.incoming_message_final_length = 0

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
            await asyncio.sleep(0) # ALSO TRY THIS WITH A REALLY LONG SLEEP DURATION

    async def task_function_we_call_a_sync_function_in_that_needs_to_send_messages(self):
        while True:
            self.syncronous_function_we_need_to_send_messages_from()
            await asyncio.sleep(0)

    def close(self):
        # https://docs.python.org/3/howto/sockets.html#disconnecting
        self.selector.unregister(self.server_sock)
        self.server_sock.shutdown(SHUT_RDWR)
        self.server_sock.close()
        if self.select_task != None:
            self.select_task.cancel()
        if self.client_sock != None:
            try:
                self.selector.unregister(self.client_sock)
            except Exception as e:
                print(f"error unregistering client socket (it was probably already unregistered/not registered). Error: {e}")
            self.client_sock.shutdown(SHUT_RDWR)
            self.client_sock.close()

    def accept_connection_callback(self, sock: Socket, mask):
        if self.client_sock != None or (self.client_sock is Socket and self.client_sock.fileno() != -1):
            log("client attempted to connect before the previous client socket connection was broken, ignoring.")
            return
        self.client_sock, address = sock.accept()
        log(f"accepting connection from {address}")
        self.client_sock.setblocking(False)
        self.selector.register(self.client_sock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=self.client_readwrite_callback)

    # with the newest version of AP now merged our _cmd fucntions could be async.
    # so we don't absolutely have to use a sync function. probably easier to just not.
    def syncronous_function_we_need_to_send_messages_from(self):
        # probably have this function get called on loop from within a `Task`

        # uhhhhhhhhhhhhhhhhhhhh ok wait how am I gonna get replies in this function after I send a message to the client?
        # hm...
        # like I probably want some mechanism for being able to easily wait for a client reply and continue from there.
        # maybe like, a queue of coros or something that goes along with a queue of unprocessed fully reconstructed incoming messages?
        # ehhhhhh maybe?
        # actually wait i can just go look at how AP solves this problem.
        # hm.
        # maybe it's just because it's like 11:30 PM but honestly I think it just kinda does stuff indirectly.
        # anyway yeah I'll probably want to figure out some kind of `await send_message_and_wait_for_response()` system for functions like this.
        # IDK I'll have to think it through more in the morning
        pass