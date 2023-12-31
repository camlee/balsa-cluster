import struct
import random
import itertools

import trio
import msgpack

from base_cluster import BaseCluster


DEFAULT_PORT = 14500


class MessageChannel(trio.abc.Channel):
    HEADER_FORMAT = "!Q"
    HEADER_LENGTH = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bytes_stream):
        self.bytes_stream = bytes_stream
        self.send_lock = trio.Lock()
        self.receive_lock = trio.Lock()

    async def aclose(self):
        await self.bytes_stream.aclose()

    def dumps(self, obj):
        return msgpack.dumps(obj)

    def loads(self, obj_bytes):
        return msgpack.loads(obj_bytes)

    async def send(self, obj):
        obj_bytes = self.dumps(obj)
        bytes_length = len(obj_bytes)
        try:
            header = struct.pack(self.HEADER_FORMAT, bytes_length) # Sending a fixed byte header to communicate the object length.
        except struct.error:
            raise ValueError("obj is too large to send")
        async with self.send_lock:
            await self.bytes_stream.send_all(header + obj_bytes)

    async def receive(self):
        async with self.receive_lock:
            header = await self._receive_all_the_bytes(self.HEADER_LENGTH) # First few bytes are the header which denote the object length.
            (bytes_length,) = struct.unpack(self.HEADER_FORMAT, header)
            obj_bytes = await self._receive_all_the_bytes(bytes_length)
        obj = self.loads(obj_bytes)
        return obj

    async def _receive_all_the_bytes(self, bytes_length):
        buffer = bytearray()
        while True:
            remaining_bytes = bytes_length - len(buffer)
            if remaining_bytes == 0:
                break
            data = await self.bytes_stream.receive_some(remaining_bytes)
            if not data: # The stream has been closed
                raise trio.EndOfChannel
            buffer += data
        return buffer


class Connection:
    def __init__(self, host, port=DEFAULT_PORT, direction=None):
        self.host = host
        self.port = port
        self.channel = None # Will be set once connected
        self.direction = direction

    def __str__(self):
        return str((self.host, self.port))

    def __hash__(self):
        return hash((self.host, self.port))

    def connected(self):
        return bool(self.channel)

    async def send(self, obj):
        if not self.connected():
            raise trio.ClosedResourceError
        try:
            await self.channel.send(obj)
            return True
        except (trio.EndOfChannel, trio.BrokenResourceError):
            await self.channel.aclose()
            self.channel = None
            return False

    async def receive(self):
        if not self.connected():
            raise trio.ClosedResourceError
        try:
            return await self.channel.receive()
        except (trio.EndOfChannel, trio.BrokenResourceError):
            await self.channel.aclose()
            self.channel = None
            raise

    async def __aiter__(self):
        try:
            while True:
                yield await self.receive()
        except (trio.EndOfChannel, trio.BrokenResourceError):
            return

class Message:
    """
    Simple container to pass around objects along with the connection that
    they came from.
    """
    def __init__(self, connection, obj):
        self.connection = connection
        self.obj = obj

    def __str__(self):
        return f"{self.obj} from {self.connection}"

class UnableToSend(Exception):
    pass


class Cluster(BaseCluster):
    """
    Class that handles:
    * Server that listens for new TCP/IP connections on a port.
    * Defines a simple protocol for initial handshake using a secret cookie and then sending/receiving objects over the connection.
    * Client that connects to other servers (presumably other programs using the same Cluster class as we are), and speaks the same protocol.
    * Servers are tracked in self.servers.
    * Objects can be sent to individual servers or broadcast to all servers.
    * Objects received from connections, either as a client or server are wrapped in a Message and pushed into the self.messages channel.


    Basic Usage:
    cluster = Cluster(
        bind=("localhost", 9000), # for us to act as a server and receive connections from clients.
        servers=[Connection("localhost", 9000), Connection("localhost", 9001), ...]) # For us to act as a client and connect to these servers.
    await cluster.run() # Run this as a background task
    async for message in cluster.messages: # Will loop over messages forever
        print(message) # Do something with the message.
    await cluster.broadcast("Hello cluster!") # Will send this string object to all connected servers and clients.
    """
    def __init__(self, *, bind=("0.0.0.0", DEFAULT_PORT), servers=set(), cookie, network_timeout=5):
        """
        Args:
            cookie (bytestring): An identifier for clients to share with servers as part
                of negotiating their connection. All nodes must use the same
                cookie to be part of the cluster. Provides some very very minimal amount of security.

            network_timeout: The amount of time in seconds to expect network operations to complete in.
                Set this lower for quicker reaction network partitions, killed servers, etc... Set this
                higher to avoid over-reacting to slow networks or busy servers.
        """
        self.bind = bind
        self.servers = set(servers)
        self.clients = set()
        self._messages, self.messages = trio.open_memory_channel(0)
        self._server_changes, self.server_changes = trio.open_memory_channel(5)
        self._client_changes, self.client_changes = trio.open_memory_channel(5)

        self.cookie = cookie
        self.network_timeout = network_timeout

        self._listening = False

    def _notify_server_change(self, server):
        try:
            self._server_changes.send_nowait(server)
        except trio.WouldBlock:
            self.server_changes.receive_nowait()
            self._server_changes.send_nowait(server)

    def _notify_client_change(self, client):
        try:
            self._client_changes.send_nowait(client)
        except trio.WouldBlock:
            self.client_changes.receive_nowait()
            self._client_changes.send_nowait(client)

    async def sleep_with_jitter(self, duration):
        """
        Sleep function that introduces some random jitter to avoid negative
        impacts from multiple servers doing things completely in sync.
        Also might help avoid code that doesn't account for network latency from working
        only in predictable network conditions.

        """
        await trio.sleep(random.uniform(duration-duration/4, duration+duration/4))

    async def listen(self):
        if self._listening:
            raise Exception("Already listening.")
        self._listening = True

        if self.bind is None:
            return

        print(f"Listening on {':'.join(str(x) for x in self.bind)} for clients.")
        await trio.serve_tcp(self._handle_client, host=self.bind[0], port=self.bind[1])
        self._listening = False

    async def _handle_client(self, stream):
        peer = stream.socket.getpeername()
        print(f"New connection attempt from {peer}.")
        try:
            with trio.fail_after(self.network_timeout): # Client must give us a cookie within a reasonable amount of time
                provided_cookie = await stream.receive_some(len(self.cookie))
        except trio.TooSlowError:
            print(f"Error: Client didn't send us a complete cookie in time ({self.network_timeout} s).")
            await stream.aclose()
            return
        if not provided_cookie:
            print("Error: Client hung up before sending their cookie.")
            return
        if provided_cookie != self.cookie:
            print(f"Error: incorrect cookie provided by client. Closing connection. (theirs: {provided_cookie}, ours: {self.cookie})")
            await stream.aclose()
            return
        await stream.send_all(bytes(reversed(self.cookie))) # Sending our cookie back for the client to know we've accepted.

        print(f"Successfull connection from {peer}.")

        client = Connection(*peer, direction="inbound")
        client.channel = MessageChannel(stream)
        self.clients.add(client)
        self._notify_client_change(client)

        try:
            async for obj in client:
                await self._messages.send(Message(client, obj))
        except trio.BrokenResourceError:
            print(f"Connection from client {peer} closed unexpectedly.")
        else:
            print(f"Connection from client {peer} closed.")
        finally:
            self.clients.remove(client)
        self._notify_client_change(client)

    async def _connect_to_server(self, server):
        """
        Try and connect to the server.
            1. No-op if already connected.
            2. Connect to the server.
            3. Send our cookie.
            4. Expect a cookie back.
            5. If all is good, make the message channel wrappping the connection.

        Args:
            server: The server to connect to.
        Returns:
            True, if the connection was successful, otherwise False.
        """

        if server.channel:
            return

        print(f"Connecting to server {server}")
        server.direction = "outbound"
        try:
            stream = await trio.open_tcp_stream(server.host, server.port)
        except OSError:
            print(f"Error: failed to find server {server}")
            return
        await stream.send_all(self.cookie)
        try:
            with trio.fail_after(self.network_timeout): # Server must give us a cookie within a reasonable amount of time
                provided_cookie_response = await stream.receive_some(len(self.cookie))
        except trio.TooSlowError:
            print("Error: Server didn't send us back a cookie in time. Is our cookie correct?")
            await stream.aclose()
            return
        expected_cookie_response = bytes(reversed(self.cookie))
        if not expected_cookie_response:
            print("Error: Server closed the connection on us. Is our cookie correct?")
        if provided_cookie_response != expected_cookie_response:
            print(f"Error: incorrect cookie response provided by server. Closing connection. (theirs: {provided_cookie_response}, ours: {expected_cookie_response})")
            await stream.aclose()
            return

        server.channel = MessageChannel(stream)

        print(f"Successfully connected to server {server}")
        self._notify_server_change(server)

        try:
            async for obj in server:
                await self._messages.send(Message(server, obj))
        except trio.BrokenResourceError:
            print(f"Connection to server {server} closed unexpectedly.")
        else:
            print(f"Connection to server {server} closed.")
        self._notify_server_change(server)

    async def connect(self):
        """
        Connect to all the servers and maintain a connection.
        """
        print("Connecting to other servers.")
        async with trio.open_nursery() as nursery:
            while True:
                for server in self.servers:
                    if not server.connected():
                        nursery.start_soon(self._connect_to_server, server)

                await self.sleep_with_jitter(self.network_timeout)

    async def broadcast(self, obj):
        results = set()
        async with trio.open_nursery() as nursery:
            for connection in itertools.chain(self.servers, self.clients):
                nursery.start_soon(self._send_with_tracking, connection, obj, results)

        return results

    async def _send_with_tracking(self, connection, obj, results):
        try:
            await self.send(connection, obj)
        except UnableToSend as e:
            print(f"Error: {e}")

            pass # Network partitions happen. If we can't send, that's reality and we just need to communicate it.
        else:
            results.add(connection)

    async def send(self, connection, obj):
        if connection.connected():
            success = await connection.send(obj)
            if not success:
                raise UnableToSend("Unable to send: connection lost.")
        else:
            raise UnableToSend("Unable to send: not connected.")

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.listen)
            nursery.start_soon(self.connect)
