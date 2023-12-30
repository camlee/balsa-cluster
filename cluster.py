import trio
import struct
import random
import msgpack


DEFAULT_PORT = 14500


class CommonMessageChannel:
    HEADER_FORMAT = "!Q"
    HEADER_LENGTH = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bytes_stream):
        self.bytes_stream = bytes_stream

    async def aclose(self):
        await self.bytes_stream.aclose()

    def dumps(self, obj):
        return msgpack.dumps(obj)

    def loads(self, obj_bytes):
        return msgpack.loads(obj_bytes)


class SendMessageChannel(CommonMessageChannel, trio.abc.SendChannel):
    async def send(self, obj):
        obj_bytes = self.dumps(obj)
        bytes_length = len(obj_bytes)
        try:
            header = struct.pack(self.HEADER_FORMAT, bytes_length) # Sending a fixed byte header to communicate the object length.
        except struct.error:
            raise ValueError("obj is too large to send")
        await self.bytes_stream.send_all(header + obj_bytes)



class ReceiveMessageChannel(CommonMessageChannel, trio.abc.ReceiveChannel):
    async def receive(self):
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

class MessageChannel(SendMessageChannel, ReceiveMessageChannel, trio.abc.Channel):
    pass


class Node:
    def __init__(self, host, port=DEFAULT_PORT):
        self.host = host
        self.port = port
        self.channel = None # Will be set once connected

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
        except trio.BrokenResourceError:
            await self.channel.aclose()
            self.channel = None
            return False

    async def receive(self):
        if not self.connected():
            raise trio.ClosedResourceError
        try:
            return await self.channel.receive()
        except trio.EndOfChannel:
            await self.channel.aclose()
            self.channel = None
            raise


class UnableToSend(Exception):
    pass


class Cluster:
    """
    Class that handles:
    * Server that listens for new TCP/IP connections on a port.
    * Defines a simple protocol for initial handshake using a secret cookie and then sending/receiving objects over the connection.
    * Client that connects to other servers (presumably other Cluster instances), called nodes here, and speaks the same protocol.
    * Nodes are tracked in self.nodes.
    * Messages can be sent to individual nodes or broadcast to all nodes.
    * Objects received from nodes, either as a client or server are pushed into the self.messages channel.


    Usage:
    cluster = Cluster(nodes=[Node("localhost", 8000), Node("localhost", 8001), ...])
    # Do each of these in a backround task:
    await cluster.listen() # Start the server, runs indefinitely.
    await cluster.connect() # Connect to the other nodes, runs indefinitely to re-connect as needed.
    async for message in cluster.messages:
        # Do something with message.
    """
    def __init__(self, nodes=set(), *, cookie, network_timeout=5):
        """
        Args:
            cookie (bytestring): An identifier for nodes to share with each other as part
                of negotiating their connection to the cluster. All servers must use the same
                cookie to be part of the cluster. Provides some very very minimal amount of security.

            network_timeout: The amount of time in seconds to expect network operations to complete in.
                Set this lower for quicker reaction network partitions, killed servers, etc... Set this
                higher to avoid over-reacting to slow networks or busy servers.
        """
        self.nodes = set(nodes)
        self.clients = set()
        self._messages, self.messages = trio.open_memory_channel(0)
        self.cookie = cookie
        self.network_timeout = network_timeout

        self._listening = False

    async def listen(self, bind="0.0.0.0", port=DEFAULT_PORT):
        if self._listening:
            raise Exception("Already listening.")
        self._listening = True

        print(f"Listening on port {port} for other nodes.")
        await trio.serve_tcp(self._handle_client, port, host=bind)
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

        node = Node(*peer)
        node.channel = MessageChannel(stream)
        self.clients.add(node)

        async for obj in node.channel:
            await self._messages.send(obj)
        self.clients.remove(node)
        print(f"Connection from node {peer} closed.")

    async def _connect_to_node(self, node):
        """
        Try and connect to the node.
            1. No-op if already connected.
            2. Connect to the server.
            3. Send our cookie.
            4. Expect a cookie back.
            5. If all is good, make the message channel wrappping the connection.

        Args:
            node: The node to connect to.
        Returns:
            True, if the connection was successful, otherwise False.
        """

        if node.channel:
            return
        print(f"Connecting to node {node}")
        try:
            stream = await trio.open_tcp_stream(node.host, node.port)
        except OSError:
            print(f"Error: failed to find node {node}")
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

        node.channel = MessageChannel(stream)

        print(f"Successfully connected to node {node}")

        async for obj in node.channel:
            await self._messages.send(obj)
        node.channel = None
        print(f"Connection to node {node} closed.")

        return

    async def discover(self):
        return

    async def connect(self):
        """
        Connect to all the nodes and maintain a connection.
        """
        print("Connecting to other nodes.")
        async with trio.open_nursery() as nursery:
            while True:
                for node in self.nodes:
                    if not node.connected():
                        nursery.start_soon(self._connect_to_node, node)

                jitter = random.uniform(0, self.network_timeout / 2)
                await trio.sleep(self.network_timeout + jitter)

    async def broadcast(self, obj):
        results = set()
        async with trio.open_nursery() as nursery:
            for node in self.nodes:
                nursery.start_soon(self.send_with_tracking, node, obj, results)

        return results

    async def send_with_tracking(self, node, obj, results):
        try:
            await self.send(node, obj)
        except UnableToSend as e:
            print(f"Error: {e}")

            pass # Network partitions happen. If we can't send to the node, that's reality and we just need to communicate it.
        else:
            results.add(node)

    async def send(self, node, obj):
        if node.connected():
            success = await node.send(obj)
            if not success:
                raise UnableToSend("Unable to send: connection to node lost.")
        else:
            raise UnableToSend("Unable to send: not connected to node.")
