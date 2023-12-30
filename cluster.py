import trio
import struct
import json

DEFAULT_PORT = 14500


class CommonMessageChannel:
    HEADER_FORMAT = "!Q"
    HEADER_LENGTH = struct.calcsize(HEADER_FORMAT)

    def __init__(self, bytes_stream):
        self.bytes_stream = bytes_stream

    async def aclose(self):
        await self.bytes_stream.aclose()

    def dumps(self, obj):
        return json.dumps(obj).encode("utf-8")

    def loads(self, obj_bytes):
        return json.loads(obj_bytes.decode("utf-8"))


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
        header = await self._receive_some_bytes(self.HEADER_LENGTH) # First few bytes are the header which denote the object length.
        (bytes_length,) = struct.unpack(self.HEADER_FORMAT, header)
        obj_bytes = await self._receive_some_bytes(bytes_length)
        obj = self.loads(obj_bytes)
        return obj

    async def _receive_some_bytes(self, bytes_length):
        data = await self.bytes_stream.receive_some(bytes_length)
        if not data: # The stream has been closed
            raise trio.EndOfChannel
        return data


class Node:
    def __init__(self, host, port=DEFAULT_PORT):
        self.host = host
        self.port = port
        self.channel = None
        self.lock = trio.Lock()

    def __str__(self):
        return str((self.host, self.port))

    def connected(self):
        return bool(self.channel)

    async def send(self, obj):
        try:
            await self.channel.send(obj)
            return True
        except trio.BrokenResourceError:
            await self.channel.aclose()
            self.channel = None
            return False


class UnableToSend(Exception):
    pass


class Cluster:
    def __init__(self, initial_nodes=set(), *, cookie, network_timeout=5, autodiscover_period=60):
        """
        Args:
            cookie (bytestring): An identifier for nodes to share with each other as part
                of negotiating their connection to the cluster. All servers must use the same
                cookie to be part of the cluster. Provides some very very minimal amount of security.

            network_timeout: The amount of time to expect typical network operations to complete in.

            autodiscover_period: Approximately how often (in seconds) to re-discover and connect to nodes
                in the cluster.
        """
        self.nodes = set(initial_nodes)
        self._messages, self.messages = trio.open_memory_channel(0)
        self.cookie = cookie
        self.network_timeout = network_timeout
        self.autodiscover_period = autodiscover_period

        self._listening = False

    async def listen(self, bind="0.0.0.0", port=DEFAULT_PORT):
        if self._listening:
            raise Exception("Already listening.")
        self._listening = True

        print(f"Listening on port {port}")
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

        async for obj in ReceiveMessageChannel(stream):
            await self._messages.send(obj)
        print("Connection closed!")

    async def connect_to_node(self, node):
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

        async with node.lock:
            if node.channel:
                return True
            print(f"Connecting to node {node}")
            try:
                node.stream = await trio.open_tcp_stream(node.host, node.port)
            except OSError:
                print(f"Error: failed to find node {node}")
                return False
            await node.stream.send_all(self.cookie)
            try:
                with trio.fail_after(self.network_timeout): # Server must give us a cookie within a reasonable amount of time
                    provided_cookie_response = await node.stream.receive_some(len(self.cookie))
            except trio.TooSlowError:
                print("Error: Server didn't send us back a cookie in time. Is our cookie correct?")
                await node.stream.aclose()
                node.stream = None
                return False
            expected_cookie_response = bytes(reversed(self.cookie))
            if not expected_cookie_response:
                print("Error: Server closed the connection on us. Is our cookie correct?")
            if provided_cookie_response != expected_cookie_response:
                print(f"Error: incorrect cookie response provided by server. Closing connection. (theirs: {provided_cookie_response}, ours: {expected_cookie_response})")
                await node.stream.aclose()
                node.stream = None
                return False

            node.channel = SendMessageChannel(node.stream)

            print(f"Successfully connected to node {node}")
            return True

    async def discover(self):
        return

    async def connect(self):
        """
        Connect to all the nodes, maintain a connection, and periodically re-discover to find new nodes.
        """
        print("Connecting to cluster.")
        while True:
            await self.discover()
            async with trio.open_nursery() as nursery:
                with trio.move_on_after(self.autodiscover_period):
                    for node in self.nodes:
                        if not node.connected():
                            nursery.start_soon(self.connect_to_node, node)
            await trio.sleep(self.autodiscover_period)
            print("Re-discovering cluster.")

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
        connected_now = await self.connect_to_node(node) # Try and connect if we're not already
        if connected_now:
            success = await node.send(obj)
            if not success:
                raise UnableToSend("Unable to send: connection to node lost.")
        else:
            raise UnableToSend("Unable to send: not connected to node.")

