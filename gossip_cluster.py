import time

import trio

from cluster import UnableToSend
from dispatch_cluster import DispatchCluster, handler
from random_name import make_name

class Node:
    def __init__(self, name, *, outbound_connection=None, inbound_connection=None):
        self.name = name
        self.outbound_connection = outbound_connection
        self.inbound_connection = inbound_connection
        self.last_heard_from = None

    def __hash__(self):
        return hash(self.name)


class GossipCluster(DispatchCluster):
    """
    Cluster implementation that sends messages between nodes to share
    helpful information (i.e. gossip) about the state of the cluster.
    This includes sharing each other's names and avoiding a connection
    to ourselves.
    """
    def __init__(self, *args, gossip_period=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = make_name()
        print(f"\n==================== {self.name} ====================\n")
        self.gossip_period = gossip_period
        self.nodes = {} # Tracking nodes based purely on gossip messages.
            # This is a higher level abstraction than clients and servers.

    async def gossip_periodically(self):
        while True:
            await self.broadcast({"type": "gossip", "from": self.name})
            await self.sleep_with_jitter(self.gossip_period)

            print("Nodes according to gossip:")
            print(f"{'node':<30} {'inbound':<10} {'outbound':<10} {'last heard from':<10}")
            for node in sorted(self.nodes.values(), key=lambda n: n.name):
                print(
                    f"{node.name:<29}"
                    f"{'*' if node.name == self.name else ' '} "
                    f"{bool(node.inbound_connection and node.inbound_connection.connected()):<10} "
                    f"{bool(node.outbound_connection and node.outbound_connection.connected()):<10} "
                    f"{time.time() - node.last_heard_from:.1f} s ago")

            servers_without_gossip = []
            for server in self.servers:
                if not server.connected():
                    continue
                for node in self.nodes.values():
                    if server == node.outbound_connection:
                        break
                else:
                    servers_without_gossip.append(server)
            if servers_without_gossip:
                print("Servers not sending gossip:")
                for server in servers_without_gossip:
                    print(server)


            clients_without_gossip = []
            for client in self.clients:
                for node in self.nodes.values():
                    if client == node.inbound_connection:
                        break
                else:
                    clients_without_gossip.append(client)
            if clients_without_gossip:
                print("Clients not sending gossip:")
                for client in clients_without_gossip:
                    print(client)


    @handler("gossip")
    async def handle_gossip(self, message):
        node_name = message.obj.get("from")
        if not node_name:
            print(f"Warning: gossip message missing from: {message.obj}")
            return

        node = self.nodes.get(node_name)
        if not node:
            node = Node(node_name)
        print(f"Got gossip message from {message.obj.get('from')} as {message.connection.direction} from {message.connection}")
        if message.connection.direction == "inbound":
            node.inbound_connection = message.connection
        elif message.connection.direction == "outbound":
            node.outbound_connection = message.connection
        else:
            print(f"Warning: unexepected connection direction of {message.connection.direction}")
        node.last_heard_from = time.time()
        self.nodes[node_name] = node

    async def gossip_about_server_changes(self):
        async for server in self.server_changes:
            try:
                await self.send(server, {"type": "gossip", "from": self.name, "debug": "I just connected to you"})
            except UnableToSend:
                pass

    async def gossip_about_client_changes(self):
        async for client in self.client_changes:
            try:
                await self.send(client, {"type": "gossip", "from": self.name, "debug": "I just received a connection from you"})
            except UnableToSend:
                pass

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(super().run)
            nursery.start_soon(self.gossip_periodically)
            nursery.start_soon(self.gossip_about_client_changes)
            nursery.start_soon(self.gossip_about_server_changes)