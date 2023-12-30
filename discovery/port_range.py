import

from cluster import Cluster, Node

class PortRangeDiscoveryCluster(Cluster):
    def __init__(self, *args, host="localhost", starting_port, ending_port, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.starting_port = starting_port
        self.ending_port = ending_port

    async def _try_to_discover_node(self, node):
        connect_succeeded = await self.connect_to_node(node)
        if connect_succeeded:
            print(f"Discovered {node} using PortRangeDiscovery")
            self.nodes.add(node)

    async def discover(self):
        """
        Tries connecting to all the ports within the specified range.
        """
        async with trio.open_nursery() as nursery:
            with trio.move_on_after(self.autodiscover_period):

                for port in range(self.starting_port, self.ending_port):
                    node = Node(self.host, port)
                    if node in self.nodes:
                        continue # We've already discovered this one. No need to try connecting again.
                    nursery.start_soon(self._try_to_discover_node, node)