import sys
import trio
import random

from cluster import Connection, Cluster
from gossip_cluster import GossipCluster
from dispatch_cluster import handler



secret_cluster_cookie = b"abc123"

async def main():
    cluster = GossipCluster(
        bind=("0.0.0.0", int(sys.argv[1])), cookie=secret_cluster_cookie,
        servers=[
            Connection("localhost", 90001),
            Connection("localhost", 90002),
            Connection("localhost", 90003),
            Connection("localhost", 90004),
            ])

    async with trio.open_nursery() as nursery:
        nursery.start_soon(cluster.run)

        while True:
            sleep_time = random.randint(10, 30)
            await trio.sleep(sleep_time)
            received = await cluster.broadcast({"type": "hello", "sleep_time": sleep_time})
            print(f"Broadcast to {len(received)}/{len(cluster.servers) + len(cluster.clients)}.")
            # if not nodes_received:
            #     print("Warning: didn't broadcast to any nodes.")
            # elif nodes_received != cluster.nodes:
            #     print("Warning: didn't broadcast to all nodes.")
    @handler("hello")
    async def handle_hello(self):
        pass

trio.run(main)