import sys
import trio
import random
from functools import partial

from cluster import Node, Cluster
from gossip_cluster import GossipCluster


secret_cluster_cookie = b"abc123"

async def main():
    cluster = Cluster(nodes=[Node("localhost", 90001), Node("localhost", 90002), Node("localhost", 90003), Node("localhost", 90004)], cookie=secret_cluster_cookie)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(partial(cluster.listen, port=int(sys.argv[1])))

        async def do_work():
            async for obj in cluster.messages:
                print(f"Do some work with {obj}")

        nursery.start_soon(do_work)



        nursery.start_soon(cluster.connect)
        while True:
            sleep_time = random.randint(10, 30)
            await trio.sleep(sleep_time)
            nodes_received = await cluster.broadcast({"eventType": "hello", "sleep_time": sleep_time})
            print(f"Broadcast to {len(nodes_received)}/{len(cluster.nodes)} nodes.")
            # if not nodes_received:
            #     print("Warning: didn't broadcast to any nodes.")
            # elif nodes_received != cluster.nodes:
            #     print("Warning: didn't broadcast to all nodes.")

trio.run(main)