import trio
import sys
from functools import partial

from cluster import Cluster

secret_cluster_cookie = b"abc123"

async def main():
    cluster = Cluster(cookie=secret_cluster_cookie)
    async with trio.open_nursery() as nusery:
        nusery.start_soon(partial(cluster.listen, port=int(sys.argv[1])))

        async for obj in cluster.messages:
            print(f"Do some work with {obj}")

trio.run(main)