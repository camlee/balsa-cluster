import trio
import sys
from functools import partial

from cluster import Cluster

secret_cluster_cookie = b"abc123"

async def main():
    cluster = Cluster(bind=("0.0.0.0", int(sys.argv[1])), cookie=secret_cluster_cookie)
    async with trio.open_nursery() as nusery:
        nusery.start_soon(cluster.run)

        async for message in cluster.messages:
            print(message)

trio.run(main)