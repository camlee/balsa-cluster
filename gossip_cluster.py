from cluster import Cluster

from random_name import make_name

class GossipCluster(Cluster):
    """
    Cluster implementation that sends messages between nodes to share
    helpful information (i.e. gossip) about the state of the cluster.
    This includes sharing each other's names and avoiding a connection
    to ourselves.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = make_name()

    def handle_gossip(self):
        pass