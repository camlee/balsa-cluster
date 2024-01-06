import time
import random

import trio

from cluster import UnableToSend
from dispatch_cluster import DispatchCluster, handler
from random_name import make_name

class Node:
    def __init__(self, name, *, outbound_connection=None, inbound_connection=None, active_timeout=12.5):
        self.name = name
        self.outbound_connection = outbound_connection
        self.inbound_connection = inbound_connection
        self.active_timeout = active_timeout
        self.last_heard_from = None # Monotonic time
        self.last_heard_from_at = None # Wall clock time

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def active(self):
        return (
            self.last_heard_from is not None
            and
            time.monotonic() - self.last_heard_from < self.active_timeout
            and
            (self.outbound_connection or self.inbound_connection)
            )


class GossipCluster(DispatchCluster):
    """
    Cluster implementation that sends messages between nodes to share
    helpful information (i.e. gossip) about the state of the cluster.
    This includes sharing each other's names and identifying when we connect
    to ourselves. This class also attempts to form  a consensus, by electing
    a leader using an implementation of the Raft algorithm: https://raft.github.io/raft.pdf
    """
    def __init__(self, *args, name=None, gossip_period=5,
            heartbeat_period=1, election_timeout_min=2.1, election_timeout_max=3.1,
            formation_min_nodes=3, formation_wait_for=0,
            **kwargs):
        """
        Args:
            gossip_period: How often to broadcast a hello message to the entire cluster, in seconds.
                Mostly serves as a way of verifying that the network connection is still working.

            heartbeat_period: How often the header updates the followers, in seconds. Should be set
                larger than the network latency (preferably much larger). The advantage of setting
                it smaller is to reduce the time it takes the cluster to recover from a failure
                and elect a new leader.

            election_timeout_min: Used for randomized leader timeouts. This is the minimum time a follower
                will wait to hear from the leader before assuming it's dead and trying to pick a new leader.
                Must be larger than heartbeat_period (preferably much larger) to avoid the leader switching
                often due to normal network latency.

            election_timeout_max: USed for randomized leader timeouts. This is the maximum time a follower
                will wait to hear from the leader before assuming it's dead and trying to pick a new leader.
                Must be larger than leadertimeout_min. Shouldn't be too close to election_timeout_min or multiple
                followers might try to pick a new leader at the same time, causing split votes and increasing
                how long it takes to pick a leader.

            formation_min_nodes: The minimum number of nodes required to form an initial consensus.
                After a leader is elected, a majority of participating nodes are required for consensus,
                including to change (add or remove) nodes from those participating in consensus.
                This setting allows for bootstraping, i.e. formation. It should be set based on your
                target cluster size and it's important that you don't provision too many more or less
                nodes.
                Typically, set this larger than half the target cluster size and equal to or lower
                than the cluster size.
                Setting this larger than the cluster size will result in no leader being elected and no consensus formed.
                Setting this equal to or lower than half the cluster size may result in split-brain: multiple nodes
                being elected as leaders and operating seperately.
                Ex. for a target cluster size of 3, probably set this to either 2 or 3.
                For a cluster size of 5, probably set this to either 3, 4, or 5.

            formation_wait_for: How long (in seconds) to wait after initial startup before trying to form an
                initial cluster. This gives nodes time to start up and talk to each other before the cluster is formed.
                Works in addition to formation_min_nodes. They can be used individually or together.
        """
        super().__init__(*args, **kwargs)
        # Attributes that don't change:
        if name is None:
            self.name = make_name()
        else:
            self.name = name
        print(f"\n==================== {self.name} ====================\n")
        self.gossip_period = gossip_period
        self.heartbeat_period = heartbeat_period
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max

        self.formation_min_nodes = formation_min_nodes
        self.formation_wait_for = formation_wait_for

        self.started_at = time.monotonic()

        # Attributes related to basic node discovery:
        self._nodes = {} # Tracking nodes based purely on gossip messages.
                         # This is a higher level abstraction than clients and servers.
        self.us = None # The node that we've identified is us.

        # Attributes related to consensus:
        self.cluster = set() # The nodes considered part of our cluster for consensus
        self.leader = None # The node that is the leader of the cluster
        self.election_timer_start = None # The time at which we last heard from the leader
        self.election_timeout = None # Duration in seconds, re-computed randomly after each heartbeat.
        self.role = "observer" # Our role
        self.term = 0
        self.vote = None
        self.votes_for = set()
        self.votes_against = set()


    def get_node(self, name):
        node = self._nodes.get(name)
        if not node:
            node = Node(name, active_timeout=self.gossip_period*2.5) # allows for one missed gossip message before considering inactive
            self._nodes[name] = node
        return node

    def get_active_nodes(self):
        nodes = self._nodes.values()
        active_nodes = [node for node in nodes if node.active()]
        sorted_active_nodes = sorted(active_nodes, key=lambda n: n.name)
        return sorted_active_nodes

    async def send_gossip(self, message):
        await self.broadcast({
            "type": "gossip",
            "from": self.name,
            "term": self.term,
            **message,
            })

    async def send_gossip_to(self, connection, message):
        await self.send(connection, {
            "type": "gossip",
            "from": self.name,
            "term": self.term,
            **message,
            })

    async def send_heartbeat(self):
        await self.send_gossip({"what": "status", "leader": self.name, "cluster": [node.name for node in self.cluster]})

    async def gossip_occasionally(self):
        while True:
            await self.send_gossip({"what": "hello", "debug": "Just checking in."})
            await trio.sleep(self.gossip_period)

    async def consensus(self):
        while True:
            if self.role == "observer":
                # Check to see if a cluster needs to be formed:
                if self.leader is None:
                    active_nodes = len(self.get_active_nodes())
                    waited_for = time.monotonic() - self.started_at
                    if waited_for > self.formation_wait_for and active_nodes >= self.formation_min_nodes:
                        print(f"{self.term}: Starting an election as an observer")
                        await self.start_election()
                    else:
                        print(f"{self.term}: Waiting for initial formation. "
                            f"{active_nodes}/{self.formation_min_nodes} nodes. "
                            f"Waited {waited_for:.1f}s of {self.formation_wait_for:.1f}s.")
                else:
                    print(f"{self.term}: Obverving that {self.leader} is the leader of a cluster sized {len(self.cluster)} ({', '.join(str(n) for n in self.cluster)}).")
                await trio.sleep(self.heartbeat_period) # Not doing heartbeat here, but this is a good small time to use.

            elif self.role == "follower":
                # Check to see if the leader has failed to check in:
                last_heard_from_leader_ago = time.monotonic() - self.election_timer_start
                if last_heard_from_leader_ago > self.election_timeout:
                    print(f"{self.term}: Starting an election as a follower. {last_heard_from_leader_ago:.1f}s of {self.election_timeout:.1f}s timeout elapsed")
                    await self.start_election()
                else:
                    # Sleep until the timeout has passed so we can check to see if the leader has checked in by then:
                    print(f"{self.term}: Following {self.leader}. {last_heard_from_leader_ago:.1f}s of {self.election_timeout:.1f}s timeout elapsed")
                    await trio.sleep(self.election_timeout - last_heard_from_leader_ago)

            elif self.role == "leader":
                # Check in with followers to maintain our leadership:
                print(f"{self.term}: We're the leader.")
                await self.send_heartbeat()
                await trio.sleep(self.heartbeat_period)

            elif self.role == "candidate":
                # Waiting on votes
                election_started_ago = time.monotonic() - self.election_timer_start
                if election_started_ago > self.election_timeout:
                    # We weren't elected leader within the timeout, reverting to follower.
                    print(f"{self.term}: We're a candidate that didn't get enough votes in time.")
                    if self.leader:
                        self.role = "follower"
                    else:
                        self.role = "observer"
                else:
                    print(f"{self.term}: We're a candidate just waiting on votes.")
                    await trio.sleep(self.heartbeat_period) # We could become a leader in which case we need to start sending out heartbeats within this time.

            else:
                print(f"Unexpected role: {self.role}")
                raise Exception(f"Unexpected role: {self.role}")

    def set_election_timer(self):
        self.election_timer_start = time.monotonic()
        self.election_timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)

    async def start_election(self):
        self.role = "candidate"
        self.term += 1
        self.vote = None # We'll vote for ourself when we receive the broadcast just like any other node.
        self.votes_for = set()
        self.votes_against = set()
        print(f"{self.term}: Starting an election for term {self.term}.")
        await self.send_gossip({"what": "nomination", "nominee": self.name})
        self.set_election_timer()
        print(f"{self.term}: Just waiting on votes now.")

    @handler("gossip")
    async def handle_gossip(self, message):
        node_name = message.obj.get("from")
        if not node_name:
            print(f"Warning: gossip message missing from: {message.obj}")
            return

        node = self.get_node(node_name)
        # print(f"Got gossip message from {message.obj.get('from')} as {message.connection.direction} from {message.connection}")
        if message.connection.direction == "inbound":
            node.inbound_connection = message.connection
        elif message.connection.direction == "outbound":
            node.outbound_connection = message.connection
        else:
            print(f"Warning: unexepected connection direction of {message.connection.direction}")
        node.last_heard_from_at = time.time()
        node.last_heard_from = time.monotonic()
        if node.name == self.name:
            self.us = node

        if message.obj.get("what") == "status" and message.obj["term"] >= self.term:
            # print(f"{self.term}: We've just been informed that the leader is {message.obj['leader']} for term {message.obj['term']}")
            self.leader = self.get_node(message.obj["leader"])
            self.term = message.obj["term"]
            self.cluster = set(self.get_node(node_name) for node_name in message.obj["cluster"])
            self.set_election_timer()
            if self.role in ("candidate", "leader") and self.leader != self.us: # Stepping down
                self.role = "follower"
            if self.role == "observer" and self.name in message.obj["cluster"]: # The leader considers us in the cluster so convert to follower
                self.role = "follower"

        if message.obj.get("what") == "nomination" and self.role != "observer":
            print(f"{self.term}: Received a nomination request from {node} for term {message.obj['term']}.")
            new_term = message.obj["term"]
            if new_term > self.term:
                print(f"{self.term} It's a new term!!! Switching to {message.obj['term']}")
                self.term = new_term
                self.role = "follower"
                self.vote = None # Only time we clear our vote is when there's a new term.
            if self.vote is None: # We never change a vote, only set it if not set.
                self.vote = message.obj["nominee"]
                print(f"{self.term}: We're voting for {self.vote}")

            await self.send_gossip_to(message.connection, {"what": "vote", "vote": self.vote})

        if message.obj.get("what") == "vote" and self.role == "candidate":
            if message.obj["term"] == self.term:
                if message.obj["vote"] == self.name:
                    print(f"{self.term}: We've received a vote from {node}")
                    self.votes_for.add(node)
                else:
                    print(f"{self.term}: We've lost a vote for term {self.term} ({node})")
                    self.votes_against.add(node)

            if not self.leader and len(self.votes_for) >= self.formation_min_nodes: # Initial formation.
                print(f"{self.term}: Forming initial cluster with {len(self.votes_for)} votes for ({len(self.votes_against)} against). Converting to leader.")
                self.role = "leader"
                self.leader = self.us
                self.cluster = self.votes_for.union(self.votes_against)
                print(f"{self.term}: Initial cluster formed with {len(self.cluster)} members.")
                await self.send_heartbeat()

            elif self.leader and len(self.votes_for) > len(self.cluster) / 2: # Simple majority.
                print(f"{self.term}: We have received {len(self.votes_for)} votes for of {len(self.cluster)} possible votes ({len(self.votes_against)} against). Converting to leader")
                self.role = "leader"
                self.leader = self.us
                await self.send_heartbeat()


    async def gossip_about_server_changes(self):
        async for server in self.server_changes:
            try:
                await self.send_gossip_to(server, {"what": "hello", "debug": "I just connected to you"})
            except UnableToSend:
                pass

    async def gossip_about_client_changes(self):
        async for client in self.client_changes:
            try:
                await self.send_gossip_to(client, {"what": "hello", "debug": "I just received a connection from you"})
            except UnableToSend:
                pass

    async def run(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(super().run)
            nursery.start_soon(self.gossip_occasionally)
            nursery.start_soon(self.consensus)
            nursery.start_soon(self.gossip_about_client_changes)
            nursery.start_soon(self.gossip_about_server_changes)

    def _print_cluster_status(self):
        print("Nodes according to gossip:")
        print(f"{'node':<35} {'inbound':<10} {'outbound':<10} {'last heard from':<10}")
        for node in self.get_active_nodes():
            node_name = node.name
            if node is self.us:
                node_name += " (us)"
            if node is self.leader:
                node_name += " *"
            print(
                f"{node_name:<35} "
                f"{bool(node.inbound_connection and node.inbound_connection.connected()):<10} "
                f"{bool(node.outbound_connection and node.outbound_connection.connected()):<10} "
                f"{time.monotonic() - node.last_heard_from:.1f} s ago")

    def _print_connections_without_gossip(self):
        servers_without_gossip = []
        for server in self.servers:
            if not server.connected():
                continue
            for node in self._nodes.values():
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
            for node in self._nodes.values():
                if client == node.inbound_connection:
                    break
            else:
                clients_without_gossip.append(client)
        if clients_without_gossip:
            print("Clients not sending gossip:")
            for client in clients_without_gossip:
                print(client)