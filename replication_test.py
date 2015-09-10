from collections import defaultdict
import os
import re
import time

from dtest import Tester, debug, PRINT_DEBUG
from tools import no_vnodes, require

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

TRACE_DETERMINE_REPLICAS = re.compile('Determining replicas for mutation')
TRACE_SEND_MESSAGE = re.compile('Sending message to /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
TRACE_RESPOND_MESSAGE = re.compile('Message received from /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
TRACE_COMMIT_LOG = re.compile('Appending to commitlog')
TRACE_FORWARD_WRITE = re.compile('Enqueuing forwarded write to /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')

# Some pre-computed murmur 3 hashes; there are no good python murmur3
# hashing libraries :(
murmur3_hashes = {
    5: -7509452495886106294,
    10: -6715243485458697746,
    16: -5477287129830487822,
    13: -5034495173465742853,
    11: -4156302194539278891,
    1: -4069959284402364209,
    19: -3974532302236993209,
    8: -3799847372828181882,
    2: -3248873570005575792,
    4: -2729420104000364805,
    18: -2695747960476065067,
    15: -1191135763843456182,
    20: 1388667306199997068,
    7: 1634052884888577606,
    6: 2705480034054113608,
    9: 3728482343045213994,
    14: 4279681877540623768,
    17: 5467144456125416399,
    12: 8582886034424406875,
    3: 9010454139840013625
}


@no_vnodes()
class ReplicationTest(Tester):
    """This test suite looks at how data is replicated across a cluster
    and who the coordinator, replicas and forwarders involved are.
    """

    def get_replicas_from_trace(self, trace):
        """Look at trace and return a list of the replicas contacted"""
        coordinator = None
        nodes_sent_write = set([])  # Nodes sent a write request
        nodes_responded_write = set([])  # Nodes that acknowledges a write
        replicas_written = set([])  # Nodes that wrote to their commitlog
        forwarders = set([])  # Nodes that forwarded a write to another node
        nodes_contacted = defaultdict(set)  # node -> list of nodes that were contacted

        for trace_event in trace.events:
            # Step 1, find coordinator node:
            activity = trace_event.description
            source = trace_event.source
            if activity.startswith('Determining replicas for mutation'):
                if not coordinator:
                    coordinator = source
                    break
            if not coordinator:
                continue

        for trace_event in trace.events:
            session = trace._session
            activity = trace_event.description
            source = trace_event.source
            source_elapsed = trace_event.source_elapsed
            thread = trace_event.thread_name

            # Step 2, find all the nodes that each node talked to:
            send_match = TRACE_SEND_MESSAGE.search(activity)
            recv_match = TRACE_RESPOND_MESSAGE.search(activity)
            if send_match:
                node_contacted = send_match.groups()[0]
                if source == coordinator:
                    nodes_sent_write.add(node_contacted)
                nodes_contacted[source].add(node_contacted)
            elif recv_match:
                node_contacted = recv_match.groups()[0]
                if source == coordinator:
                    nodes_responded_write.add(recv_match.groups()[0])

            # Step 3, find nodes that forwarded to other nodes:
            # (Happens in multi-datacenter clusters)
            if source != coordinator:
                forward_match = TRACE_FORWARD_WRITE.search(activity)
                if forward_match:
                    forwarding_node = forward_match.groups()[0]
                    nodes_sent_write.add(forwarding_node)
                    forwarders.add(forwarding_node)

            # Step 4, find nodes who actually wrote data:
            if TRACE_COMMIT_LOG.search(activity):
                replicas_written.add(source)

        return {"coordinator": coordinator,
                "forwarders": forwarders,
                "replicas": replicas_written,
                "nodes_sent_write": nodes_sent_write,
                "nodes_responded_write": nodes_responded_write,
                "nodes_contacted": nodes_contacted
                }

    def get_replicas_for_token(self, token, replication_factor,
                               strategy='SimpleStrategy', nodes=None):
        """Figure out which node(s) should receive data for a given token and
        replication factor"""
        if not nodes:
            nodes = self.cluster.nodelist()
        token_ranges = sorted(zip([n.initial_token for n in nodes], nodes))
        replicas = []

        # Find first replica:
        for i, (r, node) in enumerate(token_ranges):
            if token <= r:
                replicas.append(node.address())
                first_ring_position = i
                break
        else:
            replicas.append(token_ranges[0][1].address())
            first_ring_position = 0

        # Find other replicas:
        if strategy == 'SimpleStrategy':
            for node in nodes[first_ring_position + 1:]:
                replicas.append(node.address())
                if len(replicas) == replication_factor:
                    break
            if len(replicas) != replication_factor:
                # Replication token range looped:
                for node in nodes:
                    replicas.append(node.address())
                    if len(replicas) == replication_factor:
                        break
        elif strategy == 'NetworkTopologyStrategy':
            # NetworkTopologyStrategy can be broken down into multiple
            # SimpleStrategies, just once per datacenter:
            for dc, rf in replication_factor.items():
                dc_nodes = [n for n in nodes if n.data_center == dc]
                replicas.extend(self.get_replicas_for_token(
                    token, rf, nodes=dc_nodes))
        else:
            raise NotImplemented('replication strategy not implemented: %s'
                                 % strategy)

        return replicas

    def pprint_trace(self, trace):
        """Pretty print a trace"""
        if PRINT_DEBUG:
            print("-" * 40)
            for t in trace.events:
                print("%s\t%s\t%s\t%s" % (t.source, t.source_elapsed, t.description, t.thread_name))
            print("-" * 40)

    def simple_test(self):
        """Test the SimpleStrategy on a 3 node cluster"""
        self.cluster.populate(3).start()
        time.sleep(5)
        node1 = self.cluster.nodelist()[0]
        self.conn = self.patient_exclusive_cql_connection(node1)

        # Install a tracing session so we can get info about who the
        # coordinator is contacting:
        session = self.conn
        session.max_trace_wait = 120

        replication_factor = 3
        self.create_ks(session, 'test', replication_factor)
        session.execute('CREATE TABLE test.test (id int PRIMARY KEY, value text)', trace=False)
        # Wait for table creation, otherwise trace times out -
        # CASSANDRA-5658
        time.sleep(5)

        for key, token in murmur3_hashes.items():
            query = SimpleStatement("INSERT INTO test (id, value) VALUES (%s, 'asdf')" % key, consistency_level=ConsistencyLevel.ALL)
            future = session.execute_async(query, trace=True)
            future.result()
            trace = future.get_query_trace(max_wait=120)
            self.pprint_trace(trace)
            stats = self.get_replicas_from_trace(trace)
            replicas_should_be = set(self.get_replicas_for_token(
                token, replication_factor))
            debug('\nreplicas should be: %s' % replicas_should_be)
            debug('replicas were: %s' % stats['replicas'])

            # Make sure the correct nodes are replicas:
            self.assertEqual(stats['replicas'], replicas_should_be)
            # Make sure that each replica node was contacted and
            # acknowledged the write:
            self.assertEqual(stats['nodes_sent_write'], stats['nodes_responded_write'])

    def network_topology_test(self):
        """Test the NetworkTopologyStrategy on a 2DC 3:3 node cluster"""
        self.cluster.populate([3, 3]).start()
        time.sleep(5)
        node1 = self.cluster.nodelist()[0]
        ip_nodes = dict((node.address(), node) for node in self.cluster.nodelist())
        self.conn = self.patient_exclusive_cql_connection(node1)

        # Install a tracing session so we can get info about who the
        # coordinator is contacting:
        session = self.conn

        replication_factor = {'dc1': 2, 'dc2': 2}
        self.create_ks(session, 'test', replication_factor)
        session.execute('CREATE TABLE test.test (id int PRIMARY KEY, value text)', trace=False)
        # Wait for table creation, otherwise trace times out -
        # CASSANDRA-5658
        time.sleep(5)

        forwarders_used = set()

        for key, token in murmur3_hashes.items():
            query = SimpleStatement("INSERT INTO test (id, value) VALUES (%s, 'asdf')" % key, consistency_level=ConsistencyLevel.ALL)
            future = session.execute_async(query, trace=True)
            future.result()
            trace = future.get_query_trace(max_wait=120)
            self.pprint_trace(trace)
            stats = self.get_replicas_from_trace(trace)
            replicas_should_be = set(self.get_replicas_for_token(
                token, replication_factor, strategy='NetworkTopologyStrategy'))
            debug('Current token is %s' % token)
            debug('\nreplicas should be: %s' % replicas_should_be)
            debug('replicas were: %s' % stats['replicas'])

            # Make sure the coordinator only talked to a single node in
            # the second datacenter - CASSANDRA-5632:
            num_in_other_dcs_contacted = 0
            for node_contacted in stats['nodes_contacted'][node1.address()]:
                if ip_nodes[node_contacted].data_center != node1.data_center:
                    num_in_other_dcs_contacted += 1
            self.assertEqual(num_in_other_dcs_contacted, 1)

            # Record the forwarder used for each INSERT:
            forwarders_used = forwarders_used.union(stats['forwarders'])

            try:
                # Make sure the correct nodes are replicas:
                self.assertEqual(stats['replicas'], replicas_should_be)
                # Make sure that each replica node was contacted and
                # acknowledged the write:
                self.assertEqual(stats['nodes_sent_write'], stats['nodes_responded_write'])
            except AssertionError as e:
                debug("Failed on key %s and token %s." % (key, token))
                raise e

        # Given a diverse enough keyset, each node in the second
        # datacenter should get a chance to be a forwarder:
        self.assertEqual(len(forwarders_used), 3)


class Cassandra10238Test(Tester):
    """
    Test to repro CASSANDRA-10238, wherein consolidating racks without a restart could violate RF contract.
    """
    def check_endpoint_count(self, ks, table, nodes, expected_count):
        """
        Check a dummy key (of a stress generated table) expecting it to have expected_count endpoints on each node.
        """
        for node in nodes:
            cmd = "getendpoints {} {} 11".format(ks, table)
            out, err = node.nodetool(cmd)

            if len(err.strip()) > 0:
                debug(err)
                raise RuntimeError("Error running 'nodetool {}'".format(cmd))

            out = out.rstrip()  # remove extra \n from output

            self.assertEqual(len(out.split('\n')), expected_count, "wrong number of endpoints found ({}), should be: {}".format(len(out.split('\n')), expected_count))

    def wait_for_all_nodes_on_rack(self, nodes, rack_name):
        """
        Waits for nodes to collapse to rack_name.
        """
        for i, node in enumerate(nodes):
            wait_expire = time.time() + 120
            while time.time() < wait_expire:
                    out, err = node.nodetool("status")

                    if len(err.strip()) > 0:
                        raise RuntimeError("Error trying to run nodetool status")

                    if len(re.findall(rack_name, out)) == len(nodes):
                        # great, the topology change is propogated
                        debug("Topology change detected on node {}".format(i))
                        break
                    else:
                        debug("Waiting for topology change on node {}".format(i))
                        time.sleep(5)
            else:
                raise RuntimeError("Ran out of time waiting for topology to change on node {}".format(i))

    require('10238')
    def test_rf_collapse(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are collapsed the RF is not impacted.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        # start with separate racks
        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
                topo_file.write("dc=DC1\n")
                topo_file.write("rack=RAC{}".format(i))

        cluster.start()

        conn = self.patient_cql_connection(node1)

        # small stress write to build ks/table for us
        node1.stress(['write', 'n=20', '-rate', 'threads=50', '-schema', 'keyspace=testing', '-pop', 'seq=1..20'])
        conn.execute("""ALTER KEYSPACE testing WITH replication = {'class':'NetworkTopologyStrategy', 'DC1':3}""")

        # make sure endpoint count is correct before continuing with the rest of the test
        self.check_endpoint_count('testing', 'standard1', cluster.nodelist(), 3)

        # consolidate node1/node3 racks to "RAC1"
        for node in [node1, node3]:
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
                topo_file.write("dc=DC1\n")
                topo_file.write("rack=RAC1")

        self.wait_for_all_nodes_on_rack(cluster.nodelist(), "RAC1")

        # nodes have joined racks, check endpoint counts again
        self.check_endpoint_count('testing', 'standard1', cluster.nodelist(), 3)
