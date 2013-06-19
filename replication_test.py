from dtest import Tester, debug, TracingCursor, PRINT_DEBUG
from tools import putget
from ccmlib.cluster import Cluster
import re
import os
import time

TRACE_DETERMINE_REPLICAS = re.compile('Determining replicas for mutation')
TRACE_SEND_MESSAGE = re.compile('Sending message to /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
TRACE_RESPOND_MESSAGE = re.compile('Message received from /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
TRACE_COMMIT_LOG = re.compile('Appending to commitlog')

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
    20:  1388667306199997068,
    7:  1634052884888577606,
    6:  2705480034054113608,
    9:  3728482343045213994,
    14:  4279681877540623768,
    17:  5467144456125416399,
    12:  8582886034424406875,
    3:  9010454139840013625
}


class ReplicationTest(Tester):
    """This test suite looks at how data is replicated across a cluster
    and who the coordinator, replicas and forwarders involved are.
    """
    def __init__(self, *args, **kwargs):
        # Forcing cluster version on purpose
        os.environ['CASSANDRA_VERSION'] = '1.2.4'
        Tester.__init__(self, *args, **kwargs)

    def get_replicas_from_trace(self, trace):
        """Look at trace and return a list of the replicas contacted"""
        coordinator = None
        nodes_sent = set([])
        nodes_responded = set([])
        replicas_written = set([])
        for session, event, activity, source, source_elapsed, thread in trace:
            # Step 1, find coordinator node:
            if activity.startswith('Determining replicas for mutation'):
                if not coordinator:
                    coordinator = source
            if not coordinator:
                continue

            # Step 2, find nodes who were contacted/responded:
            if source == coordinator:
                send_match = TRACE_SEND_MESSAGE.search(activity)
                recv_match = TRACE_RESPOND_MESSAGE.search(activity)
                if send_match:
                    nodes_sent.add(send_match.groups()[0])
                elif recv_match:
                    nodes_responded.add(recv_match.groups()[0])

            # Step 3, find nodes who actually wrote data:
            if TRACE_COMMIT_LOG.search(activity):
                replicas_written.add(source)

        # Error if not all the replicas responded:
        if nodes_sent != nodes_responded:
            raise AssertionError('Not all replicas responded.')

        return {"coordinator": coordinator,
                "replicas": replicas_written,
                "nodes_sent": nodes_sent,
                "nodes_responded": nodes_responded
        }

    def get_replicas_for_token(self, token, replication_factor,
                               strategy='SimpleStrategy'):
        """Figure out which node(s) should receive data for a given token and
        replication factor"""
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
        debug('first_ring_position: %s' % first_ring_position)
        # Find other replicas:
        if strategy == 'SimpleStrategy':
            for node in nodes[first_ring_position+1:]:
                replicas.append(node.address())
                if len(replicas) == replication_factor:
                    break
            if len(replicas) != replication_factor:
                # Replication token range looped:
                for node in nodes:
                    replicas.append(node.address())
                    if len(replicas) == replication_factor:
                        break
        else:
            raise NotImplemented('replication strategy not implemented: %s' 
                                 % strategy)
        debug("replicas: %s" % replicas)
        return replicas
    
    def pprint_trace(self, trace):
        """Pretty print a trace"""
        if PRINT_DEBUG:
            print("-" * 40)
            for t in trace:
                print("%s\t%s\t%s\t%s" % (t[3], t[4], t[2], t[5]))
            print("-" * 40)
    
    def simple_test(self):
        """Test the SimpleStrategy on a 3 node cluster"""
        self.cluster.populate(3).start()
        time.sleep(20)
        node1 = self.cluster.nodelist()[0]
        self.conn = self.cql_connection(node1)
        
        # Install a tracing cursor so we can get info about who the
        # coordinator is contacting: 
        self.conn.cursorclass = TracingCursor
        cursor = self.conn.cursor()

        replication_factor = 3
        self.create_ks(cursor, 'test', replication_factor)
        cursor.execute('CREATE TABLE test.test (id int PRIMARY KEY, value text)', trace=False)
        # Wait for table creation, otherwise trace times out -
        # CASSANDRA-5658
        time.sleep(5)

        for key, token in murmur3_hashes.items():
            cursor.execute("INSERT INTO test (id, value) VALUES (%s, 'asdf')" % key)
            time.sleep(30)
            trace = cursor.get_last_trace()
            stats = self.get_replicas_from_trace(trace)
            replicas_should_be = set(self.get_replicas_for_token(
                token, replication_factor))
            debug('\nreplicas should be: %s' % replicas_should_be)
            debug('replicas were: %s' % stats['replicas'])
            self.pprint_trace(trace)
            self.assertEqual(stats['replicas'], replicas_should_be)
