import logging
import types

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import Node

from dtest import Tester
from tools.assertions import (assert_all)

from flaky import flaky

from cassandra.metadata import BytesToken, OrderedDict
import pytest
from itertools import chain
from tools.misc import new_node

logging.getLogger('cassandra').setLevel(logging.CRITICAL)

NODELOCAL = 11

def jmx_start(to_start, **kwargs):
    kwargs['jvm_args'] = kwargs.get('jvm_args', []) + ['-XX:-PerfDisableSharedMem']
    to_start.start(**kwargs)


def gen_expected(*values):
    return [["%05d" % i, i, i] for i in chain(*values)]


def repair_nodes(nodes):
    for node in nodes:
        node.nodetool('repair -pr')

def cleanup_nodes(nodes):
    for node in nodes:
        node.nodetool('cleanup')

def patch_start(startable):
    old_start = startable.start

    def new_start(self, *args, **kwargs):
        kwargs['jvm_args'] = kwargs.get('jvm_args', []) + ['-XX:-PerfDisableSharedMem'
                                                           ' -Dcassandra.enable_nodelocal_queries=true']
        return old_start(*args, **kwargs)

    startable.start = types.MethodType(new_start, startable)

class TestTransientReplicationRing(Tester):

    keyspace = "ks"
    table = "tbl"

    def select(self):
        return "SELECT * from %s.%s" % (self.keyspace, self.table)

    def select_statement(self):
        return SimpleStatement(self.select(), consistency_level=NODELOCAL)

    def point_select(self):
        return "SELECT * from %s.%s where pk = %%s" % (self.keyspace, self.table)

    def point_select_statement(self):
        return SimpleStatement(self.point_select(), consistency_level=NODELOCAL)

    def check_expected(self, sessions, expected, node=[i for i in range(0,1000)], cleanup=False):
        """Check that each node has the expected values present"""
        for idx, session, expect, node in zip(range(0, 1000), sessions, expected, node):
            print("Checking idx " + str(idx))
            print(str([row for row in session.execute(self.select_statement())]))
            if cleanup:
                node.nodetool('cleanup')
            assert_all(session,
                       self.select(),
                       expect,
                       cl=NODELOCAL)

    def check_replication(self, sessions, exactly=None, gte=None, lte=None):
        """Assert that the test values are replicated a required number of times"""
        for i in range(0, 40):
            count = 0
            for session in sessions:
                for row in session.execute(self.point_select_statement(), ["%05d" % i]):
                    count += 1
            if exactly:
                assert count == exactly, "Wrong replication for %05d should be exactly" % (i, exactly)
            if gte:
                assert count >= gte, "Count for %05d should be >= %d" % (i, gte)
            if lte:
                assert count <= lte, "Count for %05d should be <= %d" % (i, lte)

    @pytest.fixture
    def cheap_quorums(self):
        return False

    @pytest.fixture(scope='function', autouse=True)
    def setup_cluster(self, fixture_dtest_setup):
        self.tokens = ['00010', '00020', '00030']

        patch_start(self.cluster)
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False,
                                                       'num_tokens': 1,
                                                       'commitlog_sync_period_in_ms': 500,
                                                       'enable_transient_replication': True,
                                                       'partitioner' : 'org.apache.cassandra.dht.OrderPreservingPartitioner'})
        print("CLUSTER INSTALL DIR: ")
        print(self.cluster.get_install_dir())
        self.cluster.populate(3, tokens=self.tokens, debug=True, install_byteman=True)
        # self.cluster.populate(3, debug=True, install_byteman=True)
        self.cluster.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=['-Dcassandra.enable_nodelocal_queries=true'])

        # enable shared memory
        for node in self.cluster.nodelist():
            patch_start(node)
            print(node.logfilename())

        self.nodes = self.cluster.nodelist()
        self.node1, self.node2, self.node3 = self.nodes
        session = self.exclusive_cql_connection(self.node3)

        replication_params = OrderedDict()
        replication_params['class'] = 'NetworkTopologyStrategy'
        replication_params['datacenter1'] = '3/1'
        replication_params = ', '.join("'%s': '%s'" % (k, v) for k, v in replication_params.items())

        session.execute("CREATE KEYSPACE %s WITH REPLICATION={%s}" % (self.keyspace, replication_params))
        print("CREATE KEYSPACE %s WITH REPLICATION={%s}" % (self.keyspace, replication_params))
        self.create_table(session)

    def create_table(self, session, never_speculate=False):
        if never_speculate:
            session.execute("CREATE TABLE %s.%s (pk varchar, ck int, value int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NEVER' AND read_repair = 'NONE'" % (self.keyspace, self.table))
        else:
            session.execute("CREATE TABLE %s.%s (pk varchar, ck int, value int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'" % (self.keyspace, self.table))
        print(str(self.node1.run_cqlsh("describe table %s.%s" % (self.keyspace, self.table))))

    def quorum(self, session, stmt_str):
        return session.execute(SimpleStatement(stmt_str, consistency_level=ConsistencyLevel.QUORUM))

    def insert_row(self, pk, ck, value, session=None, node=None):
        session = session or self.exclusive_cql_connection(node or self.node1)
        #token = BytesToken.from_key(pack('>i', pk)).value
        #assert token < BytesToken.from_string(self.tokens[0]).value or BytesToken.from_string(self.tokens[-1]).value < token   # primary replica should be node1
        #TODO Is quorum really right? I mean maybe we want ALL with retries since we really don't want to the data
        #not at a replica unless it is intentional
        self.quorum(session, "INSERT INTO %s.%s (pk, ck, value) VALUES ('%05d', %s, %s)" % (self.keyspace, self.table, pk, ck, value))

    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_bootstrap_and_cleanup(self):
        """Test bootstrapping a new node across a mix of repaired and unrepaired data"""
        main_session = self.patient_cql_connection(self.node1)
        self.table = 'tbl2'
        self.create_table(main_session, never_speculate=True)
        nodes = [self.node1, self.node2, self.node3]

        for i in range(0, 40, 2):
            self.insert_row(i, i, i, main_session)

        sessions = [self.exclusive_cql_connection(node) for node in [self.node1, self.node2, self.node3]]

        expected = [gen_expected(range(0, 11, 2), range(22, 40, 2)),
                    gen_expected(range(0, 22, 2), range(32, 40, 2)),
                    gen_expected(range(12, 31, 2))]
        self.check_expected(sessions, expected)

        #Make sure at least a little data is repaired, this shouldn't move data anywhere
        repair_nodes(nodes)

        self.check_expected(sessions, expected)

        #Ensure that there is at least some transient data around, because of this if it's missing after bootstrap
        #We know we failed to get it from the transient replica losing the range entirely
        nodes[1].stop(wait_other_notice=True)

        for i in range(1, 40, 2):
            self.insert_row(i, i, i, main_session)

        nodes[1].start(wait_for_binary_proto=True, wait_other_notice=True)

        sessions = [self.exclusive_cql_connection(node) for node in [self.node1, self.node2, self.node3]]

        expected = [gen_expected(range(0, 11), range(11, 20, 2), range(21, 40)),
                    gen_expected(range(0, 21, 2), range(32, 40, 2)),
                    gen_expected(range(1, 11, 2), range(11, 31), range(31, 40, 2))]

        #Every node should have some of its fully replicated data and one and two should have some transient data
        self.check_expected(sessions, expected)

        node4 = new_node(self.cluster, bootstrap=True, token='00040')
        patch_start(node4)
        nodes.append(node4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        expected.append(gen_expected(range(11, 20, 2), range(21, 40)))
        sessions.append(self.exclusive_cql_connection(node4))

        #Because repair was never run and nodes had transient data it will have data for transient ranges (node1, 11-20)
        assert_all(sessions[3],
                   self.select(),
                   expected[3],
                   cl=NODELOCAL)

        #Node1 no longer transiently replicates 11-20, so cleanup will clean it up
        #Node1 also now transiently replicates 21-30 and half the values in that range were repaired
        expected[0] = gen_expected(range(0, 11), range(21, 30, 2), range(31, 40))
        #Node2 still missing data since it was down during some insertions, it also lost some range (31-40)
        expected[1] = gen_expected(range(0, 21, 2))
        expected[2] = gen_expected(range(1, 11, 2), range(11, 31))

        #Cleanup should only impact if a node lost a range entirely or started to transiently replicate it and the data
        #was repaired
        self.check_expected(sessions, expected, nodes, cleanup=True)

        repair_nodes(nodes)

        expected = [gen_expected(range(0, 11), range(31, 40)),
                    gen_expected(range(0, 21)),
                    gen_expected(range(11, 31)),
                    gen_expected(range(21, 40))]

        self.check_expected(sessions, expected, nodes, cleanup=True)

        #Every value should be replicated exactly 2 times
        self.check_replication(sessions, exactly=2)

    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def move_test(self, move_token, expected_after_move, expected_after_repair):
        """Helper method to run a move test cycle"""
        node4 = new_node(self.cluster, bootstrap=True, token='00040')
        patch_start(node4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        main_session = self.patient_cql_connection(self.node1)
        self.table = 'tbl2'
        self.create_table(main_session, never_speculate=True)
        nodes = [self.node1, self.node2, self.node3, node4]

        for i in range(0, 40, 2):
            print("Inserting " + str(i))
            self.insert_row(i, i, i, main_session)

        # Make sure at least a little data is repaired
        repair_nodes(nodes)

        # Ensure that there is at least some transient data around, because of this if it's missing after bootstrap
        # We know we failed to get it from the transient replica losing the range entirely
        nodes[1].stop(wait_other_notice=True)

        for i in range(1, 40, 2):
            print("Inserting " + str(i))
            self.insert_row(i, i, i, main_session)

        nodes[1].start(wait_for_binary_proto=True, wait_other_notice=True)
        sessions = [self.exclusive_cql_connection(node) for node in [self.node1, self.node2, self.node3, node4]]

        expected = [gen_expected(range(0, 11), range(31, 40)),
                    gen_expected(range(0, 21, 2)),
                    gen_expected(range(1, 11, 2), range(11, 31)),
                    gen_expected(range(11, 20, 2), range(21, 40))]
        self.check_expected(sessions, expected)
        self.check_replication(sessions, exactly=2)

        nodes[0].nodetool('move %s' % move_token)
        cleanup_nodes(nodes)

        self.check_replication(sessions, gte=2, lte=3)
        self.check_expected(sessions, expected=expected_after_move)

        repair_nodes(nodes)

        self.check_expected(sessions, expected_after_repair, nodes, cleanup=True)
        self.check_replication(sessions, exactly=2)


    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_move_forwards_between_and_cleanup(self):
        """Test moving a node forwards past a neighbor token"""
        move_token = '00025'
        expected_after_move = [gen_expected(range(0, 26), range(31, 40, 2)),
                               gen_expected(range(0, 21, 2), range(31, 40)),
                               gen_expected(range(1, 11, 2), range(11, 21, 2), range(21,31)),
                               gen_expected(range(21, 26, 2), range(26, 40))]
        expected_after_repair = [gen_expected(range(0, 26)),
                                 gen_expected(range(0, 21), range(31, 40)),
                                 gen_expected(range(21, 31),),
                                 gen_expected(range(26, 40))]
        self.move_test(move_token, expected_after_move, expected_after_repair)


    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_move_forwards_and_cleanup(self):
        """Test moving a node forwards without going past a neighbor token"""
        move_token = '00015'
        expected_after_move = [gen_expected(range(0, 16), range(31, 40)),
                               gen_expected(range(0, 21, 2)),
                               gen_expected(range(1, 16, 2), range(16, 31)),
                               gen_expected(range(17, 20, 2), range(21, 40))]
        expected_after_repair = [gen_expected(range(0, 16), range(31, 40)),
                                 gen_expected(range(0, 21)),
                                 gen_expected(range(16, 31)),
                                 gen_expected(range(21, 40))]
        self.move_test(move_token, expected_after_move, expected_after_repair)


    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_move_backwards_between_and_cleanup(self):
        """Test moving a node backwards past it's preceding neighbor's token"""
        move_token = '00035'
        expected_after_move = [gen_expected(range(1, 21, 2), range(21, 36)),
                               gen_expected(range(0, 21, 2), range(36, 40)),
                               gen_expected(range(0, 31), range(37, 40, 2)),
                               gen_expected(range(21, 30, 2), range(31, 40))]
        expected_after_repair = [gen_expected(range(21, 36)),
                                 gen_expected(range(0, 21), range(36, 40)),
                                 gen_expected(range(0, 31)),
                                 gen_expected(range(31, 40))]
        self.move_test(move_token, expected_after_move, expected_after_repair)


    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_move_backwards_and_cleanup(self):
        """Test moving a node backwards without moving past a neighbor token"""
        move_token = '00005'
        expected_after_move = [gen_expected(range(0, 6), range(31, 40)),
                               gen_expected(range(0, 21, 2)),
                               gen_expected(range(1, 6, 2), range(6, 31)),
                               gen_expected(range(7, 20, 2), range(21, 40))]
        expected_after_repair = [gen_expected(range(0, 6), range(31, 40)),
                                 gen_expected(range(0, 21)),
                                 gen_expected(range(6, 31)),
                                 gen_expected(range(21, 40))]
        self.move_test(move_token, expected_after_move, expected_after_repair)


    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_decommission(self):
        """Test decommissioning a node correctly streams out all the data"""
        node4 = new_node(self.cluster, bootstrap=True, token='00040')
        patch_start(node4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        main_session = self.patient_cql_connection(self.node1)
        self.table = 'tbl2'
        self.create_table(main_session, never_speculate=True)
        nodes = [self.node1, self.node2, self.node3, node4]

        for i in range(0, 40, 2):
            print("Inserting " + str(i))
            self.insert_row(i, i, i, main_session)

        # Make sure at least a little data is repaired
        repair_nodes(nodes)

        # Ensure that there is at least some transient data around, because of this if it's missing after bootstrap
        # We know we failed to get it from the transient replica losing the range entirely
        nodes[1].stop(wait_other_notice=True)

        for i in range(1, 40, 2):
            print("Inserting " + str(i))
            self.insert_row(i, i, i, main_session)

        nodes[1].start(wait_for_binary_proto=True, wait_other_notice=True)
        sessions = [self.exclusive_cql_connection(node) for node in [self.node1, self.node2, self.node3, node4]]

        expected = [gen_expected(range(0, 11), range(31, 40)),
                    gen_expected(range(0, 21, 2)),
                    gen_expected(range(1, 11, 2), range(11, 31)),
                    gen_expected(range(11, 20, 2), range(21, 40))]

        self.check_expected(sessions, expected)

        #node1 has transient data we want to see streamed out on move
        nodes[3].nodetool('decommission')

        nodes = nodes[:-1]
        sessions = sessions[:-1]

        expected = [gen_expected(range(0, 11), range(11, 21, 2), range(21, 40)),
                    gen_expected(range(0, 21, 2), range(21, 30, 2), range(31, 40)),
                    gen_expected(range(1, 11, 2), range(11, 31), range(31, 40, 2))]

        cleanup_nodes(nodes)

        self.check_replication(sessions, gte=2, lte=3)
        self.check_expected(sessions, expected)

        repair_nodes(nodes)

        #There should be no transient data anywhere
        expected = [gen_expected(range(0, 11), range(21, 40)),
                    gen_expected(range(0, 21), range(31, 40)),
                    gen_expected(range(11, 31))]

        self.check_expected(sessions, expected, nodes, cleanup=True)
        self.check_replication(sessions, exactly=2)


    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_remove(self):
        """Test  a mix of ring change operations across a mix of transient and repaired/unrepaired data"""
        node4 = new_node(self.cluster, bootstrap=True, token='00040')
        patch_start(node4)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)
        main_session = self.patient_cql_connection(self.node1)
        self.table = 'tbl2'
        self.create_table(main_session, never_speculate=True)
        nodes = [self.node1, self.node2, self.node3]

        #We want the node being removed to have no data on it so nodetool remove always gets all the necessary data
        #from survivors
        node4_id = node4.nodetool('info').stdout[25:61]
        node4.stop(wait_other_notice=True)

        for i in range(0, 40):
            print("Inserting " + str(i))
            self.insert_row(i, i, i, main_session)

        sessions = [self.exclusive_cql_connection(node) for node in [self.node1, self.node2, self.node3]]

        expected = [gen_expected(range(0, 11), range(21, 40)),
                    gen_expected(range(0, 21), range(31, 40)),
                    gen_expected(range(11, 31))]

        # Every node should some of its fully replicated data and one and two should have some transient data
        self.check_expected(sessions, expected)

        nodes[0].nodetool('removenode ' + node4_id)

        #Give streaming time to occur, it's asynchronous from removenode completing at other ndoes
        import time
        time.sleep(15)

        # Everyone should have everything except
        expected = [gen_expected(range(0, 40)),
                    gen_expected(range(0, 40)),
                    gen_expected(range(0,40))]

        self.check_replication(sessions, exactly=3)
        self.check_expected(sessions, expected)
        repair_nodes(nodes)
        cleanup_nodes(nodes)

        self.check_replication(sessions, exactly=2)

        expected = [gen_expected(range(0,11), range(21,40)),
                    gen_expected(range(0,21), range(31, 40)),
                    gen_expected(range(11,31))]
        self.check_expected(sessions, expected)

    @flaky(max_runs=1)
    @pytest.mark.no_vnodes
    def test_replace(self):
        main_session = self.patient_cql_connection(self.node1)
        self.table = 'tbl2'
        self.create_table(main_session, never_speculate=True)

        #We want the node being replaced to have no data on it so the replacement definitely fetches all the data
        self.node2.stop(wait_other_notice=True)

        for i in range(0, 40):
            print("Inserting " + str(i))
            self.insert_row(i, i, i, main_session)

        replacement_address = self.node2.address()
        self.node2.stop(wait_other_notice=True)
        self.cluster.remove(self.node2)
        self.node2 = Node('replacement', cluster=self.cluster, auto_bootstrap=True,
                                         thrift_interface=None, storage_interface=(replacement_address, 7000),
                                         jmx_port='7400', remote_debug_port='0', initial_token=None, binary_interface=(replacement_address, 9042))
        patch_start(self.node2)
        nodes = [self.node1, self.node2, self.node3]
        self.cluster.add(self.node2, False, data_center='datacenter1')
        jvm_args = ["-Dcassandra.replace_address=%s" % replacement_address,
                    "-Dcassandra.ring_delay_ms=10000",
                    "-Dcassandra.broadcast_interval_ms=10000"]
        self.node2.start(jvm_args=jvm_args, wait_for_binary_proto=True, wait_other_notice=True)

        sessions = [self.exclusive_cql_connection(node) for node in [self.node1, self.node2, self.node3]]

        # Everyone should have everything
        expected = [gen_expected(range(0, 40)),
                    gen_expected(range(0, 40)),
                    gen_expected(range(0,40))]

        self.check_replication(sessions, exactly=3)
        self.check_expected(sessions, expected)

        repair_nodes(nodes)
        cleanup_nodes(nodes)

        self.check_replication(sessions, exactly=2)

        expected = [gen_expected(range(0,11), range(21,40)),
                    gen_expected(range(0,21), range(31, 40)),
                    gen_expected(range(11,31))]
        self.check_expected(sessions, expected)