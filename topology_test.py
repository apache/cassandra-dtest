import re
import time
from threading import Thread
from unittest import skip

from cassandra import ConsistencyLevel
from ccmlib.node import TimeoutError, ToolError
from nose.plugins.attrib import attr

from dtest import Tester, debug, create_ks, create_cf
from tools.assertions import assert_almost_equal, assert_all, assert_none
from tools.data import insert_c1c2, query_c1c2
from tools.decorators import no_vnodes, since


class TestTopology(Tester):

    def do_not_join_ring_test(self):
        """
        @jira_ticket CASSANDRA-9034
        Check that AssertionError is not thrown on SizeEstimatesRecorder before node joins ring
        """
        cluster = self.cluster.populate(1)
        node1, = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False,
                    jvm_args=["-Dcassandra.size_recorder_interval=1"])

        # initial delay is 30s
        time.sleep(40)

        node1.stop(gently=False)

    @since('3.0.11')
    def size_estimates_multidc_test(self):
        """
        Test that primary ranges are correctly generated on
        system.size_estimates for multi-dc, multi-ks scenario
        @jira_ticket CASSANDRA-9639
        """
        debug("Creating cluster")
        cluster = self.cluster
        cluster.set_configuration_options(values={'num_tokens': 2})
        cluster.populate([2, 1])
        node1_1, node1_2, node2_1 = cluster.nodelist()

        debug("Setting tokens")
        node1_tokens, node2_tokens, node3_tokens = ['-6639341390736545756,-2688160409776496397',
                                                    '-2506475074448728501,8473270337963525440',
                                                    '-3736333188524231709,8673615181726552074']
        node1_1.set_configuration_options(values={'initial_token': node1_tokens})
        node1_2.set_configuration_options(values={'initial_token': node2_tokens})
        node2_1.set_configuration_options(values={'initial_token': node3_tokens})
        cluster.set_configuration_options(values={'num_tokens': 2})

        debug("Starting cluster")
        cluster.start()

        out, _, _ = node1_1.nodetool('ring')
        debug("Nodetool ring output {}".format(out))

        debug("Creating keyspaces")
        session = self.patient_cql_connection(node1_1)
        create_ks(session, 'ks1', 3)
        create_ks(session, 'ks2', {'dc1': 2})
        create_cf(session, 'ks1.cf1', columns={'c1': 'text', 'c2': 'text'})
        create_cf(session, 'ks2.cf2', columns={'c1': 'text', 'c2': 'text'})

        debug("Refreshing size estimates")
        node1_1.nodetool('refreshsizeestimates')
        node1_2.nodetool('refreshsizeestimates')
        node2_1.nodetool('refreshsizeestimates')

        """
        CREATE KEYSPACE ks1 WITH replication =
            {'class': 'SimpleStrategy', 'replication_factor': '3'}
        CREATE KEYSPACE ks2 WITH replication =
            {'class': 'NetworkTopologyStrategy', 'dc1': '2'}  AND durable_writes = true;

        Datacenter: dc1
        ==========
        Address     Token
                    8473270337963525440
        127.0.0.1   -6639341390736545756
        127.0.0.1   -2688160409776496397
        127.0.0.2   -2506475074448728501
        127.0.0.2   8473270337963525440

        Datacenter: dc2
        ==========
        Address     Token
                    8673615181726552074
        127.0.0.3   -3736333188524231709
        127.0.0.3   8673615181726552074
        """

        debug("Checking node1_1 size_estimates primary ranges")
        session = self.patient_exclusive_cql_connection(node1_1)
        assert_all(session, "SELECT range_start, range_end FROM system.size_estimates "
                            "WHERE keyspace_name = 'ks1'", [['-3736333188524231709', '-2688160409776496397'],
                                                            ['-9223372036854775808', '-6639341390736545756'],
                                                            ['8673615181726552074', '-9223372036854775808']])
        assert_all(session, "SELECT range_start, range_end FROM system.size_estimates "
                            "WHERE keyspace_name = 'ks2'", [['-3736333188524231709', '-2688160409776496397'],
                                                            ['-6639341390736545756', '-3736333188524231709'],
                                                            ['-9223372036854775808', '-6639341390736545756'],
                                                            ['8473270337963525440', '8673615181726552074'],
                                                            ['8673615181726552074', '-9223372036854775808']])

        debug("Checking node1_2 size_estimates primary ranges")
        session = self.patient_exclusive_cql_connection(node1_2)
        assert_all(session, "SELECT range_start, range_end FROM system.size_estimates "
                            "WHERE keyspace_name = 'ks1'", [['-2506475074448728501', '8473270337963525440'],
                                                            ['-2688160409776496397', '-2506475074448728501']])
        assert_all(session, "SELECT range_start, range_end FROM system.size_estimates "
                            "WHERE keyspace_name = 'ks2'", [['-2506475074448728501', '8473270337963525440'],
                                                            ['-2688160409776496397', '-2506475074448728501']])

        debug("Checking node2_1 size_estimates primary ranges")
        session = self.patient_exclusive_cql_connection(node2_1)
        assert_all(session, "SELECT range_start, range_end FROM system.size_estimates "
                            "WHERE keyspace_name = 'ks1'", [['-6639341390736545756', '-3736333188524231709'],
                                                            ['8473270337963525440', '8673615181726552074']])
        assert_none(session, "SELECT range_start, range_end FROM system.size_estimates "
                             "WHERE keyspace_name = 'ks2'")

    def simple_decommission_test(self):
        """
        @jira_ticket CASSANDRA-9912
        Check that AssertionError is not thrown on SizeEstimatesRecorder after node is decommissioned
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.size_recorder_interval=1"])
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)

        if cluster.version() >= '2.2':
            # reduce system_distributed RF to 2 so we don't require forceful decommission
            session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'2'};")

        # write some data
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])

        # Decommission node and wipe its data
        node2.decommission()
        node2.stop()

        # This sleep is here to give the cluster time to hit the AssertionError
        # described in 9912. Do not remove it.
        time.sleep(10)

    @skip('Hangs on CI for 2.1')
    def concurrent_decommission_not_allowed_test(self):
        """
        Test concurrent decommission is not allowed
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(2).start(wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node2)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ALL)

        mark = node2.mark_log()

        def decommission():
            node2.nodetool('decommission')

        # Launch first decommission in a external thread
        t = Thread(target=decommission)
        t.start()

        # Make sure first decommission is initialized before second decommission
        node2.watch_log_for('DECOMMISSIONING', filename='debug.log')

        # Launch a second decommission, should fail
        with self.assertRaises(ToolError):
            node2.nodetool('decommission')

        # Check data is correctly forwarded to node1 after node2 is decommissioned
        t.join()
        node2.watch_log_for('DECOMMISSIONED', from_mark=mark)
        session = self.patient_cql_connection(node1)
        session.execute('USE ks')
        for n in xrange(0, 10000):
            query_c1c2(session, n, ConsistencyLevel.ONE)

    @since('3.10')
    def resumable_decommission_test(self):
        """
        @jira_ticket CASSANDRA-12008

        Test decommission operation is resumable
        """
        self.ignore_log_patterns = [r'Streaming error occurred', r'Error while decommissioning node', r'Remote peer 127.0.0.2 failed stream session', r'Remote peer 127.0.0.2:7000 failed stream session']
        cluster = self.cluster
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(3, install_byteman=True).start(wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node2)
        # reduce system_distributed RF to 2 so we don't require forceful decommission
        session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'2'};")
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ALL)

        # Execute first rebuild, should fail
        with self.assertRaises(ToolError):
            if cluster.version() >= '4.0':
                script = ['./byteman/4.0/decommission_failure_inject.btm']
            else:
                script = ['./byteman/pre4.0/decommission_failure_inject.btm']
            node2.byteman_submit(script)
            node2.nodetool('decommission')

        # Make sure previous ToolError is due to decommission
        node2.watch_log_for('Error while decommissioning node')

        # Decommission again
        mark = node2.mark_log()
        node2.nodetool('decommission')

        # Check decommision is done and we skipped transfereed ranges
        node2.watch_log_for('DECOMMISSIONED', from_mark=mark)
        node2.grep_log("Skipping transferred range .* of keyspace ks, endpoint {}".format(node2.address_for_current_version_slashy()), filename='debug.log')

        # Check data is correctly forwarded to node1 and node3
        cluster.remove(node2)
        node3.stop(gently=False)
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')
        for i in xrange(0, 10000):
            query_c1c2(session, i, ConsistencyLevel.ONE)
        node1.stop(gently=False)
        node3.start()
        session.shutdown()
        mark = node3.mark_log()
        node3.watch_log_for('Starting listening for CQL clients', from_mark=mark)
        session = self.patient_exclusive_cql_connection(node3)
        session.execute('USE ks')
        for i in xrange(0, 10000):
            query_c1c2(session, i, ConsistencyLevel.ONE)

    @no_vnodes()
    def movement_test(self):
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(3, tokens=[0, 2**48, 2**62]).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=30000, consistency=ConsistencyLevel.ONE)

        cluster.flush()

        # Move nodes to balance the cluster
        def move_node(node, token, ip):
            mark = node.mark_log()
            node.move(token)  # can't assume 0 is balanced with m3p
            node.watch_log_for('{} state jump to NORMAL'.format(ip), from_mark=mark, timeout=180)
            time.sleep(3)

        balancing_tokens = cluster.balanced_tokens(3)

        move_node(node1, balancing_tokens[0], '127.0.0.1')
        move_node(node2, balancing_tokens[1], '127.0.0.2')
        move_node(node3, balancing_tokens[2], '127.0.0.3')

        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 30000):
            query_c1c2(session, n, ConsistencyLevel.ONE)

        # Now the load should be basically even
        sizes = [node.data_size() for node in [node1, node2, node3]]

        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal(sizes[0], sizes[2])
        assert_almost_equal(sizes[1], sizes[2])

    @no_vnodes()
    def decommission_test(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(4)
        cluster.populate(4, tokens=tokens).start()
        node1, node2, node3, node4 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=30000, consistency=ConsistencyLevel.QUORUM)

        cluster.flush()
        sizes = [node.data_size() for node in cluster.nodelist() if node.is_running()]
        init_size = sizes[0]
        assert_almost_equal(*sizes)

        time.sleep(.5)
        node4.decommission()
        node4.stop()
        cluster.cleanup()
        time.sleep(.5)

        # Check we can get all the keys
        for n in xrange(0, 30000):
            query_c1c2(session, n, ConsistencyLevel.QUORUM)

        sizes = [node.data_size() for node in cluster.nodelist() if node.is_running()]
        debug(sizes)
        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal((2.0 / 3.0) * sizes[0], sizes[2])
        assert_almost_equal(sizes[2], init_size)

    @no_vnodes()
    def move_single_node_test(self):
        """ Test moving a node in a single-node cluster (#4200) """
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(1, tokens=[0]).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ONE)

        cluster.flush()

        node1.move(2**25)
        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(session, n, ConsistencyLevel.ONE)

    @since('3.0')
    def decommissioned_node_cant_rejoin_test(self):
        '''
        @jira_ticket CASSANDRA-8801

        Test that a decommissioned node can't rejoin the cluster by:

        - creating a cluster,
        - decommissioning a node, and
        - asserting that the "decommissioned node won't rejoin" error is in the
        logs for that node and
        - asserting that the node is not running.
        '''
        rejoin_err = 'This node was decommissioned and will not rejoin the ring'
        try:
            self.ignore_log_patterns = list(self.ignore_log_patterns)
        except AttributeError:
            self.ignore_log_patterns = []
        self.ignore_log_patterns.append(rejoin_err)

        self.cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = self.cluster.nodelist()

        debug('decommissioning...')
        node3.decommission(force=self.cluster.version() >= '4.0')
        debug('stopping...')
        node3.stop()
        debug('attempting restart...')
        node3.start(wait_other_notice=False)
        try:
            # usually takes 3 seconds, so give it a generous 15
            node3.watch_log_for(rejoin_err, timeout=15)
        except TimeoutError:
            # TimeoutError is not very helpful to the reader of the test output;
            # let that pass and move on to string assertion below
            pass

        self.assertIn(rejoin_err,
                      '\n'.join(['\n'.join(err_list)
                                 for err_list in node3.grep_log_for_errors()]))

        # Give the node some time to shut down once it has detected
        # its invalid state. If it doesn't shut down in the 30 seconds,
        # consider filing a bug. It shouldn't take more than 10, in most cases.
        start = time.time()
        while start + 30 > time.time() and node3.is_running():
            time.sleep(1)

        self.assertFalse(node3.is_running())

    @since('3.0')
    def crash_during_decommission_test(self):
        """
        If a node crashes whilst another node is being decommissioned,
        upon restarting the crashed node should not have invalid entries
        for the decommissioned node
        @jira_ticket CASSANDRA-10231
        """
        cluster = self.cluster
        self.ignore_log_patterns = [r'Streaming error occurred', 'Stream failed']
        cluster.populate(3).start(wait_other_notice=True)

        node1, node2 = cluster.nodelist()[0:2]

        t = DecommissionInParallel(node1)
        t.start()

        node1.watch_log_for("DECOMMISSIONING", filename='debug.log')
        null_status_pattern = re.compile(".N(?:\s*)127\.0\.0\.1(?:.*)null(?:\s*)rack1")
        while t.is_alive():
            out = self.show_status(node2)
            if null_status_pattern.search(out):
                debug("Matched null status entry")
                break
            debug("Restarting node2")
            node2.stop(gently=False)
            node2.start(wait_for_binary_proto=True, wait_other_notice=False)

        debug("Waiting for decommission to complete")
        t.join()
        self.show_status(node2)

        debug("Sleeping for 30 seconds to allow gossip updates")
        time.sleep(30)
        out = self.show_status(node2)
        self.assertFalse(null_status_pattern.search(out))

    @since('3.12')
    @attr('resource-intensive')
    def stop_decommission_too_few_replicas_multi_dc_test(self):
        """
        Decommission should fail when it would result in the number of live replicas being less than
        the replication factor. --force should bypass this requirement.
        @jira_ticket CASSANDRA-12510
        @expected_errors ToolError when # nodes will drop below configured replicas in NTS/SimpleStrategy
        """
        cluster = self.cluster
        cluster.populate([2, 2]).start(wait_for_binary_proto=True)
        node1, node2, node3, node4 = self.cluster.nodelist()
        session = self.patient_cql_connection(node2)
        session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'2'};")
        create_ks(session, 'ks', {'dc1': 2, 'dc2': 2})
        with self.assertRaises(ToolError):
            node4.nodetool('decommission')

        session.execute('DROP KEYSPACE ks')
        create_ks(session, 'ks2', 4)
        with self.assertRaises(ToolError):
            node4.nodetool('decommission')

        node4.nodetool('decommission --force')
        decommissioned = node4.watch_log_for("DECOMMISSIONED", timeout=120)
        self.assertTrue(decommissioned, "Node failed to decommission when passed --force")

    def show_status(self, node):
        out, _, _ = node.nodetool('status')
        debug("Status as reported by node {}".format(node.address()))
        debug(out)
        return out


class DecommissionInParallel(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        node = self.node
        mark = node.mark_log()
        try:
            out, err, _ = node.nodetool("decommission")
            node.watch_log_for("DECOMMISSIONED", from_mark=mark)
            debug(out)
            debug(err)
        except ToolError as e:
            debug("Decommission failed with exception: " + str(e))
            pass
