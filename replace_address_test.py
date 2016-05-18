import os
from itertools import chain
from shutil import rmtree
from time import sleep

from cassandra import ConsistencyLevel, ReadTimeout, Unavailable
from cassandra.query import SimpleStatement
from ccmlib.node import Node, NodeError

from dtest import DISABLE_VNODES, Tester, debug
from tools import InterruptBootstrap, known_failure, since


class NodeUnavailable(Exception):
    pass


class TestReplaceAddress(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread',
            # ignore streaming error during bootstrap
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)
        self.allow_log_errors = True

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11652',
                   flaky=True,
                   notes='windows')
    def replace_stopped_node_test(self):
        """
        Test that we can replace a node that is not shutdown gracefully.
        """
        self._replace_node_test(gently=False)

    def replace_shutdown_node_test(self):
        """
        @jira_ticket CASSANDRA-9871
        Test that we can replace a node that is shutdown gracefully.
        """
        self._replace_node_test(gently=True)

    def _replace_node_test(self, gently):
        """
        Check that the replace address function correctly replaces a node that has failed in a cluster.
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        session.default_timeout = 45
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
                session.execute(query)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")

        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.3', wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start(wait_other_notice=False)
        checkCollision = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

    def replace_active_node_test(self):

        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # replace active node 3 with node 4
        debug("Starting node 4 to replace active node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)

        mark = node4.mark_log()
        node4.start(replace_address='127.0.0.3', wait_other_notice=False)
        node4.watch_log_for("java.lang.UnsupportedOperationException: Cannot replace a live node...", from_mark=mark)
        self.check_not_running(node4)

    def replace_nonexistent_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        debug('Start node 4 and replace an address with no node')
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)

        # try to replace an unassigned ip address
        mark = node4.mark_log()
        node4.start(replace_address='127.0.0.5', wait_other_notice=False)
        node4.watch_log_for("java.lang.RuntimeException: Cannot replace_address /127.0.0.5 because it doesn't exist in gossip", from_mark=mark)
        self.check_not_running(node4)

    def check_not_running(self, node):
        attempts = 0
        while node.is_running() and attempts < 10:
            sleep(1)
            attempts = attempts + 1

        self.assertFalse(node.is_running())

    def replace_first_boot_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=False)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                session.execute(query, timeout=30)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"], wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start(wait_other_notice=False)
        checkCollision = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

        # restart node4 (if error's might have to change num_tokens)
        node4.stop(gently=False)
        node4.start(wait_for_binary_proto=True, wait_other_notice=False)

        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

        # we redo this check because restarting node should not result in tokens being moved again, ie number should be same
        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11691',
                   flaky=False,
                   notes='Windows')
    @since('3.6')
    def fail_without_replace_test(self):
        """
        When starting a node from a clean slate with the same address as
        an existing down node, the node should error out even when
        auto_bootstrap = false (or the node is a seed) and tell the user
        to use replace_address.
        @jira_ticket CASSANDRA-10134
        """
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        cluster.seeds.remove(node3)
        NUM_TOKENS = os.environ.get('NUM_TOKENS', '256')
        if DISABLE_VNODES:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': 1})
        else:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': NUM_TOKENS})
        cluster.start()

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])

        mark = None
        for auto_bootstrap in (True, False):
            debug("Stopping node 3.")
            node3.stop(gently=False)

            # completely delete the data, commitlog, and saved caches
            for d in chain([os.path.join(node3.get_path(), "commitlogs")],
                           [os.path.join(node3.get_path(), "saved_caches")],
                           node3.data_directories()):
                if os.path.exists(d):
                    rmtree(d)

            node3.set_configuration_options(values={'auto_bootstrap': auto_bootstrap})
            debug("Starting node 3 with auto_bootstrap = {val}".format(val=auto_bootstrap))
            node3.start(wait_other_notice=False)
            node3.watch_log_for('Use cassandra.replace_address if you want to replace this node', from_mark=mark, timeout=20)
            mark = node3.mark_log()

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11700',
                   flaky=True,
                   notes='windows')
    @since('3.6')
    def unsafe_replace_test(self):
        """
        To handle situations such as failed disk in a JBOD, it may be desirable to
        replace a node without bootstrapping. In such scenarios best practice
        advice has been to wipe the node's system keyspace data, set the initial
        tokens via cassandra.yaml, startup without bootstrap and then repair.
        Starting the node as a replacement allows the tokens to be learned from
        gossip, but previously required auto_bootstrap=true. Since CASSANDRA-10134
        replacement is allowed without bootstrapping, but it requires the operator
        to acknowledge the risk in doing so by setting the cassandra.allow_unsafe_replace
        system property at startup.

        @jira_ticket CASSANDRA-10134
        """
        debug('Starting cluster with 3 nodes.')
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        cluster.seeds.remove(node3)
        NUM_TOKENS = os.environ.get('NUM_TOKENS', '256')
        if DISABLE_VNODES:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': 1})
        else:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': NUM_TOKENS})
        cluster.start()

        debug('Inserting Data...')
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        query = SimpleStatement('select * from keyspace1.standard1 LIMIT 1', consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        for set_allow_unsafe_flag in [False, True]:
            debug('Stopping node 3.')
            node3.stop(gently=False)

            # completely delete the system keyspace data plus commitlog and saved caches
            for d in node3.data_directories():
                system_data = os.path.join(d, 'system')
                if os.path.exists(system_data):
                    rmtree(system_data)

            for d in ['commitlogs', 'saved_caches']:
                p = os.path.join(node3.get_path(), d)
                if os.path.exists(p):
                    rmtree(p)

            node3.set_configuration_options(values={'auto_bootstrap': False})
            mark = node3.mark_log()

            if set_allow_unsafe_flag:
                debug('Starting node3 with auto_bootstrap = false and replace_address = 127.0.0.3 and allow_unsafe_replace = true')
                node3.start(replace_address='127.0.0.3', wait_for_binary_proto=True, jvm_args=['-Dcassandra.allow_unsafe_replace=true'])
                # query should work again
                debug('Verifying querying works again.')
                finalData = list(session.execute(query))
                self.assertListEqual(initialData, finalData)
            else:
                debug('Starting node 3 with auto_bootstrap = false and replace_address = 127.0.0.3')
                node3.start(replace_address='127.0.0.3', wait_other_notice=False)
                node3.watch_log_for('To perform this operation, please restart with -Dcassandra.allow_unsafe_replace=true',
                                    from_mark=mark, timeout=20)

    @since('2.2')
    def resumable_replace_test(self):
        """
        Test resumable bootstrap while replacing node. Feature introduced in
        2.2 with ticket https://issues.apache.org/jira/browse/CASSANDRA-8838

        @jira_ticket https://issues.apache.org/jira/browse/CASSANDRA-8838
        """

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        # keep timeout low so that test won't hang
        node4.set_configuration_options(values={'streaming_socket_timeout_in_ms': 1000})
        cluster.add(node4, False)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"], wait_other_notice=False)
        except NodeError:
            pass  # node doesn't start as expected
        t.join()

        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start()
        node4.nodetool('bootstrap resume')
        # check if we skipped already retrieved ranges
        node4.watch_log_for("already available. Skipping streaming.")
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...")

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11835')
    @since('2.2')
    def replace_with_reset_resume_state_test(self):
        """Test replace with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        # keep timeout low so that test won't hang
        node4.set_configuration_options(values={'streaming_socket_timeout_in_ms': 1000})
        cluster.add(node4, False)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"], wait_other_notice=False)
        except NodeError:
            pass  # node doesn't start as expected
        t.join()
        node1.start()

        # restart node4 bootstrap with resetting bootstrap state
        node4.stop()
        mark = node4.mark_log()
        node4.start(jvm_args=[
                    "-Dcassandra.replace_address_first_boot=127.0.0.3",
                    "-Dcassandra.reset_bootstrap_progress=true"
                    ])
        # check if we reset bootstrap state
        node4.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...", from_mark=mark)

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)
