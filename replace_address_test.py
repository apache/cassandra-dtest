import os
from itertools import chain
from shutil import rmtree
import tempfile

from cassandra import ConsistencyLevel, ReadTimeout, Unavailable
from cassandra.query import SimpleStatement
from ccmlib.node import Node, NodeError

from dtest import DISABLE_VNODES, Tester, debug
from tools import InterruptBootstrap, known_failure, since, new_node, rows_to_list

from bootstrap_test import assert_bootstrap_state
from assertions import assert_not_running, assert_all


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

    @known_failure(failure_source='systemic',
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
            num_tokens = 1
        else:
            # a little hacky but grep_log returns the whole line...
            num_tokens = int(node3.get_conf_option('num_tokens'))

        debug("testing with num_tokens: {}".format(num_tokens))

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        session.default_timeout = 45
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initial_data = rows_to_list(session.execute(query))

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
        node4 = Node('node4', cluster=cluster, auto_bootstrap=True, thrift_interface=('127.0.0.4', 9160),
                     storage_interface=('127.0.0.4', 7000), jmx_port='7400', remote_debug_port='0',
                     initial_token=None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.3', wait_for_binary_proto=True)

        debug("Verifying tokens migrated sucessfully")
        moved_tokens = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug("number of moved tokens: {}".format(len(moved_tokens)))
        self.assertEqual(len(moved_tokens), num_tokens)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start(wait_other_notice=False)
        collision_log = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(collision_log)
        self.assertEqual(len(collision_log), 1)
        node3.stop(gently=False)

        # query should work again
        debug("Stopping old nodes")
        node1.stop(gently=False, wait_other_notice=True)
        node2.stop(gently=False, wait_other_notice=True)

        debug("Verifying data on new node.")
        session = self.patient_exclusive_cql_connection(node4)
        assert_all(session, 'SELECT * from {} LIMIT 1'.format(stress_table),
                   expected=initial_data,
                   cl=ConsistencyLevel.ONE)

    def replace_active_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # replace active node 3 with node 4
        debug("Starting node 4 to replace active node 3")
        node4 = Node('node4', cluster=cluster, auto_bootstrap=True, thrift_interface=('127.0.0.4', 9160),
                     storage_interface=('127.0.0.4', 7000), jmx_port='7400', remote_debug_port='0',
                     initial_token=None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)

        mark = node4.mark_log()
        node4.start(replace_address='127.0.0.3', wait_other_notice=False)
        node4.watch_log_for("java.lang.UnsupportedOperationException: Cannot replace a live node...", from_mark=mark)
        assert_not_running(node4)

    def replace_nonexistent_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        debug('Start node 4 and replace an address with no node')
        node4 = Node('node4', cluster=cluster, auto_bootstrap=True, thrift_interface=('127.0.0.4', 9160),
                     storage_interface=('127.0.0.4', 7000), jmx_port='7400', remote_debug_port='0',
                     initial_token=None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)

        # try to replace an unassigned ip address
        mark = node4.mark_log()
        node4.start(replace_address='127.0.0.5', wait_other_notice=False)
        node4.watch_log_for("java.lang.RuntimeException: Cannot replace_address /127.0.0.5 because it doesn't exist in gossip", from_mark=mark)
        assert_not_running(node4)

    def replace_first_boot_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            num_tokens = 1
        else:
            # a little hacky but grep_log returns the whole line...
            num_tokens = int(node3.get_conf_option('num_tokens'))

        debug("testing with num_tokens: {}".format(num_tokens))

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initial_data = rows_to_list(session.execute(query))

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
        node4 = Node('node4', cluster=cluster, auto_bootstrap=True, thrift_interface=('127.0.0.4', 9160),
                     storage_interface=('127.0.0.4', 7000), jmx_port='7400', remote_debug_port='0',
                     initial_token=None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"], wait_for_binary_proto=True)

        debug("Verifying tokens migrated sucessfully")
        moved_tokens = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug("number of moved tokens: {}".format(len(moved_tokens)))
        self.assertEqual(len(moved_tokens), num_tokens)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start(wait_other_notice=False)
        collision_log = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(collision_log)
        self.assertEqual(len(collision_log), 1)

        # restart node4 (if error's might have to change num_tokens)
        node4.stop(gently=False)
        node4.start(wait_for_binary_proto=True, wait_other_notice=False)

        # we redo this check because restarting node should not result in tokens being moved again, ie number should be same
        debug("Verifying tokens migrated sucessfully")
        moved_tokens = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug("number of moved tokens: {}".format(len(moved_tokens)))
        self.assertEqual(len(moved_tokens), num_tokens)

        # query should work again
        debug("Stopping old nodes")
        node1.stop(gently=False, wait_other_notice=True)
        node2.stop(gently=False, wait_other_notice=True)

        debug("Verifying data on new node.")
        session = self.patient_exclusive_cql_connection(node4)
        assert_all(session, 'SELECT * from {} LIMIT 1'.format(stress_table),
                   expected=initial_data,
                   cl=ConsistencyLevel.ONE)

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
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])

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
        cluster.set_batch_commitlog(enabled=True)
        node1, node2, node3 = cluster.nodelist()
        cluster.seeds.remove(node3)
        NUM_TOKENS = os.environ.get('NUM_TOKENS', '256')
        if DISABLE_VNODES:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': 1})
        else:
            cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': NUM_TOKENS})
        cluster.start()

        debug('Inserting Data...')
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])
        cluster.flush()

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from {} LIMIT 1'.format(stress_table), consistency_level=ConsistencyLevel.THREE)
        initial_data = rows_to_list(session.execute(query))

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
                debug("Stopping old nodes")
                node1.stop(gently=False, wait_other_notice=True)
                node2.stop(gently=False, wait_other_notice=True)

                debug("Verifying data on new node.")
                session = self.patient_exclusive_cql_connection(node3)
                assert_all(session, 'SELECT * from {} LIMIT 1'.format(stress_table),
                           expected=initial_data,
                           cl=ConsistencyLevel.ONE)
            else:
                debug('Starting node 3 with auto_bootstrap = false and replace_address = 127.0.0.3')
                node3.start(replace_address='127.0.0.3', wait_other_notice=False)
                node3.watch_log_for('To perform this operation, please restart with -Dcassandra.allow_unsafe_replace=true',
                                    from_mark=mark, timeout=20)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12085',
                   flaky=True,
                   notes='windows')
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

        node1.stress(['write', 'n=100K', 'no-warmup', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initial_data = rows_to_list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster=cluster, auto_bootstrap=True, thrift_interface=('127.0.0.4', 9160),
                     storage_interface=('127.0.0.4', 7000), jmx_port='7400', remote_debug_port='0',
                     initial_token=None, binary_interface=('127.0.0.4', 9042))
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
        assert_bootstrap_state(self, node4, 'COMPLETED')

        # query should work again
        debug("Stopping old nodes")
        node1.stop(gently=False, wait_other_notice=True)
        node2.stop(gently=False, wait_other_notice=True)

        debug("Verifying data on new node.")
        session = self.patient_exclusive_cql_connection(node4)
        assert_all(session, 'SELECT * from {} LIMIT 1'.format(stress_table),
                   expected=initial_data,
                   cl=ConsistencyLevel.ONE)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11835')
    @since('2.2')
    def replace_with_reset_resume_state_test(self):
        """Test replace with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=100K', 'no-warmup', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initial_data = rows_to_list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster=cluster, auto_bootstrap=True, thrift_interface=('127.0.0.4', 9160),
                     storage_interface=('127.0.0.4', 7000), jmx_port='7400', remote_debug_port='0',
                     initial_token=None, binary_interface=('127.0.0.4', 9042))

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
        assert_bootstrap_state(self, node4, 'COMPLETED')

        # query should work again
        debug("Stopping old nodes")
        node1.stop(gently=False, wait_other_notice=True)
        node2.stop(gently=False, wait_other_notice=True)

        debug("Verifying data on new node.")
        session = self.patient_exclusive_cql_connection(node4)
        assert_all(session, 'SELECT * from {} LIMIT 1'.format(stress_table),
                   expected=initial_data,
                   cl=ConsistencyLevel.ONE)

    def replace_with_insufficient_replicas_test(self):
        """
        Test that replace fails when there are insufficient replicas
        @jira_ticket CASSANDRA-11848
        """
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            num_tokens = 1
        else:
            # a little hacky but grep_log returns the whole line...
            num_tokens = int(node3.get_conf_option('num_tokens'))

        debug("testing with num_tokens: {}".format(num_tokens))

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=2)'])

        # stop node to replace
        debug("Stopping node to replace.")
        node3.stop(wait_other_notice=True)

        # stop other replica
        debug("Stopping node2 (other replica)")
        node2.stop(wait_other_notice=True)

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")

        node4 = Node('node4', cluster=cluster, auto_bootstrap=True, thrift_interface=('127.0.0.4', 9160),
                     storage_interface=('127.0.0.4', 7000), jmx_port='7400', remote_debug_port='0',
                     initial_token=None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.3', wait_for_binary_proto=False, wait_other_notice=False)

        # replace should fail due to insufficient replicas
        node4.watch_log_for("Unable to find sufficient sources for streaming range")
        assert_not_running(node4)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12276',
                   flaky=False,
                   notes='windows')
    def multi_dc_replace_with_rf1_test(self):
        """
        Test that multi-dc replace works when rf=1 on each dc
        """
        cluster = self.cluster
        cluster.populate([1, 1])
        cluster.start()
        node1, node2 = cluster.nodelist()

        node1 = cluster.nodes['node1']
        yaml_config = """
        # Create the keyspace and table
        keyspace: keyspace1
        keyspace_definition: |
          CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
        table: users
        table_definition:
          CREATE TABLE users (
            username text,
            first_name text,
            last_name text,
            email text,
            PRIMARY KEY(username)
          ) WITH compaction = {'class':'SizeTieredCompactionStrategy'};
        insert:
          partitions: fixed(1)
          batchtype: UNLOGGED
        queries:
          read:
            cql: select * from users where username = ?
            fields: samerow
        """
        with tempfile.NamedTemporaryFile(mode='w+') as stress_config:
            stress_config.write(yaml_config)
            stress_config.flush()
            node1.stress(['user', 'profile=' + stress_config.name, 'n=10k', 'no-warmup',
                          'ops(insert=1)', '-rate', 'threads=50'])

        session = self.patient_cql_connection(node1)

        # change system_auth keyspace to 2 (default is 1) to avoid
        # "Unable to find sufficient sources for streaming" warning
        if cluster.cassandra_version() >= '2.2.0':
            session.execute("""
                ALTER KEYSPACE system_auth
                    WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};
            """)

        # Save initial data
        stress_table = 'keyspace1.users'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.TWO)
        initial_data = rows_to_list(session.execute(query))

        # stop node to replace
        debug("Stopping node to replace.")
        node2.stop(wait_other_notice=True)

        node3 = new_node(cluster, data_center='dc2')
        node3.start(replace_address='127.0.0.2', wait_for_binary_proto=True)

        assert_bootstrap_state(self, node3, 'COMPLETED')

        # Check that keyspace was replicated from dc1 to dc2
        self.assertFalse(node3.grep_log("Unable to find sufficient sources for streaming range"))

        # query should work again with node1 stopped
        node1.stop(wait_other_notice=True)
        debug("Verifying data on new node.")
        session = self.patient_exclusive_cql_connection(node3)
        assert_all(session, 'SELECT * from {} LIMIT 1'.format(stress_table),
                   expected=initial_data,
                   cl=ConsistencyLevel.LOCAL_ONE)
