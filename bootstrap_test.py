import os
import random
import re
import shutil
import tempfile
import threading
import time

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodeError

from assertions import assert_almost_equal, assert_one, assert_not_running
from dtest import Tester, debug, DISABLE_VNODES
from tools import (InterruptBootstrap, KillOnBootstrap, known_failure,
                   new_node, no_vnodes, query_c1c2, since)


def assert_bootstrap_state(tester, node, expected_bootstrap_state):
    """
    Assert that a node is on a given bootstrap state
    @param tester The dtest.Tester object to fetch the exclusive connection to the node
    @param node The node to check bootstrap state
    @param expected_bootstrap_state Bootstrap state to expect

    Examples:
    assert_bootstrap_state(self, node3, 'COMPLETED')
    """
    session = tester.patient_exclusive_cql_connection(node)
    assert_one(session, "SELECT bootstrapped FROM system.local WHERE key='local'", [expected_bootstrap_state])


class TestBootstrap(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)
        self.allow_log_errors = True

    def _base_bootstrap_test(self, bootstrap):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        debug("[node1, node2] tokens: %r" % (tokens,))

        keys = 10000

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start(wait_other_notice=True)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        # record the size before inserting any of our own data
        empty_size = node1.data_size()
        debug("node1 empty size : %s" % float(empty_size))

        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        node1.flush()
        node1.compact()
        initial_size = node1.data_size()
        debug("node1 size before bootstrapping node2: %s" % float(initial_size))

        # Reads inserted data all during the bootstrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE))

        # Bootstrapping a new node
        node2 = bootstrap(cluster, tokens[1])
        node2.compact()

        reader.check()
        node1.cleanup()
        debug("node1 size after cleanup: %s" % float(node1.data_size()))
        node1.compact()
        debug("node1 size after compacting: %s" % float(node1.data_size()))
        time.sleep(.5)
        reader.check()

        debug("node2 size after compacting: %s" % float(node2.data_size()))

        size1 = float(node1.data_size())
        size2 = float(node2.data_size())
        assert_almost_equal(size1, size2, error=0.3)
        assert_almost_equal(float(initial_size - empty_size), 2 * (size1 - float(empty_size)))

        assert_bootstrap_state(self, node2, 'COMPLETED')

    @no_vnodes()
    def simple_bootstrap_test(self):
        def bootstrap(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(wait_for_binary_proto=True)
            return node2

        self._base_bootstrap_test(bootstrap)

    @no_vnodes()
    def bootstrap_on_write_survey_test(self):
        def bootstrap_on_write_survey_and_join(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(jvm_args=["-Dcassandra.write_survey=true"], wait_for_binary_proto=True)

            self.assertTrue(len(node2.grep_log('Startup complete, but write survey mode is active, not becoming an active ring member.')))
            assert_bootstrap_state(self, node2, 'IN_PROGRESS')

            node2.nodetool("join")
            self.assertTrue(len(node2.grep_log('Leaving write survey mode and joining ring at operator request')))
            return node2

        self._base_bootstrap_test(bootstrap_on_write_survey_and_join)

    def simple_bootstrap_test_nodata(self):
        """
        @jira_ticket CASSANDRA-11010
        Test that bootstrap completes if streaming from nodes with no data
        """

        cluster = self.cluster
        # Create a two-node cluster
        cluster.populate(2)
        cluster.start(wait_other_notice=True)

        # Bootstrapping a new node
        node3 = new_node(cluster)
        node3.start(wait_for_binary_proto=True, wait_other_notice=True)

        assert_bootstrap_state(self, node3, 'COMPLETED')

    def read_from_bootstrapped_node_test(self):
        """
        Test bootstrapped node sees existing data
        @jira_ticket CASSANDRA-6648
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8', '-schema', 'replication(factor=2)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        original_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))

        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node4)
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        self.assertEquals(original_rows, new_rows)

    def consistent_range_movement_true_with_replica_down_should_fail_test(self):
        self._bootstrap_test_with_replica_down(True)

    def consistent_range_movement_false_with_replica_down_should_succeed_test(self):
        self._bootstrap_test_with_replica_down(False)

    def consistent_range_movement_true_with_rf1_should_fail_test(self):
        self._bootstrap_test_with_replica_down(True, rf=1)

    def consistent_range_movement_false_with_rf1_should_succeed_test(self):
        self._bootstrap_test_with_replica_down(False, rf=1)

    def _bootstrap_test_with_replica_down(self, consistent_range_movement, rf=2):
        """
        Test to check consistent bootstrap will not succeed when there are insufficient replicas
        @jira_ticket CASSANDRA-11848
        """
        cluster = self.cluster

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node3_token = None
        # Make token assignment deterministic
        if DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 1})
            tokens = cluster.balanced_tokens(3)
            debug("non-vnode tokens: %r" % (tokens,))
            node1.set_configuration_options(values={'initial_token': tokens[0]})
            node2.set_configuration_options(values={'initial_token': tokens[2]})
            node3_token = tokens[1]  # Add node 3 between node1 and node2

        cluster.start()

        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8', '-schema', 'replication(factor={})'.format(rf)])

        # change system_auth keyspace to 2 (default is 1) to avoid
        # "Unable to find sufficient sources for streaming" warning
        if cluster.cassandra_version() >= '2.2.0':
            session = self.patient_cql_connection(node1)
            session.execute("""
                ALTER KEYSPACE system_auth
                    WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};
            """)

        # Stop node2, so node3 will not be able to perform consistent range movement
        node2.stop(wait_other_notice=True)

        successful_bootstrap_expected = not consistent_range_movement

        node3 = new_node(cluster, token=node3_token)
        node3.start(wait_for_binary_proto=successful_bootstrap_expected, wait_other_notice=successful_bootstrap_expected,
                    jvm_args=["-Dcassandra.consistent.rangemovement={}".format(consistent_range_movement)])

        if successful_bootstrap_expected:
            # with rf=1 and cassandra.consistent.rangemovement=false, missing sources are ignored
            if not consistent_range_movement and rf == 1:
                node3.watch_log_for("Unable to find sufficient sources for streaming range")
            self.assertTrue(node3.is_running())
            assert_bootstrap_state(self, node3, 'COMPLETED')
        else:
            if consistent_range_movement:
                node3.watch_log_for("A node required to move the data consistently is down")
            else:
                node3.watch_log_for("Unable to find sufficient sources for streaming range")
            assert_not_running(node3)

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11414',
                   flaky=True)
    @since('2.2')
    def resumable_bootstrap_test(self):
        """
        Test resuming bootstrap after data streaming failure
        """

        cluster = self.cluster
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100K', 'no-warmup', 'cl=TWO', '-schema', 'replication(factor=2)', '-rate', 'threads=50'])
        cluster.flush()

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        # keep timeout low so that test won't hang
        node3.set_configuration_options(values={'streaming_socket_timeout_in_ms': 1000})
        node3.start(wait_other_notice=False, wait_for_binary_proto=True)
        t.join()

        # wait for node3 ready to query
        node3.watch_log_for("Starting listening for CQL clients")
        mark = node3.mark_log()
        # check if node3 is still in bootstrap mode
        assert_bootstrap_state(self, node3, 'IN_PROGRESS')

        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start(wait_other_notice=True)
        node3.nodetool('bootstrap resume')

        node3.watch_log_for("Resume complete", from_mark=mark)
        assert_bootstrap_state(self, node3, 'COMPLETED')

        # cleanup to guarantee each node will only have sstables of its ranges
        cluster.cleanup()

        debug("Check data is present")
        # Let's check stream bootstrap completely transferred data

        stdout, stderr, _ = node3.stress(['read', 'n=100k', 'no-warmup', '-schema', 'replication(factor=2)', '-rate', 'threads=8'])

        if stdout is not None:
            self.assertNotIn("FAILURE", stdout)

    @since('2.2')
    def bootstrap_with_reset_bootstrap_state_test(self):
        """Test bootstrap with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        try:
            node3.start()
        except NodeError:
            pass  # node doesn't start as expected
        t.join()
        node1.start()

        # restart node3 bootstrap with resetting bootstrap progress
        node3.stop()
        mark = node3.mark_log()
        node3.start(jvm_args=["-Dcassandra.reset_bootstrap_progress=true"])
        # check if we reset bootstrap state
        node3.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query
        node3.watch_log_for("Listening for thrift clients...", from_mark=mark)

        # check if 2nd bootstrap succeeded
        assert_bootstrap_state(self, node3, 'COMPLETED')

    def manual_bootstrap_test(self):
        """
            Test adding a new node and bootstrapping it manually. No auto_bootstrap.
            This test also verify that all data are OK after the addition of the new node.
            @jira_ticket CASSANDRA-9022
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)
        (node1, node2) = cluster.nodelist()

        node1.stress(['write', 'n=1K', 'no-warmup', '-schema', 'replication(factor=2)',
                      '-rate', 'threads=1', '-pop', 'dist=UNIFORM(1..1000)'])

        session = self.patient_exclusive_cql_connection(node2)
        stress_table = 'keyspace1.standard1'

        original_rows = list(session.execute("SELECT * FROM %s" % stress_table))

        # Add a new node
        node3 = new_node(cluster, bootstrap=False)
        node3.start(wait_for_binary_proto=True)
        node3.repair()
        node1.cleanup()

        current_rows = list(session.execute("SELECT * FROM %s" % stress_table))
        self.assertEquals(original_rows, current_rows)

    def local_quorum_bootstrap_test(self):
        """
        Test that CL local_quorum works while a node is bootstrapping.
        @jira_ticket CASSANDRA-8058
        """

        cluster = self.cluster
        cluster.populate([1, 1])
        cluster.start()

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
            node1.stress(['user', 'profile=' + stress_config.name, 'n=2M', 'no-warmup',
                          'ops(insert=1)', '-rate', 'threads=50'])

        node3 = new_node(cluster, data_center='dc2')
        node3.start(no_wait=True)
        time.sleep(3)

        out, err, _ = node1.stress(['user', 'profile=' + stress_config.name, 'ops(insert=1)',
                                    'n=500K', 'no-warmup', 'cl=LOCAL_QUORUM',
                                    '-rate', 'threads=5',
                                    '-errors', 'retries=2'])

        debug(out)
        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(out)
        self.assertIsNone(failure, "Error during stress while bootstrapping")

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11281',
                   flaky=True,
                   notes='windows')
    def shutdown_wiped_node_cannot_join_test(self):
        self._wiped_node_cannot_join_test(gently=True)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11281',
                   flaky=True,
                   notes='windows')
    def killed_wiped_node_cannot_join_test(self):
        self._wiped_node_cannot_join_test(gently=False)

    def _wiped_node_cannot_join_test(self, gently):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we stop a node and wipe its data then the node cannot join
        when it is not a seed. Test both a nice shutdown or a forced shutdown, via
        the gently parameter.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        stress_table = 'keyspace1.standard1'

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node4 = new_node(cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node4)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Stop the new node and wipe its data
        node4.stop(gently=gently)
        self._cleanup(node4)
        # Now start it, it should not be allowed to join.
        mark = node4.mark_log()
        node4.start(no_wait=True, wait_other_notice=False)
        node4.watch_log_for("A node with address /127.0.0.4 already exists, cancelling join", from_mark=mark)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11281',
                   flaky=True,
                   notes='windows')
    def decommissioned_wiped_node_can_join_test(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we decommission a node and then wipe its data, it can join the cluster.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        stress_table = 'keyspace1.standard1'

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node4 = new_node(cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node4)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Decommission the new node and wipe its data
        node4.decommission()
        node4.stop()
        self._cleanup(node4)
        # Now start it, it should be allowed to join
        mark = node4.mark_log()
        node4.start(wait_other_notice=True)
        node4.watch_log_for("JOINING:", from_mark=mark)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11281',
                   flaky=True,
                   notes='windows')
    def decommissioned_wiped_node_can_gossip_to_single_seed_test(self):
        """
        @jira_ticket CASSANDRA-8072
        @jira_ticket CASSANDRA-8422
        Test that if we decommission a node, kill it and wipe its data, it can join a cluster with a single
        seed node.
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        # Decommision the new node and kill it
        debug("Decommissioning & stopping node2")
        node2.decommission()
        node2.stop(wait_other_notice=False)

        # Wipe its data
        for data_dir in node2.data_directories():
            debug("Deleting {}".format(data_dir))
            shutil.rmtree(data_dir)

        commitlog_dir = os.path.join(node2.get_path(), 'commitlogs')
        debug("Deleting {}".format(commitlog_dir))
        shutil.rmtree(commitlog_dir)

        # Now start it, it should be allowed to join
        mark = node2.mark_log()
        debug("Restarting wiped node2")
        node2.start(wait_other_notice=False)
        node2.watch_log_for("JOINING:", from_mark=mark)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11281',
                   flaky=True,
                   notes='windows, one fail on linux no-vnode 2.2')
    def failed_bootstrap_wiped_node_can_join_test(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if a node fails to bootstrap, it can join the cluster even if the data is wiped.
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.start(wait_for_binary_proto=True)

        stress_table = 'keyspace1.standard1'

        # write some data, enough for the bootstrap to fail later on
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=100K', 'no-warmup', '-rate', 'threads=8'])
        node1.flush()

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)

        # kill node2 in the middle of bootstrap
        t = KillOnBootstrap(node2)
        t.start()

        node2.start()
        t.join()
        self.assertFalse(node2.is_running())

        # wipe any data for node2
        self._cleanup(node2)
        # Now start it again, it should be allowed to join
        mark = node2.mark_log()
        node2.start(wait_other_notice=True)
        node2.watch_log_for("JOINING:", from_mark=mark)

    @since('2.1.1')
    def simultaneous_bootstrap_test(self):
        """
        Attempt to bootstrap two nodes at once, to assert the second bootstrapped node fails, and does not interfere.

        Start a one node cluster and run a stress write workload.
        Start up a second node, and wait for the first node to detect it has joined the cluster.
        While the second node is bootstrapping, start a third node. This should fail.

        @jira_ticket CASSANDRA-7069
        @jira_ticket CASSANDRA-9484
        """

        bootstrap_error = ("Other bootstrapping/leaving/moving nodes detected,"
                           " cannot bootstrap while cassandra.consistent.rangemovement is true")

        self.ignore_log_patterns.append(bootstrap_error)

        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        node1, = cluster.nodelist()

        node1.stress(['write', 'n=500K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=10'])

        node2 = new_node(cluster)
        node2.start(wait_other_notice=True)

        node3 = new_node(cluster, remote_debug_port='2003')
        process = node3.start(wait_other_notice=False)
        stdout, stderr = process.communicate()
        self.assertIn(bootstrap_error, stderr, msg=stderr)
        time.sleep(.5)
        self.assertFalse(node3.is_running(), msg="Two nodes bootstrapped simultaneously")

        node2.watch_log_for("Starting listening for CQL clients")

        session = self.patient_exclusive_cql_connection(node2)

        # Repeat the select count(*) query, to help catch
        # bugs like 9484, where count(*) fails at higher
        # data loads.
        for _ in xrange(5):
            assert_one(session, "SELECT count(*) from keyspace1.standard1", [500000], cl=ConsistencyLevel.ONE)

    def test_cleanup(self):
        """
        @jira_ticket CASSANDRA-11179
        Make sure we remove processed files during cleanup
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'concurrent_compactors': 4})
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)
        node1, = cluster.nodelist()
        for x in xrange(0, 5):
            node1.stress(['write', 'n=100k', 'no-warmup', '-schema', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)', 'replication(factor=1)', '-rate', 'threads=10'])
            node1.flush()
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        event = threading.Event()
        failed = threading.Event()
        jobs = 1
        thread = threading.Thread(target=self._monitor_datadir, args=(node1, event, len(node1.get_sstables("keyspace1", "standard1")), jobs, failed))
        thread.start()
        node1.nodetool("cleanup -j {} keyspace1 standard1".format(jobs))
        event.set()
        thread.join()
        self.assertFalse(failed.is_set())

    def _monitor_datadir(self, node, event, basecount, jobs, failed):
        while True:
            sstables = [s for s in node.get_sstables("keyspace1", "standard1") if "tmplink" not in s]
            debug("---")
            for sstable in sstables:
                debug(sstable)
            if len(sstables) > basecount + jobs:
                debug("Current count is {}, basecount was {}".format(len(sstables), basecount))
                failed.set()
                return
            if event.is_set():
                return
            time.sleep(.1)

    def _cleanup(self, node):
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        for data_dir in node.data_directories():
            debug("Deleting {}".format(data_dir))
            shutil.rmtree(data_dir)
        shutil.rmtree(commitlog_dir)
