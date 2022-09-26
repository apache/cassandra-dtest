import os
import random
import re
import shutil
import tempfile
import threading
import time
import logging
import signal

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib.node import NodeError, TimeoutError, ToolError, Node

import pytest

from dtest import Tester, create_ks, create_cf, data_size, mk_bman_path
from tools.assertions import (assert_almost_equal, assert_bootstrap_state, assert_not_running,
                              assert_one, assert_stderr_clean)
from tools.data import query_c1c2
from tools.intervention import InterruptBootstrap, KillOnBootstrap, KillOnReadyToBootstrap
from tools.misc import new_node, generate_ssl_stores

since = pytest.mark.since
ported_to_in_jvm = pytest.mark.ported_to_in_jvm
logger = logging.getLogger(__name__)


class BootstrapTester(Tester):
    byteman_submit_path_pre_4_0 = mk_bman_path('pre4.0/stream_failure.btm')
    byteman_submit_path_4_0 = mk_bman_path('4.0/stream_failure.btm')

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.allow_log_errors = True
        fixture_dtest_setup.ignore_log_patterns = (
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        )

    def _base_bootstrap_test(self, bootstrap=None, bootstrap_from_version=None,
                             enable_ssl=None):
        def default_bootstrap(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(wait_for_binary_proto=True)
            return node2

        if bootstrap is None:
            bootstrap = default_bootstrap

        cluster = self.cluster

        if enable_ssl:
            logger.debug("***using internode ssl***")
            generate_ssl_stores(self.fixture_dtest_setup.test_path)
            cluster.enable_internode_ssl(self.fixture_dtest_setup.test_path)

        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        logger.debug("[node1, node2] tokens: %r" % (tokens,))

        keys = 10000

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        if bootstrap_from_version:
            logger.debug("starting source node on version {}".format(bootstrap_from_version))
            node1.set_install_dir(version=bootstrap_from_version)
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        # record the size before inserting any of our own data
        empty_size = data_size(node1, 'ks','cf')
        logger.debug("node1 empty size for ks.cf: %s" % float(empty_size))

        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        node1.flush()
        node1.compact()
        initial_size = data_size(node1,'ks','cf')
        logger.debug("node1 size for ks.cf before bootstrapping node2: %s" % float(initial_size))

        # Reads inserted data all during the bootstrap process. We shouldn't
        # get any error
        query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE)
        session.shutdown()

        # Bootstrapping a new node in the current version
        node2 = bootstrap(cluster, tokens[1])
        node2.compact()

        node1.cleanup()
        logger.debug("node1 size for ks.cf after cleanup: %s" % float(data_size(node1,'ks','cf')))
        node1.compact()
        logger.debug("node1 size for ks.cf after compacting: %s" % float(data_size(node1,'ks','cf')))

        logger.debug("node2 size for ks.cf after compacting: %s" % float(data_size(node2,'ks','cf')))

        size1 = float(data_size(node1,'ks','cf'))
        size2 = float(data_size(node2,'ks','cf'))
        assert_almost_equal(size1, size2, error=0.3)
        assert_almost_equal(float(initial_size - empty_size), 2 * (size1 - float(empty_size)))

        assert_bootstrap_state(self, node2, 'COMPLETED')

    @pytest.mark.no_vnodes
    def test_simple_bootstrap_with_ssl(self):
        self._base_bootstrap_test(enable_ssl=True)

    @pytest.mark.no_vnodes
    def test_simple_bootstrap(self):
        self._base_bootstrap_test()

    @pytest.mark.no_vnodes
    def test_bootstrap_on_write_survey(self):
        def bootstrap_on_write_survey_and_join(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(jvm_args=["-Dcassandra.write_survey=true"], wait_for_binary_proto=True)

            assert len(node2.grep_log('Startup complete, but write survey mode is active, not becoming an active ring member.'))
            assert_bootstrap_state(self, node2, 'IN_PROGRESS')

            node2.nodetool("join")
            assert len(node2.grep_log('Leaving write survey mode and joining ring at operator request'))
            return node2

        self._base_bootstrap_test(bootstrap_on_write_survey_and_join)

    def _test_bootstrap_with_compatibility_flag_on(self, bootstrap_from_version):
        def bootstrap_with_compatibility_flag_on(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            # cassandra.force_3_0_protocol_version parameter is needed to allow schema
            # changes during the bootstrapping for upgrades from 3.0.14+ to anything upwards for 3.0.x or 3.x clusters.
            # @jira_ticket CASSANDRA-13004 for detailed context on `cassandra.force_3_0_protocol_version` flag
            node2.start(jvm_args=["-Dcassandra.force_3_0_protocol_version=true"],
                        wait_for_binary_proto=True)
            return node2

        self._base_bootstrap_test(bootstrap_with_compatibility_flag_on,
                                      bootstrap_from_version=bootstrap_from_version)

    @since('3.10')
    @pytest.mark.no_vnodes
    def test_simple_bootstrap_small_keepalive_period(self):
        """
        @jira_ticket CASSANDRA-11841
        Test that bootstrap completes if it takes longer than streaming_socket_timeout_in_ms or
        2*streaming_keep_alive_period_in_secs to receive a single sstable
        """
        cluster = self.cluster
        yaml_opts = {'streaming_keep_alive_period_in_secs': 2}
        if cluster.version() < '4.0':
            yaml_opts['streaming_socket_timeout_in_ms'] = 1000
        cluster.set_configuration_options(values=yaml_opts)

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]

        logger.debug("Setting up byteman on {}".format(node1.name))
        # set up byteman
        node1.byteman_port = '8100'
        node1.import_config_files()

        cluster.start()

        # Create more than one sstable larger than 1MB
        node1.stress(['write', 'n=1K', '-rate', 'threads=8', '-schema',
                      'compaction(strategy=SizeTieredCompactionStrategy, enabled=false)'])
        cluster.flush()

        logger.debug("Submitting byteman script to {} to".format(node1.name))
        # Sleep longer than streaming_socket_timeout_in_ms to make sure the node will not be killed
        node1.byteman_submit([mk_bman_path('stream_5s_sleep.btm')])

        # Bootstraping a new node with very small streaming_socket_timeout_in_ms
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        # Shouldn't fail due to streaming socket timeout timeout
        assert_bootstrap_state(self, node2, 'COMPLETED')

        if cluster.version() < '4.0':
            for node in cluster.nodelist():
                assert node.grep_log('Scheduling keep-alive task with 2s period.', filename='debug.log')
                assert node.grep_log('Sending keep-alive', filename='debug.log')
                assert node.grep_log('Received keep-alive', filename='debug.log')

    def test_simple_bootstrap_nodata(self):
        """
        @jira_ticket CASSANDRA-11010
        Test that bootstrap completes if streaming from nodes with no data
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        # Create a two-node cluster
        cluster.populate(2)
        cluster.start()

        # Bootstrapping a new node
        node3 = new_node(cluster)
        node3.start(wait_for_binary_proto=True)

        assert_bootstrap_state(self, node3, 'COMPLETED')

    def test_schema_removed_nodes(self):
        """
        @jira_ticket CASSANDRA-16577
        Test that nodes can bootstrap after a schema change performed with a node removed
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(2)
        cluster.start()

        node1, node2 = cluster.nodelist()

        node2.decommission(force=cluster.version() > '4')

        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")

        node3 = new_node(cluster)
        node3.start(wait_for_binary_proto=True)

    def test_read_from_bootstrapped_node(self):
        """
        Test bootstrapped node sees existing data
        @jira_ticket CASSANDRA-6648
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
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
        assert original_rows == new_rows

    @since('3.0')
    def test_bootstrap_waits_for_streaming_to_finish(self):
             """
             Test that bootstrap completes and is marked as such after streaming finishes.
             """

             cluster = self.cluster
             cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')

             logger.debug("Create a cluster")
             cluster.populate(1)
             node1 = cluster.nodelist()[0]

             logger.debug("Start node 1")
             node1.start(wait_for_binary_proto=True)

             logger.debug("Insert 10k rows")
             node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8', '-schema', 'replication(factor=2)'])

             logger.debug("Bootstrap node 2 with delay")
             node2 = new_node(cluster, byteman_port='4200')
             node2.update_startup_byteman_script(mk_bman_path('bootstrap_5s_sleep.btm'))
             node2.start(wait_for_binary_proto=True)

             assert_bootstrap_state(self, node2, 'COMPLETED')
             assert node2.grep_log('Bootstrap completed', filename='debug.log')

    def test_consistent_range_movement_true_with_replica_down_should_fail(self):
        self._bootstrap_test_with_replica_down(True)

    def test_consistent_range_movement_false_with_replica_down_should_succeed(self):
        self._bootstrap_test_with_replica_down(False)

    def test_consistent_range_movement_true_with_rf1_should_fail(self):
        self._bootstrap_test_with_replica_down(True, rf=1)

    def test_consistent_range_movement_false_with_rf1_should_succeed(self):
        self._bootstrap_test_with_replica_down(False, rf=1)

    def test_rf_gt_nodes_multidc_should_succeed(self):
        """
        Validating a KS with RF > N on multi DC doesn't break bootstrap
        @jira_ticket CASSANDRA-16296 CASSANDRA-16411
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate([1, 1])
        cluster.start()

        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE k WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : '3'}")

        if cluster.version() >= '4.0':
            warning = 'Your replication factor 3 for keyspace k is higher than the number of nodes 1 for datacenter dc1'
            assert len(node1.grep_log(warning)) == 1
            assert len(node2.grep_log(warning)) == 0

        session.execute("ALTER KEYSPACE k WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : '2'}")
        session.execute("CREATE TABLE k.testgtrfmultidc (KEY text PRIMARY KEY)")
        session.execute("INSERT INTO k.testgtrfmultidc (KEY) VALUES ('test_rf_gt_nodes_multidc_should_succeed')")

        if cluster.version() >= '4.0':
            warning = 'Your replication factor 2 for keyspace k is higher than the number of nodes 1 for datacenter dc1'
            assert len(node1.grep_log(warning)) == 1
            assert len(node2.grep_log(warning)) == 0

        marks = map(lambda n: n.mark_log(), cluster.nodelist())
        node3 = Node(name='node3',
                     cluster=cluster,
                     auto_bootstrap=True,
                     thrift_interface=('127.0.0.3', 9160),
                     storage_interface=('127.0.0.3', 7000),
                     jmx_port='7300',
                     remote_debug_port='0',
                     initial_token=None,
                     binary_interface=('127.0.0.3', 9042))
        cluster.add(node3, is_seed=False, data_center="dc1")
        node3.start(wait_for_binary_proto=True)
        if cluster.version() >= '4.0':
            warning = 'is higher than the number of nodes'
            for (node, mark) in zip(cluster.nodelist(), marks):
                assert len(node.grep_log(warning, from_mark=mark)) == 0

        session3 = self.patient_exclusive_cql_connection(node3)
        assert_one(session3, "SELECT * FROM k.testgtrfmultidc", ["test_rf_gt_nodes_multidc_should_succeed"])

    def _bootstrap_test_with_replica_down(self, consistent_range_movement, rf=2):
        """
        Test to check consistent bootstrap will not succeed when there are insufficient replicas
        @jira_ticket CASSANDRA-11848
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node3_token = None
        # Make token assignment deterministic
        if not self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 1})
            tokens = cluster.balanced_tokens(3)
            logger.debug("non-vnode tokens: %r" % (tokens,))
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
            assert node3.is_running()
            assert_bootstrap_state(self, node3, 'COMPLETED')
        else:
            if consistent_range_movement:
                if cluster.version() < '4.0':
                    node3.watch_log_for("A node required to move the data consistently is down")
                else:
                    node3.watch_log_for("Necessary replicas for strict consistency were removed by source filters")
            else:
                node3.watch_log_for("Unable to find sufficient sources for streaming range")
            assert_not_running(node3)

    @since('2.2')
    def test_resumable_bootstrap(self):
        """
        Test resuming bootstrap after data streaming failure
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(2)

        node1 = cluster.nodes['node1']
        # set up byteman
        node1.byteman_port = '8100'
        node1.import_config_files()

        cluster.start()
        # kill stream to node3 in the middle of streaming to let it fail
        if cluster.version() < '4.0':
            node1.byteman_submit([self.byteman_submit_path_pre_4_0])
        else:
            node1.byteman_submit([self.byteman_submit_path_4_0])
        node1.stress(['write', 'n=1K', 'no-warmup', 'cl=TWO', '-schema', 'replication(factor=2)', '-rate', 'threads=50'])
        cluster.flush()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.start(jvm_args=['-Dcassandra.reset_bootstrap_progress=false'], wait_other_notice=False)

        # let streaming fail as we expect
        node3.watch_log_for('Some data streaming failed')

        # bring back node3 and invoke nodetool bootstrap to resume bootstrapping
        node3.nodetool(BootstrapTester.nodetool_resume_command(cluster))
        node3.wait_for_binary_interface()
        assert_bootstrap_state(self, node3, 'COMPLETED')

        # cleanup to guarantee each node will only have sstables of its ranges
        cluster.cleanup()

        logger.debug("Check data is present")
        # Let's check stream bootstrap completely transferred data
        stdout, stderr, _ = node3.stress(['read', 'n=1k', 'no-warmup', '-schema', 'replication(factor=2)', '-rate', 'threads=8'])

        if stdout is not None:
            assert "FAILURE" not in stdout

    @since('2.2')
    def test_bootstrap_with_reset_bootstrap_state(self):
        """Test bootstrap with resetting bootstrap progress"""
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(2).start()

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
        node3.stop(signal_event=signal.SIGKILL)
        mark = node3.mark_log()
        node3.start(jvm_args=["-Dcassandra.reset_bootstrap_progress=true"])
        # check if we reset bootstrap state
        node3.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query, 180s as the node needs to bootstrap
        node3.wait_for_binary_interface(from_mark=mark, timeout=180)

        # check if 2nd bootstrap succeeded
        assert_bootstrap_state(self, node3, 'COMPLETED')

    def test_manual_bootstrap(self):
        """
            Test adding a new node and bootstrapping it manually. No auto_bootstrap.
            This test also verify that all data are OK after the addition of the new node.
            @jira_ticket CASSANDRA-9022
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(2).start()
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
        assert original_rows == current_rows

    def test_local_quorum_bootstrap(self):
        """
        Test that CL local_quorum works while a node is bootstrapping.
        @jira_ticket CASSANDRA-8058
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
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
            node1.stress(['user', 'profile=' + stress_config.name, 'n=200K', 'no-warmup',
                          'ops(insert=1)', '-rate', 'threads=10'])

            node3 = new_node(cluster, data_center='dc2')
            node3.start(jvm_args=["-Dcassandra.write_survey=true"], no_wait=True)

            node3_seen = False
            for _ in range(30):  # give node3 up to 30 seconds to start
                ntout = node1.nodetool('status').stdout
                if re.search(r'UJ\s+' + node3.ip_addr, ntout):
                    node3_seen = True
                    break
                time.sleep(1)

            assert node3_seen, "expected {} in status:\n{}".format(node3.ip_addr, ntout)

            out, err, _ = node1.stress(['user', 'profile=' + stress_config.name, 'ops(insert=1)',
                                        'n=10k', 'no-warmup', 'cl=LOCAL_QUORUM',
                                        '-rate', 'threads=10',
                                        '-errors', 'retries=2'])
            ntout = node1.nodetool('status').stdout
            assert re.search(r'UJ\s+' + node3.ip_addr, ntout), ntout

        logger.debug(out)
        assert_stderr_clean(err)
        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(str(out))
        assert failure is None, "Error during stress while bootstrapping"

    def test_shutdown_wiped_node_cannot_join(self):
        self._wiped_node_cannot_join_test(gently=True)

    def test_killed_wiped_node_cannot_join(self):
        self._wiped_node_cannot_join_test(gently=False)

    def _wiped_node_cannot_join_test(self, gently):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we stop a node and wipe its data then the node cannot join
        when it is not a seed. Test both a nice shutdown or a forced shutdown, via
        the gently parameter.
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(3)
        cluster.start()

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
        assert original_rows == list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Stop the new node and wipe its data
        node4.stop(gently=gently)
        self._cleanup(node4)
        # Now start it, it should not be allowed to join.
        mark = node4.mark_log()
        node4.start(no_wait=True, wait_other_notice=False)
        node4.watch_log_for("A node with address {} already exists, cancelling join".format(node4.address_for_current_version_slashy()), from_mark=mark)

    def test_decommissioned_wiped_node_can_join(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we decommission a node and then wipe its data, it can join the cluster.
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(3)
        cluster.start()

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
        assert original_rows == list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Decommission the new node and wipe its data
        node4.decommission()
        node4.stop()
        self._cleanup(node4)
        # Now start it, it should be allowed to join
        mark = node4.mark_log()
        node4.start()
        node4.watch_log_for("JOINING:", from_mark=mark)

    def test_decommissioned_wiped_node_can_gossip_to_single_seed(self):
        """
        @jira_ticket CASSANDRA-8072
        @jira_ticket CASSANDRA-8422
        Test that if we decommission a node, kill it and wipe its data, it can join a cluster with a single
        seed node.
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(1)
        cluster.start()

        node1 = cluster.nodelist()[0]
        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)

        if cluster.version() >= '2.2':
            # reduce system_distributed RF to 2 so we don't require forceful decommission
            session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")

        session.execute("ALTER KEYSPACE system_traces WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")

        # Decommision the new node and kill it
        logger.debug("Decommissioning & stopping node2")
        node2.decommission()
        node2.stop(wait_other_notice=False)

        # Wipe its data
        for data_dir in node2.data_directories():
            logger.debug("Deleting {}".format(data_dir))
            shutil.rmtree(data_dir)

        commitlog_dir = os.path.join(node2.get_path(), 'commitlogs')
        logger.debug("Deleting {}".format(commitlog_dir))
        shutil.rmtree(commitlog_dir)

        # Now start it, it should be allowed to join
        mark = node2.mark_log()
        logger.debug("Restarting wiped node2")
        node2.start(wait_other_notice=False)
        node2.watch_log_for("JOINING:", from_mark=mark)

    def test_failed_bootstrap_wiped_node_can_join(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if a node fails to bootstrap, it can join the cluster even if the data is wiped.
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(1)
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.start()

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
        assert not node2.is_running()

        # wipe any data for node2
        self._cleanup(node2)
        # Now start it again, it should be allowed to join
        mark = node2.mark_log()
        node2.start()
        node2.watch_log_for("JOINING:", from_mark=mark)

    @since('3.0')
    @ported_to_in_jvm('4.1')
    def test_node_cannot_join_as_hibernating_node_without_replace_address(self):
        """
        @jira_ticket CASSANDRA-14559
        Test that a node cannot bootstrap without replace_address if a hibernating node exists with that address
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(2)
        # Setting seed node to first node to make sure replaced node is not in own seed list
        cluster.set_configuration_options({
            'seed_provider': [{'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                               'parameters': [{'seeds': '127.0.0.1'}]
                               }]
        })

        cluster.start()

        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]

        replacement_address = node2.address()
        node2.stop()

        jvm_option = 'replace_address'

        logger.debug("Starting replacement node {} with jvm_option '{}={}'".format(replacement_address, jvm_option,
                                                                                   replacement_address))
        replacement_node = Node('replacement', cluster=self.cluster, auto_bootstrap=True,
                                thrift_interface=None, storage_interface=(replacement_address, 7000),
                                jmx_port='7400', remote_debug_port='0', initial_token=None,
                                binary_interface=(replacement_address, 9042))
        cluster.add(replacement_node, False)

        extra_jvm_args = []
        extra_jvm_args.extend(["-Dcassandra.{}={}".format(jvm_option, replacement_address),
                               "-Dcassandra.ring_delay_ms=10000",
                               "-Dcassandra.broadcast_interval_ms=10000"])

        wait_other_notice = False
        wait_for_binary_proto = False

        # Killing node earlier in bootstrap to prevent node making it to 'normal' status.
        t = KillOnReadyToBootstrap(replacement_node)

        t.start()

        replacement_node.start(jvm_args=extra_jvm_args,
                               wait_for_binary_proto=wait_for_binary_proto, wait_other_notice=wait_other_notice)

        t.join()

        logger.debug("Asserting that original replacement node is not running")
        assert not replacement_node.is_running()

        # Assert node is actually in hibernate for test to be accurate.
        logger.debug("Asserting that node is actually in hibernate status for test accuracy")
        assert 'hibernate' in node1.nodetool("gossipinfo").stdout

        extra_jvm_args = []
        extra_jvm_args.extend(["-Dcassandra.ring_delay_ms=10000",
                               "-Dcassandra.broadcast_interval_ms=10000"])

        logger.debug("Starting blind replacement node {}".format(replacement_address))
        blind_replacement_node = Node('blind_replacement', cluster=self.cluster, auto_bootstrap=True,
                                      thrift_interface=None, storage_interface=(replacement_address, 7000),
                                      jmx_port='7400', remote_debug_port='0', initial_token=None,
                                      binary_interface=(replacement_address, 9042))
        cluster.add(blind_replacement_node, False)
        wait_other_notice = False
        wait_for_binary_proto = False

        blind_replacement_node.start(wait_for_binary_proto=wait_for_binary_proto, wait_other_notice=wait_other_notice)

        # Asserting that the new node has correct log entry
        self.assert_log_had_msg(blind_replacement_node, "A node with the same IP in hibernate status was detected", timeout=60)
        # Waiting two seconds to give node a chance to stop in case above assertion is True.
        # When this happens cassandra may not shut down fast enough and the below assertion fails.
        time.sleep(15)
        # Asserting that then new node is not running.
        # This tests the actual expected state as opposed to just checking for the existance of the above error message.
        assert not blind_replacement_node.is_running()


    @since('2.1.1')
    def test_simultaneous_bootstrap(self):
        """
        Attempt to bootstrap two nodes at once, to assert the second bootstrapped node fails, and does not interfere.

        Start a one node cluster and run a stress write workload.
        Start up a second node, and wait for the first node to detect it has joined the cluster.
        While the second node is bootstrapping, start a third node. This should fail.

        @jira_ticket CASSANDRA-7069
        @jira_ticket CASSANDRA-9484
        """

        bootstrap_error = "Other bootstrapping/leaving/moving nodes detected," \
                          " cannot bootstrap while cassandra.consistent.rangemovement is true"

        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(1)
        cluster.start()

        node1, = cluster.nodelist()

        node1.stress(['write', 'n=500K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=10'])

        node2 = new_node(cluster)
        node2.start()

        for _ in range(30):  # wait until node2 shows up
            ntout = node1.nodetool('status').stdout
            if re.search(r'UJ\s+' + node2.ip_addr, ntout):
                break
            time.sleep(0.1)

        node3 = new_node(cluster, remote_debug_port='2003')
        try:
            node3.start(wait_other_notice=False, verbose=False)
        except NodeError:
            pass  # node doesn't start as expected

        time.sleep(.5)
        node2.watch_log_for("Starting listening for CQL clients")

        node3.watch_log_for(bootstrap_error)

        session = self.patient_exclusive_cql_connection(node2)

        # Repeat the select count(*) query, to help catch
        # bugs like 9484, where count(*) fails at higher
        # data loads.
        for _ in range(5):
            assert_one(session, "SELECT count(*) from keyspace1.standard1", [500000], cl=ConsistencyLevel.ONE)

    def test_cleanup(self):
        """
        @jira_ticket CASSANDRA-11179
        Make sure we remove processed files during cleanup
        """
        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.set_configuration_options(values={'concurrent_compactors': 4})
        cluster.populate(1)
        cluster.start()
        node1, = cluster.nodelist()
        for x in range(0, 5):
            node1.stress(['write', 'n=100k', 'no-warmup', '-schema', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)', 'replication(factor=1)', '-rate', 'threads=10'])
            node1.flush()
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)
        event = threading.Event()
        failed = threading.Event()
        jobs = 1
        thread = threading.Thread(target=self._monitor_datadir, args=(node1, event, len(node1.get_sstables("keyspace1", "standard1")), jobs, failed))
        thread.setDaemon(True)
        thread.start()
        node1.nodetool("cleanup -j {} keyspace1 standard1".format(jobs))
        event.set()
        thread.join()
        assert not failed.is_set()

    def _monitor_datadir(self, node, event, basecount, jobs, failed):
        while True:
            sstables = [s for s in node.get_sstables("keyspace1", "standard1") if "tmplink" not in s]
            if len(sstables) > basecount + jobs:
                logger.error("---")
                for sstable in sstables:
                    logger.error(sstable)
                logger.error("Current count is {}, basecount was {}".format(len(sstables), basecount))
                failed.set()
                return
            if event.is_set():
                return
            time.sleep(.1)

    def _cleanup(self, node):
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        for data_dir in node.data_directories():
            logger.debug("Deleting {}".format(data_dir))
            shutil.rmtree(data_dir)
        shutil.rmtree(commitlog_dir)

    @since('2.2')
    @pytest.mark.ported_to_in_jvm # see org.apache.cassandra.distributed.test.BootstrapBinaryDisabledTest
    def test_bootstrap_binary_disabled(self):
        """
        Test binary while bootstrapping and streaming fails.

        This test was ported to jvm-dtest org.apache.cassandra.distributed.test.BootstrapBinaryDisabledTest,
        as of this writing there are a few limitations with jvm-dtest which requries this test to
        stay, namely vnode support (ci also tests under different configs).  Once jvm-dtest supports
        vnodes, this test can go away in favor of that class.

        @jira_ticket CASSANDRA-14526, CASSANDRA-14525, CASSANDRA-16127
        """
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
                  'permissions_validity_in_ms': 0,
                  'roles_validity_in_ms': 0}

        cluster = self.cluster
        cluster.set_environment_variable('CASSANDRA_TOKEN_PREGENERATION_DISABLED', 'True')
        cluster.populate(1)

        node1 = cluster.nodes['node1']
        # set up byteman
        node1.byteman_port = '8100'
        node1.import_config_files()

        cluster.start()
        # kill stream to node2 in the middle of streaming to let it fail
        if cluster.version() < '4.0':
            node1.byteman_submit([self.byteman_submit_path_pre_4_0])
        else:
            node1.byteman_submit([self.byteman_submit_path_4_0])
        node1.stress(['write', 'n=1K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=3)', '-rate', 'threads=50', '-mode', 'native', 'cql3', 'user=cassandra', 'password=cassandra'])
        cluster.flush()

        # start bootstrapping node2 and wait for streaming
        node2 = new_node(cluster)
        node2.set_configuration_options(values=config)
        node2.byteman_port = '8101' # set for when we add node3
        node2.import_config_files()
        node2.start(jvm_args=["-Dcassandra.ring_delay_ms=5000", "-Dcassandra.reset_bootstrap_progress=false"])
        self.assert_log_had_msg(node2, 'Some data streaming failed')

        try:
            node2.nodetool('join')
            pytest.fail('nodetool should have errored and failed to join ring')
        except ToolError as t:
            assert "Cannot join the ring until bootstrap completes" in t.stdout

        node2.nodetool(BootstrapTester.nodetool_resume_command(cluster))
        node2.wait_for_binary_interface()
        assert_bootstrap_state(self, node2, 'COMPLETED', user='cassandra', password='cassandra')

        # Test write survey behaviour
        node3 = new_node(cluster)
        node3.set_configuration_options(values=config)

        # kill stream to node3 in the middle of streaming to let it fail
        if cluster.version() < '4.0':
            node1.byteman_submit([self.byteman_submit_path_pre_4_0])
            node2.byteman_submit([self.byteman_submit_path_pre_4_0])
        else:
            node1.byteman_submit([self.byteman_submit_path_4_0])
            node2.byteman_submit([self.byteman_submit_path_4_0])
        node3.start(jvm_args=["-Dcassandra.write_survey=true", "-Dcassandra.ring_delay_ms=5000", "-Dcassandra.reset_bootstrap_progress=false"])
        self.assert_log_had_msg(node3, 'Some data streaming failed')
        self.assert_log_had_msg(node3, "Not starting client transports in write_survey mode as it's bootstrapping or auth is enabled")

        try:
            node3.nodetool('join')
            pytest.fail('nodetool should have errored and failed to join ring')
        except ToolError as t:
            assert "Cannot join the ring until bootstrap completes" in t.stdout

        node3.nodetool(BootstrapTester.nodetool_resume_command(cluster))
        self.assert_log_had_msg(node3, "Not starting client transports in write_survey mode as it's bootstrapping or auth is enabled")

        # Should succeed in joining
        node3.nodetool('join')
        self.assert_log_had_msg(node3, "Leaving write survey mode and joining ring at operator request")
        assert_bootstrap_state(self, node3, 'COMPLETED', user='cassandra', password='cassandra')
        node3.wait_for_binary_interface()

    @staticmethod
    def nodetool_resume_command(cluster):
        """
        In versions after 4.1, we disable resumable bootstrap by default (see CASSANDRA-17679). In order to run
        nodetool boostrap resume in these subsequent versions we have to manually indicate the intent to allow resumability.
        """
        nt_resume_cmd = 'bootstrap resume'
        if cluster.version() >= '4.2':
            nt_resume_cmd += ' -f'
        return nt_resume_cmd


class TestBootstrap(BootstrapTester):
    """
    This child class is a helper for PyTest to pick up the test methods.
    Since the same test methods are executed as part of upgrade, this helper child class is
    necessary to avoid double execution of the same tests.
    It is necessary to put some dummy expression in this child class.
    """
    pass
