import os
import random
import time
import shutil
import subprocess
import tempfile
import re
from dtest import Tester, debug
from tools import new_node, query_c1c2, since, require, KillOnBootstrap, InterruptBootstrap
from assertions import assert_almost_equal
from ccmlib.node import NodeError
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args


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

    def simple_bootstrap_test(self):
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

        # Reads inserted data all during the boostrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE))

        # Boostraping a new node
        node2 = new_node(cluster)
        node2.set_configuration_options(values={'initial_token': tokens[1]})
        node2.start(wait_for_binary_proto=True)
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

    def read_from_bootstrapped_node_test(self):
        """Test bootstrapped node sees existing data, eg. CASSANDRA-6648"""
        cluster = self.cluster
        cluster.populate(3)
        version = cluster.version()
        cluster.start()

        node1 = cluster.nodes['node1']
        if version < "2.1":
            node1.stress(['-n', '10000'])
        else:
            node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        original_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))

        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node4)
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        self.assertEquals(original_rows, new_rows)

    @since('2.2')
    def resumable_bootstrap_test(self):
        """Test resuming bootstrap after data streaming failure"""

        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        try:
            node3.start()
        except NodeError:
            pass  # node doesn't start as expected
        t.join()

        # wait for node3 ready to query
        node3.watch_log_for("Starting listening for CQL clients")
        mark = node3.mark_log()
        # check if node3 is still in bootstrap mode
        session = self.exclusive_cql_connection(node3)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert len(rows) == 1
        assert rows[0][0] == 'IN_PROGRESS', rows[0][0]
        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start(wait_other_notice=True)
        node3.nodetool('bootstrap resume')
        # check if we skipped already retrieved ranges
        node3.watch_log_for("already available. Skipping streaming.")
        node3.watch_log_for("Resume complete", from_mark=mark)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert rows[0][0] == 'COMPLETED', rows[0][0]

    @since('2.2')
    def bootstrap_with_reset_bootstrap_state_test(self):
        """Test bootstrap with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
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
        session = self.exclusive_cql_connection(node3)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

    def manual_bootstrap_test(self):
        """Test adding a new node and bootstrappig it manually. No auto_bootstrap.
           This test also verify that all data are OK after the addition of the new node.
           eg. CASSANDRA-9022
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)
        (node1, node2) = cluster.nodelist()

        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '-n', '1000', '-l', '2', '-t', '1'])
        else:
            node1.stress(['write', 'n=1000', '-schema', 'replication(factor=2)',
                          '-rate', 'threads=1', '-pop', 'dist=UNIFORM(1..1000)'])

        session = self.patient_exclusive_cql_connection(node2)
        if cluster.version() < "2.1":
            stress_table = '"Keyspace1"."Standard1"'
        else:
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
        """Test that CL local_quorum works while a node is bootstrapping. CASSANDRA-8058"""

        cluster = self.cluster
        cluster.populate([1, 1])
        version = cluster.version()
        cluster.start()

        node1 = cluster.nodes['node1']
        if version < "2.1":
            node1.stress(['-n', '2000000', '-t', '50', '-S', '100',
                          '--replication-strategy', 'NetworkTopologyStrategy',
                          '--strategy-properties', 'dc1:1,dc2:1'])
        else:
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
            stress_config = tempfile.NamedTemporaryFile(mode='w+', delete=False)
            stress_config.write(yaml_config)
            stress_config.close()
            node1.stress(['user', 'profile=' + stress_config.name, 'n=2000000',
                          'ops(insert=1)', '-rate', 'threads=50'])

        node3 = new_node(cluster, data_center='dc2')
        node3.start(no_wait=True)
        time.sleep(3)

        with tempfile.TemporaryFile(mode='w+') as tmpfile:
            if version < "2.1":
                node1.stress(['-o', 'insert', '-n', '500000', '-t', '5', '-e', 'LOCAL_QUORUM', '-K', '2'],
                             stdout=tmpfile, stderr=subprocess.STDOUT)
            else:
                node1.stress(['user', 'profile=' + stress_config.name, 'ops(insert=1)',
                              'n=500000', 'cl=LOCAL_QUORUM',
                              '-rate', 'threads=5',
                              '-errors', 'retries=2'],
                             stdout=tmpfile, stderr=subprocess.STDOUT)
                os.unlink(stress_config.name)

            tmpfile.seek(0)
            output = tmpfile.read()

        debug(output)
        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(output)
        self.assertIsNone(failure, "Error during stress while bootstrapping")

    @require("9765")
    def shutdown_wiped_node_cannot_join_test(self):
        self._wiped_node_cannot_join_test(gently=True)

    @require("9765")
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

        version = cluster.version()
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'

        # write some data
        node1 = cluster.nodelist()[0]
        if version < "2.1":
            node1.stress(['-n', '10000'])
        else:
            node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node2)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Stop the new node and wipe its data
        node2.stop(gently=gently)
        data_dir = os.path.join(node2.get_path(), 'data')
        debug("Deleting {}".format(data_dir))
        shutil.rmtree(data_dir)

        # Now start it, it should not be allowed to join.
        mark = node2.mark_log()
        node2.start(no_wait=True)
        node2.watch_log_for("A node with address /127.0.0.4 already exists, cancelling join", from_mark=mark, timeout=60)

    @require("9765")
    def decommissioned_wiped_node_can_join_test(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we decommission a node and then wipe its data, it can join the cluster.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True)

        version = cluster.version()
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'

        # write some data
        node1 = cluster.nodelist()[0]
        if version < "2.1":
            node1.stress(['-n', '10000'])
        else:
            node1.stress(['write', 'n=10000', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node2)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Decommision the new node and wipe its data
        node2.nodetool('decommission')
        node2.stop(gently=True)
        data_dir = os.path.join(node2.get_path(), 'data')
        debug("Deleting {}".format(data_dir))
        shutil.rmtree(data_dir)

        # Now start it, it should be allowed to join
        mark = node2.mark_log()
        node2.start(wait_other_notice=True)
        node2.watch_log_for("JOINING:", from_mark=mark, timeout=60)

    @require("9765")
    def failed_bootstap_wiped_node_can_join_test(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if a node fails to bootstrap, it can join the cluster even if the data is wiped.
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.start(wait_for_binary_proto=True)

        version = cluster.version()
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'

        # write some data, enough for the bootstrap to fail later on
        node1 = cluster.nodelist()[0]
        if version < "2.1":
            node1.stress(['-n', '100000'])
        else:
            node1.stress(['write', 'n=100000', '-rate', 'threads=8'])
        node1.flush()

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})

        # kill node2 in the middle of bootstrap
        t = KillOnBootstrap(node2)
        t.start()

        node2.start()
        t.join()
        self.assertFalse(node2.is_running())

        # wipe any data for node2
        data_dir = os.path.join(node2.get_path(), 'data')
        debug("Deleting {}".format(data_dir))
        shutil.rmtree(data_dir)

        # Now start it again, it should be allowed to join
        mark = node2.mark_log()
        node2.start(wait_other_notice=True)
        node2.watch_log_for("JOINING:", from_mark=mark, timeout=60)
