import random
import subprocess
import time
import tempfile

from dtest import debug, Tester
from tools import new_node, query_c1c2, since, InterruptBootstrap
from assertions import assert_almost_equal
from ccmlib.node import NodeError
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args


class TestBootstrap(Tester):

    def __init__(self, *args, **kwargs):
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

        keys = 10000

        # Create a single node cluster
        cluster.populate(1, tokens=[tokens[0]]).start(wait_other_notice=True)
        node1 = cluster.nodes["node1"]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        # record the size before inserting any of our own data
        empty_size = node1.data_size()

        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        node1.flush()
        node1.compact()
        initial_size = node1.data_size()

        # Reads inserted data all during the boostrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE))

        # Boostraping a new node
        node2 = new_node(cluster, token=tokens[1])
        node2.start(wait_for_binary_proto=True)

        reader.check()
        node1.cleanup()
        node1.compact()
        time.sleep(.5)
        reader.check()

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

        node4 = new_node(cluster)
        node4.start()

        session = self.patient_cql_connection(node4)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        rows = session.execute('select * from %s limit 10' % stress_table)
        assert len(list(rows)) == 10

    @since('3.0')
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
        node3.watch_log_for("Listening for thrift clients...")
        mark = node3.mark_log()
        # check if node3 is still in bootstrap mode
        cursor = self.exclusive_cql_connection(node3)
        rows = cursor.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert len(rows) == 1
        assert rows[0][0] == 'IN_PROGRESS', rows[0][0]
        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start(wait_other_notice=True)
        node3.nodetool('bootstrap resume')
        # check if we skipped already retrieved ranges
        node3.watch_log_for("already available. Skipping streaming.")
        node3.watch_log_for("Resume complete", from_mark=mark)
        rows = cursor.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert rows[0][0] == 'COMPLETED', rows[0][0]

    @since('3.0')
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
        cursor = self.exclusive_cql_connection(node3)
        rows = cursor.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
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

        # Add a new node
        node3 = new_node(cluster, bootstrap=False)
        node3.start()
        node3.repair()
        node1.cleanup()

        # Verify the data
        with tempfile.TemporaryFile(mode='w+') as tmpfile:
            if cluster.version() < "2.1":
                node2.stress(['-o', 'read', '-n', '1000', '-e', 'ALL', '-t', '1'],
                             stdout=tmpfile, stderr=subprocess.STDOUT)
            else:
                node2.stress(['read', 'n=1000', 'cl=ALL', '-rate', 'threads=1',
                              '-pop', 'dist=UNIFORM(1..1000)'],
                             stdout=tmpfile, stderr=subprocess.STDOUT)

            tmpfile.seek(0)
            output = tmpfile.read()

        debug(output)
        failure = output.find("Data returned was not validated")
        self.assertEqual(failure, -1, "Stress failed to validate all data")
