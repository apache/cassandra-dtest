import os
import re
import time

import ccmlib.common
from ccmlib.node import ToolError

from dtest import Tester, debug
from jmxutils import JolokiaAgent, enable_jmx_ssl, make_mbean, remove_perf_disable_shared_mem
from tools import known_failure, since, generate_ssl_stores


class TestJMX(Tester):

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-10915',
                   flaky=False,
                   notes='Fails on Windows, flaps on Linux')
    def netstats_test(self):
        """
        Check functioning of nodetool netstats, especially with restarts.
        @jira_ticket CASSANDRA-8122, CASSANDRA-6577
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=500K', 'no-warmup', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stop(gently=False)

        with self.assertRaisesRegexp(ToolError, "ConnectException: 'Connection refused'."):
            node1.nodetool('netstats')

        # don't wait; we're testing for when nodetool is called on a node mid-startup
        node1.start(wait_for_binary_proto=False)

        # until the binary interface is available, try `nodetool netstats`
        binary_interface = node1.network_interfaces['binary']
        time_out_at = time.time() + 30
        running = False
        while (not running and time.time() <= time_out_at):
            running = ccmlib.common.check_socket_listening(binary_interface, timeout=0.5)
            try:
                node1.nodetool('netstats')
            except Exception as e:
                self.assertNotIn('java.lang.reflect.UndeclaredThrowableException', str(e),
                                 'Netstats failed with UndeclaredThrowableException (CASSANDRA-8122)')
                if not isinstance(e, ToolError):
                    raise
                else:
                    self.assertIn("ConnectException: 'Connection refused'.", str(e))

        self.assertTrue(running, msg='node1 never started')

    def table_metric_mbeans_test(self):
        """
        Test some basic table metric mbeans with simple writes.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        version = cluster.version()
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])

        typeName = "ColumnFamily" if version <= '2.2.X' else 'Table'
        debug('Version {} typeName {}'.format(version, typeName))

        # TODO the keyspace and table name are capitalized in 2.0
        memtable_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='AllMemtablesHeapSize')
        disk_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='LiveDiskSpaceUsed')
        sstable_count = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='LiveSSTableCount')

        with JolokiaAgent(node1) as jmx:
            mem_size = jmx.read_attribute(memtable_size, "Value")
            self.assertGreater(int(mem_size), 10000)

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            self.assertEquals(int(on_disk_size), 0)

            node1.flush()

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            self.assertGreater(int(on_disk_size), 10000)

            sstables = jmx.read_attribute(sstable_count, "Value")
            self.assertGreaterEqual(int(sstables), 1)

    def test_compactionstats(self):
        """
        @jira_ticket CASSANDRA-10504
        @jira_ticket CASSANDRA-10427

        Test that jmx MBean used by nodetool compactionstats
        properly updates the progress of a compaction
        """

        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)

        # Run a quick stress command to create the keyspace and table
        node.stress(['write', 'n=1', 'no-warmup'])
        # Disable compaction on the table
        node.nodetool('disableautocompaction keyspace1 standard1')
        node.nodetool('setcompactionthroughput 1')
        node.stress(['write', 'n=150K', 'no-warmup'])
        node.flush()
        # Run a major compaction. This will be the compaction whose
        # progress we track.
        node.nodetool_process('compact')
        # We need to sleep here to give compaction time to start
        # Why not do something smarter? Because if the bug regresses,
        # we can't rely on jmx to tell us that compaction started.
        time.sleep(5)

        compaction_manager = make_mbean('db', type='CompactionManager')
        with JolokiaAgent(node) as jmx:
            progress_string = jmx.read_attribute(compaction_manager, 'CompactionSummary')[0]

            # Pause in between reads
            # to allow compaction to move forward
            time.sleep(2)

            updated_progress_string = jmx.read_attribute(compaction_manager, 'CompactionSummary')[0]

            progress = int(re.search('standard1, (\d+)\/', progress_string).groups()[0])
            updated_progress = int(re.search('standard1, (\d+)\/', updated_progress_string).groups()[0])

            debug(progress_string)
            debug(updated_progress_string)

            # We want to make sure that the progress is increasing,
            # and that values other than zero are displayed.
            self.assertGreater(updated_progress, progress)
            self.assertGreaterEqual(progress, 0)
            self.assertGreater(updated_progress, 0)

            # Block until the major compaction is complete
            # Otherwise nodetool will throw an exception
            # Give a timeout, in case compaction is broken
            # and never ends.
            start = time.time()
            max_query_timeout = 600
            debug("Waiting for compaction to finish:")
            while (len(jmx.read_attribute(compaction_manager, 'CompactionSummary')) > 0) and (time.time() - start < max_query_timeout):
                debug(jmx.read_attribute(compaction_manager, 'CompactionSummary'))
                time.sleep(2)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11241',
                   flaky=False,
                   notes='windows')
    @since('2.2')
    def phi_test(self):
        """
        Check functioning of nodetool failuredetector.
        @jira_ticket CASSANDRA-9526
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        phivalues = node1.nodetool("failuredetector").stdout.splitlines()
        endpoint1Values = phivalues[1].split()
        endpoint2Values = phivalues[2].split()

        endpoint1 = endpoint1Values[0][1:-1]
        endpoint2 = endpoint2Values[0][1:-1]

        self.assertItemsEqual([endpoint1, endpoint2], ['127.0.0.2', '127.0.0.3'])

        endpoint1Phi = float(endpoint1Values[1])
        endpoint2Phi = float(endpoint2Values[1])

        max_phi = 2.0
        self.assertGreater(endpoint1Phi, 0.0)
        self.assertLess(endpoint1Phi, max_phi)

        self.assertGreater(endpoint2Phi, 0.0)
        self.assertLess(endpoint2Phi, max_phi)


@since('3.9')
class TestJMXSSL(Tester):

    keystore_password = 'cassandra'
    truststore_password = 'cassandra'

    def truststore(self):
        return os.path.join(self.test_path, 'truststore.jks')

    def keystore(self):
        return os.path.join(self.test_path, 'keystore.jks')

    def jmx_connection_test(self):
        """
        Check connecting with a JMX client (via nodetool) where SSL is enabled for JMX
        @jira_ticket CASSANDRA-12109
        """
        cluster = self._populateCluster(require_client_auth=False)
        node = cluster.nodelist()[0]
        cluster.start()

        self.assert_insecure_connection_rejected(node)

        node.nodetool("info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd}"
                      .format(ts=self.truststore(), ts_pwd=self.truststore_password))

    def require_client_auth_test(self):
        """
        Check connecting with a JMX client (via nodetool) where SSL is enabled and
        client certificate auth is also configured
        @jira_ticket CASSANDRA-12109
        """
        cluster = self._populateCluster(require_client_auth=True)
        node = cluster.nodelist()[0]
        cluster.start()

        self.assert_insecure_connection_rejected(node)

        # specifying only the truststore containing the server cert should fail
        with self.assertRaisesRegexp(ToolError, ".*SSLHandshakeException.*"):
            node.nodetool("info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd}"
                          .format(ts=self.truststore(), ts_pwd=self.truststore_password))

        # when both truststore and a keystore containing the client key are supplied, connection should succeed
        node.nodetool("info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd} -Djavax.net.ssl.keyStore={ks} -Djavax.net.ssl.keyStorePassword={ks_pwd}"
                      .format(ts=self.truststore(), ts_pwd=self.truststore_password, ks=self.keystore(), ks_pwd=self.keystore_password))

    def assert_insecure_connection_rejected(self, node):
        """
        Attempts to connect to JMX (via nodetool) without any client side ssl parameters, expecting failure
        """
        with self.assertRaises(ToolError):
            node.nodetool("info")

    def _populateCluster(self, require_client_auth=False):
        cluster = self.cluster
        cluster.populate(1)

        generate_ssl_stores(self.test_path)
        if require_client_auth:
            ts = self.truststore()
            ts_pwd = self.truststore_password
        else:
            ts = None
            ts_pwd = None

        enable_jmx_ssl(cluster.nodelist()[0],
                       require_client_auth=require_client_auth,
                       keystore=self.keystore(),
                       keystore_password=self.keystore_password,
                       truststore=ts,
                       truststore_password=ts_pwd)
        return cluster
