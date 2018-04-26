import os
import time
import pytest
import parse
import re
import logging

import ccmlib.common
from ccmlib.node import ToolError

from dtest import Tester
from tools.jmxutils import (JolokiaAgent, enable_jmx_ssl, make_mbean,
                            remove_perf_disable_shared_mem)
from tools.misc import generate_ssl_stores

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestJMX(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            r'Failed to properly handshake with peer.* Closing the channel'
        )

    def test_netstats(self):
        """
        Check functioning of nodetool netstats, especially with restarts.
        @jira_ticket CASSANDRA-8122, CASSANDRA-6577
        """
        #
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=500K', 'no-warmup', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stop(gently=False)

        with pytest.raises(ToolError, message="ConnectException: 'Connection refused( \(Connection refused\))?'."):
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
                assert 'java.lang.reflect.UndeclaredThrowableException' not in str(e), \
                    'Netstats failed with UndeclaredThrowableException (CASSANDRA-8122)'
                if not isinstance(e, ToolError):
                    raise
                else:
                    assert re.search("ConnectException: 'Connection refused( \(Connection refused\))?'.", repr(e))

        assert running, 'node1 never started'

    def test_table_metric_mbeans(self):
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

        typeName = "ColumnFamily" if version < '3.0' else 'Table'
        logger.debug('Version {} typeName {}'.format(version, typeName))

        # TODO the keyspace and table name are capitalized in 2.0
        memtable_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1',
                                   name='AllMemtablesHeapSize')
        disk_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1',
                               name='LiveDiskSpaceUsed')
        sstable_count = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1',
                                   name='LiveSSTableCount')

        with JolokiaAgent(node1) as jmx:
            mem_size = jmx.read_attribute(memtable_size, "Value")
            assert int(mem_size) > 10000

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            assert int(on_disk_size) == 0

            node1.flush()

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            assert int(on_disk_size) > 10000

            sstables = jmx.read_attribute(sstable_count, "Value")
            assert int(sstables) >= 1

    @since('3.0')
    def test_mv_metric_mbeans_release(self):
        """
        Test that the right mbeans are created and released when creating mvs
        """
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)

        node.run_cqlsh(cmds="""
            CREATE KEYSPACE mvtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 1 };
            CREATE TABLE mvtest.testtable (
                foo int,
                bar text,
                baz text,
                PRIMARY KEY (foo, bar)
            );

            CREATE MATERIALIZED VIEW mvtest.testmv AS
                SELECT foo, bar, baz FROM mvtest.testtable WHERE
                foo IS NOT NULL AND bar IS NOT NULL AND baz IS NOT NULL
            PRIMARY KEY (foo, bar, baz);""")

        table_memtable_size = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testtable',
                                         name='AllMemtablesHeapSize')
        table_view_read_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testtable',
                                          name='ViewReadTime')
        table_view_lock_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testtable',
                                          name='ViewLockAcquireTime')
        mv_memtable_size = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testmv',
                                      name='AllMemtablesHeapSize')
        mv_view_read_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testmv',
                                       name='ViewReadTime')
        mv_view_lock_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testmv',
                                       name='ViewLockAcquireTime')

        missing_metric_message = "Table metric %s should have been registered after creating table %s" \
                                 "but wasn't!"

        with JolokiaAgent(node) as jmx:
            assert jmx.read_attribute(table_memtable_size, "Value") is not None, \
                missing_metric_message.format("AllMemtablesHeapSize", "testtable")
            assert jmx.read_attribute(table_view_read_time, "Count") is not None, \
                missing_metric_message.format("ViewReadTime", "testtable")
            assert jmx.read_attribute(table_view_lock_time, "Count") is not None, \
                missing_metric_message.format("ViewLockAcquireTime", "testtable")
            assert jmx.read_attribute(mv_memtable_size, "Value") is not None, \
                missing_metric_message.format("AllMemtablesHeapSize", "testmv")
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=mv_view_read_time, attribute="Count", verbose=False)
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=mv_view_lock_time, attribute="Count", verbose=False)

        node.run_cqlsh(cmds="DROP KEYSPACE mvtest;")
        with JolokiaAgent(node) as jmx:
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=table_memtable_size, attribute="Value", verbose=False)
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=table_view_lock_time, attribute="Count", verbose=False)
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=table_view_read_time, attribute="Count", verbose=False)
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=mv_memtable_size, attribute="Value", verbose=False)
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=mv_view_lock_time, attribute="Count", verbose=False)
            with pytest.raises(Exception, match=".*InstanceNotFoundException.*"):
                jmx.read_attribute(mbean=mv_view_read_time, attribute="Count", verbose=False)

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
            var = 'Compaction@{uuid}(keyspace1, standard1, {progress}/{total})bytes'
            progress = int(parse.search(var, progress_string).named['progress'])
            updated_progress = int(parse.search(var, updated_progress_string).named['progress'])

            logger.debug(progress_string)
            logger.debug(updated_progress_string)

            # We want to make sure that the progress is increasing,
            # and that values other than zero are displayed.
            assert updated_progress > progress
            assert progress >= 0
            assert updated_progress > 0

            # Block until the major compaction is complete
            # Otherwise nodetool will throw an exception
            # Give a timeout, in case compaction is broken
            # and never ends.
            start = time.time()
            max_query_timeout = 600
            logger.debug("Waiting for compaction to finish:")
            while (len(jmx.read_attribute(compaction_manager, 'CompactionSummary')) > 0) and (
                    time.time() - start < max_query_timeout):
                logger.debug(jmx.read_attribute(compaction_manager, 'CompactionSummary'))
                time.sleep(2)

    @since('2.2')
    def test_phi(self):
        """
        Check functioning of nodetool failuredetector.
        @jira_ticket CASSANDRA-9526
        """
        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        stdout = node1.nodetool("failuredetector").stdout
        phivalues = stdout.splitlines()
        endpoint1values = phivalues[1].split()
        endpoint2values = phivalues[2].split()

        endpoint1 = endpoint1values[0][1:-1]
        endpoint2 = endpoint2values[0][1:-1]

        assert '127.0.0.2' in [endpoint1, endpoint2]
        assert '127.0.0.3' in [endpoint1, endpoint2]

        endpoint1phi = float(endpoint1values[1])
        endpoint2phi = float(endpoint2values[1])

        max_phi = 2.0
        assert endpoint1phi > 0.0
        assert endpoint1phi < max_phi

        assert endpoint2phi > 0.0
        assert endpoint2phi < max_phi

    @since('4.0')
    def test_set_get_batchlog_replay_throttle(self):
        """
        @jira_ticket CASSANDRA-13614

        Test that batchlog replay throttle can be set and get through JMX
        """
        cluster = self.cluster
        cluster.populate(2)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node)
        cluster.start()

        # Set and get throttle with JMX, ensuring that the rate change is logged
        with JolokiaAgent(node) as jmx:
            mbean = make_mbean('db', 'StorageService')
            jmx.write_attribute(mbean, 'BatchlogReplayThrottleInKB', 4096)
            assert len(node.grep_log('Updating batchlog replay throttle to 4096 KB/s, 2048 KB/s per endpoint',
                                     filename='debug.log')) > 0
            assert 4096 == jmx.read_attribute(mbean, 'BatchlogReplayThrottleInKB')


@since('3.9')
class TestJMXSSL(Tester):
    keystore_password = 'cassandra'
    truststore_password = 'cassandra'

    def truststore(self):
        return os.path.join(self.fixture_dtest_setup.test_path, 'truststore.jks')

    def keystore(self):
        return os.path.join(self.fixture_dtest_setup.test_path, 'keystore.jks')

    def test_jmx_connection(self):
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

    def test_require_client_auth(self):
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
        with pytest.raises(ToolError, match=".*SSLHandshakeException.*"):
            node.nodetool("info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd}"
                          .format(ts=self.truststore(), ts_pwd=self.truststore_password))

        # when both truststore and a keystore containing the client key are supplied, connection should succeed
        node.nodetool(
            "info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd} -Djavax.net.ssl.keyStore={ks} -Djavax.net.ssl.keyStorePassword={ks_pwd}"
            .format(ts=self.truststore(), ts_pwd=self.truststore_password, ks=self.keystore(),
                    ks_pwd=self.keystore_password))

    def assert_insecure_connection_rejected(self, node):
        """
        Attempts to connect to JMX (via nodetool) without any client side ssl parameters, expecting failure
        """
        with pytest.raises(ToolError):
            node.nodetool("info")

    def _populateCluster(self, require_client_auth=False):
        cluster = self.cluster
        cluster.populate(1)

        generate_ssl_stores(self.fixture_dtest_setup.test_path)
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
