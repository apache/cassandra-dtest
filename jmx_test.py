import re
import sys
import time
import unittest

import ccmlib.common
from ccmlib.node import NodetoolError

from dtest import Tester, debug
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem
from tools import known_failure, since


class TestJMX(Tester):

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11415')
    @unittest.skipIf(sys.platform == "win32", 'Skip long tests on Windows')
    def cfhistograms_test(self):
        """
        Test cfhistograms on large and small datasets
        @jira_ticket CASSANDRA-8028
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # issue large stress write to load data into cluster
        node1.stress(['write', 'n=15M', '-schema', 'replication(factor=3)', '-rate', 'threads=50'])
        node1.flush()

        try:
            # TODO the keyspace and table name are capitalized in 2.0
            histogram = node1.nodetool("cfhistograms keyspace1 standard1", capture_output=True)
            error_msg = "Unable to compute when histogram overflowed"
            debug(histogram)
            self.assertFalse(error_msg in histogram)
            self.assertTrue("NaN" not in histogram)

        except Exception as e:
            self.fail("Cfhistograms command failed: " + str(e))

        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}")
        session.execute("CREATE TABLE test.tab(key int primary key, val int);")

        try:
            finalhistogram = node1.nodetool("cfhistograms test tab", capture_output=True)
            debug(finalhistogram)

            error_msg = "Unable to compute when histogram overflowed"
            self.assertFalse(error_msg in finalhistogram)
            correct_error_msg = "No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles"
            self.assertTrue(correct_error_msg in finalhistogram[1])
        except Exception as e:
            debug(finalhistogram)
            self.fail("Cfhistograms command failed: " + str(e))

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

        node1.stress(['write', 'n=500K', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stop(gently=False)

        with self.assertRaisesRegexp(NodetoolError, "ConnectException: 'Connection refused'."):
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
                if not isinstance(e, NodetoolError):
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
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])

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
        node.stress(['write', 'n=1'])
        # Disable compaction on the table
        node.nodetool('disableautocompaction keyspace1 standard1')
        node.nodetool('setcompactionthroughput 1')
        node.stress(['write', 'n=150K'])
        node.flush()
        # Run a major compaction. This will be the compaction whose
        # progress we track.
        node.nodetool('compact', capture_output=False, wait=False)
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

        phivalues = node1.nodetool("failuredetector")[0].splitlines()
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
