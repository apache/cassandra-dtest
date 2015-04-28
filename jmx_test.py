from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since, InterruptBootstrap
import time
from jmxutils import make_mbean, JolokiaAgent
from multiprocessing import Process

class TestJMX(Tester):

    def cfhistograms_test(self):
        """
        Test cfhistograms on large and small datasets
        @jira_ticket CASSANDRA-8028
        """

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        #issue large stress write to load data into cluster
        node1.stress(['write', 'n=15M', '-schema', 'replication(factor=3)'])
        node1.flush()

        try:
            histogram = node1.nodetool("cfhistograms keyspace1 standard1", capture_output=True)
            error_msg = "Unable to compute when histogram overflowed"
            debug(histogram)
            self.assertFalse(error_msg in histogram)
            self.assertTrue("NaN" not in histogram)

        except Exception, e:
            self.fail("Cfhistograms command failed: " + str(e))

        cursor = self.patient_cql_connection(node1)

        cursor.execute("CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}")
        cursor.execute("CREATE TABLE test.tab(key int primary key, val int);")

        try:
            finalhistogram = node1.nodetool("cfhistograms test tab", capture_output=True)
            debug(finalhistogram)

            error_msg = "Unable to compute when histogram overflowed"
            self.assertFalse(error_msg in finalhistogram)
            correct_error_msg = "No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles"
            self.assertTrue(correct_error_msg in finalhistogram[1])
        except Exception, e:
            debug(finalhistogram)
            self.fail("Cfhistograms command failed: " + str(e))

    def netstats_test(self):
        """
        Check functioning of nodetool netstats, especially with restarts.
        @jira_ticket CASSANDRA-8122, CASSANDRA-6577
        """

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=5M', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stop(gently=False)
        try:
            node1.nodetool("netstats")
        except Exception, e:
            if "ConcurrentModificationException" in str(e):
                self.fail("Netstats failed due to CASSANDRA-6577")
            else:
                debug(str(e))

        node1.start()

        try:
            node1.nodetool("netstats")
        except Exception, e:
            if 'java.lang.reflect.UndeclaredThrowableException' in str(e):
                debug(str(e))
                self.fail("Netstats failed with UndeclaredThrowableException (CASSANDRA-8122)")
            else:
                self.fail(str(e))


    def table_metric_mbeans_test(self):
        """
        Test some basic table metric mbeans with simple writes.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=5000', '-schema', 'replication(factor=3)'])

        memtable_size = make_mbean('metrics', type='ColumnFamily', keyspace='keyspace1', scope='standard1', name='AllMemtablesHeapSize')
        disk_size = make_mbean('metrics', type='ColumnFamily', keyspace='keyspace1', scope='standard1', name='LiveDiskSpaceUsed')
        sstable_count = make_mbean('metrics', type='ColumnFamily', keyspace='keyspace1', scope='standard1', name='LiveSSTableCount')


        with JolokiaAgent(node1) as jmx:
            mem_size = jmx.read_attribute(memtable_size, "Value")
            self.assertGreaterThan(int(mem_size), 10000)

            on_disk_size = jmx.read_attribute(disk_size, "Value")
            self.assertEquals(int(on_disk_size), 0)

            node1.flush()

            on_disk_size = jmx.read_attribute(disk_size, "Value")
            self.assertGreaterThan(int(on_disk_size), 10000)

            sstables = jmx.read_attribute(sstable_count, "Value")
            self.assertGreaterEqual(int(sstables), 1)
