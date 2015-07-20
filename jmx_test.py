from flaky import flaky

from dtest import Tester, debug
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem
from tools import since


class TestJMX(Tester):

    @since('2.1')
    @flaky  # flaps on 2.2
    def cfhistograms_test(self):
        """
        Test cfhistograms on large and small datasets
        @jira_ticket CASSANDRA-8028
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # issue large stress write to load data into cluster
        node1.stress(['write', 'n=15M', '-schema', 'replication(factor=3)'])
        node1.flush()

        try:
            # TODO the keyspace and table name are capitalized in 2.0
            histogram = node1.nodetool("cfhistograms keyspace1 standard1", capture_output=True)
            error_msg = "Unable to compute when histogram overflowed"
            debug(histogram)
            self.assertFalse(error_msg in histogram)
            self.assertTrue("NaN" not in histogram)

        except Exception, e:
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
        except Exception, e:
            debug(finalhistogram)
            self.fail("Cfhistograms command failed: " + str(e))

    @since('2.1')
    def netstats_test(self):
        """
        Check functioning of nodetool netstats, especially with restarts.
        @jira_ticket CASSANDRA-8122, CASSANDRA-6577
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
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

        node1.start(wait_for_binary_proto=True)

        try:
            node1.nodetool("netstats")
        except Exception, e:
            if 'java.lang.reflect.UndeclaredThrowableException' in str(e):
                debug(str(e))
                self.fail("Netstats failed with UndeclaredThrowableException (CASSANDRA-8122)")
            else:
                self.fail(str(e))

    @since('2.1')
    def table_metric_mbeans_test(self):
        """
        Test some basic table metric mbeans with simple writes.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        # TODO the keyspace and table name are capitalized in 2.0
        memtable_size = make_mbean('metrics', type='ColumnFamily', keyspace='keyspace1', scope='standard1', name='AllMemtablesHeapSize')
        disk_size = make_mbean('metrics', type='ColumnFamily', keyspace='keyspace1', scope='standard1', name='LiveDiskSpaceUsed')
        sstable_count = make_mbean('metrics', type='ColumnFamily', keyspace='keyspace1', scope='standard1', name='LiveSSTableCount')

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
