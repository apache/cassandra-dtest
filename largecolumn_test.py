import pytest
import re
import logging

from dtest import Tester

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('2.2')
class TestLargeColumn(Tester):
    """
    Check that inserting and reading large columns to the database doesn't cause off heap memory usage
    that is proportional to the size of the memory read/written.
    """

    def stress_with_col_size(self, cluster, node, size):
        size = str(size)
        node.stress(['write', 'n=5', "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=2)", "-col", "n=fixed(1)", "size=fixed(" + size + ")", "-rate", "threads=1"])
        node.stress(['read', 'n=5', "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=2)", "-col", "n=fixed(1)", "size=fixed(" + size + ")", "-rate", "threads=1"])

    def directbytes(self, node):
        def is_number(s):
            try:
                float(s)
                return True
            except ValueError:
                return False

        output, err, _ = node.nodetool("gcstats")
        logger.debug(output)
        output = output.split("\n")
        assert re.search('Interval', output[0].strip())
        fields = output[1].split()
        assert len(fields) >= 6, "Expected output from nodetool gcstats has at least six fields. However >= fields is: {}".format(fields)
        for field in fields:
            assert is_number(field.strip()) or field == 'NaN', "Expected numeric from fields from nodetool gcstats. However, field.strip() is: {}".format(field.strip())
        return fields[6]

    def test_cleanup(self):
        """
        @jira_ticket CASSANDRA-8670
        """
        cluster = self.cluster
        # Commit log segment size needs to increase for the database to be willing to accept columns that large
        # internode compression is disabled because the regression being tested occurs in NIO buffer pooling without compression
        configuration = {'commitlog_segment_size_in_mb': 128, 'internode_compression': 'none'}
        if cluster.version() >= '4.0':
            configuration['internode_max_message_size_in_bytes'] = 128 * 1024 * 1024
        if cluster.version() >= '4.1':
            configuration['native_transport_max_request_data_in_flight'] = '64MiB'
            configuration['native_transport_max_request_data_in_flight_per_ip'] = '64MiB'
        cluster.set_configuration_options(configuration)

        # Have Netty allocate memory on heap so it is clear if memory used for large columns is related to intracluster messaging
        # NOTE: we still have direct memory used by Cassandra for networking cache and other places
        cluster.populate(2).start(jvm_args=[" -Dcassandra.netty_use_heap_allocator=true "])
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        logger.info("Before stress, direct memory: {0}".format(self.directbytes(node1)))
        logger.debug("Running stress")
        # Run the full stack to see how much memory is utilized for "small" columns
        self.stress_with_col_size(cluster, node1, 1)
        beforeLargeStress = self.directbytes(node1)
        logger.info("Ran small column stress once, direct memory: {0}".format(beforeLargeStress))

        # Now run the full stack to warm up internal caches/pools
        LARGE_COLUMN_SIZE = 1024 * 1024 * 63
        self.stress_with_col_size(cluster, node1, LARGE_COLUMN_SIZE)
        after1stLargeStress = self.directbytes(node1)
        logger.info("After 1st large column stress, direct memory: {0}".format(after1stLargeStress))

        # Now run the full stack to see how much memory is allocated for the second "large" columns request
        self.stress_with_col_size(cluster, node1, LARGE_COLUMN_SIZE)
        after2ndLargeStress = self.directbytes(node1)
        logger.info("After 2nd large column stress, direct memory: {0}".format(after2ndLargeStress))

        # We may allocate direct memory proportional to size of a request
        # but we want to ensure that when we do subsequent calls the used direct memory is not growing
        diff = int(after2ndLargeStress) - int(after1stLargeStress)
        logger.info("Direct memory delta: {0}".format(diff))
        assert diff < LARGE_COLUMN_SIZE
