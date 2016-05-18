from dtest import Tester, debug
from tools import known_failure, since


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

        output = node.nodetool("gcstats", capture_output=True)
        debug(output)
        output = output[0].split("\n")
        assert output[0].strip().startswith("Interval"), "Expected output from nodetool gcstats starts with a header line with first column Interval"
        fields = output[1].split()
        assert len(fields) >= 6, "Expected output from nodetool gcstats has at least six fields"
        for field in fields:
            assert is_number(field.strip()) or field == 'NaN', "Expected numeric from fields from nodetool gcstats"
        return fields[6]

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11838',
                   flaky=True,
                   notes='OOM on trunk')
    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11234',
                   flaky=False,
                   notes='windows')
    def cleanup_test(self):
        """
        @jira_ticket CASSANDRA-8670
        """
        cluster = self.cluster
        # Commit log segment size needs to increase for the database to be willing to accept columns that large
        # internode compression is disabled because the regression being tested occurs in NIO buffer pooling without compression
        cluster.set_configuration_options({'commitlog_segment_size_in_mb': 128, 'internode_compression': 'none'})
        # Have Netty allocate memory on heap so it is clear if memory used for large columns is related to intracluster messaging
        cluster.populate(2).start(jvm_args=[" -Dcassandra.netty_use_heap_allocator=true "])
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        debug("Before stress {0}".format(self.directbytes(node1)))
        debug("Running stress")
        # Run the full stack to see how much memory is utilized for "small" columns
        self.stress_with_col_size(cluster, node1, 1)
        beforeStress = self.directbytes(node1)
        debug("Ran stress once {0}".format(beforeStress))

        # Now run the full stack to see how much memory is utilized for "large" columns
        LARGE_COLUMN_SIZE = 1024 * 1024 * 63
        self.stress_with_col_size(cluster, node1, LARGE_COLUMN_SIZE)

        output = node1.nodetool("gcstats", capture_output=True)
        afterStress = self.directbytes(node1)
        debug("After stress {0}".format(afterStress))

        # Any growth in memory usage should not be proportional column size. Really almost no memory should be used
        # since Netty was instructed to use a heap allocator
        diff = int(afterStress) - int(beforeStress)
        self.assertLess(diff, LARGE_COLUMN_SIZE)
