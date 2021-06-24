import pytest
import logging

from cassandra import ConsistencyLevel, OperationTimedOut, ReadTimeout
from cassandra.query import SimpleStatement

from dtest import Tester

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestSchemaChanges(Tester):

    @since('2.0')
    def test_friendly_unrecognized_table_handling(self):
        """
        After upgrading one of two nodes, create a new table (which will
        not be propagated to the old node) and check that queries against
        that table result in user-friendly warning logs.
        """
        cluster = self.cluster
        cluster.populate(2)
        cluster.start()

        node1, node2 = cluster.nodelist()
        original_version = node1.get_cassandra_version()
        upgraded_version = None
        if original_version.vstring.startswith('2.0'):
            upgraded_version = 'github:apache/cassandra-2.1'
        elif original_version.vstring.startswith('2.1'):
            upgraded_version = 'github:apache/cassandra-2.2'
        else:
            pytest.skip(msg="This test is only designed to work with 2.0 and 2.1 right now")

        # start out with a major behind the previous version

        # upgrade node1
        node1.stop()
        node1.set_install_dir(version=upgraded_version)
        logger.debug("Set new cassandra dir for %s: %s" % (node1.name, node1.get_install_dir()))

        node1.set_log_level("INFO")
        node1.start()

        session = self.patient_exclusive_cql_connection(node1)
        session.cluster.max_schema_agreement_wait = -1  # don't wait for schema agreement

        logger.debug("Creating keyspace and table")
        session.execute("CREATE KEYSPACE test_upgrades WITH replication={'class': 'SimpleStrategy', "
                        "'replication_factor': '2'}")
        session.execute("CREATE TABLE test_upgrades.foo (a int primary key, b int)")

        pattern = r".*Got .* command for nonexistent table test_upgrades.foo.*"

        try:
            session.execute(SimpleStatement("SELECT * FROM test_upgrades.foo", consistency_level=ConsistencyLevel.ALL))
            pytest.fail("expected failure")
        except (ReadTimeout, OperationTimedOut):
            logger.debug("Checking node2 for warning in log")
            node2.watch_log_for(pattern, timeout=10)

        # non-paged range slice
        try:
            session.execute(SimpleStatement("SELECT * FROM test_upgrades.foo", consistency_level=ConsistencyLevel.ALL,
                                            fetch_size=None))
            pytest.fail("expected failure")
        except (ReadTimeout, OperationTimedOut):
            logger.debug("Checking node2 for warning in log")
            pattern = r".*Got .* command for nonexistent table test_upgrades.foo.*"
            node2.watch_log_for(pattern, timeout=10)

        # single-partition slice
        try:
            for i in range(20):
                session.execute(SimpleStatement("SELECT * FROM test_upgrades.foo WHERE a = %d" % (i,),
                                                consistency_level=ConsistencyLevel.ALL, fetch_size=None))
            pytest.fail("expected failure")
        except (ReadTimeout, OperationTimedOut):
            logger.debug("Checking node2 for warning in log")
            pattern = r".*Got .* command for nonexistent table test_upgrades.foo.*"
            node2.watch_log_for(pattern, timeout=10)
