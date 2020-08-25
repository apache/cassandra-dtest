import pytest
import logging

from dtest import Tester

since = pytest.mark.since
logger = logging.getLogger(__name__)

VERSION_30 = 'github:apache/cassandra-3.0'
VERSION_311 = 'github:apache/cassandra-3.11'


@since('3.0', max_version='3.11')
class TestDropCompactStorage(Tester):
    """
    Test to verify that dropping compact storage is not possible prior running `nodetool upgradesstables`.
    @CASSANDRA-16063
    """

    def test_drop_compact_storage(self):
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()
        cluster.set_install_dir(version="2.1.14")
        cluster.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        self._insert_data(session)

        logging.debug("Upgrading to current version")
        for node in [node1, node2]:
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

            self.set_node_to_current_version(node)
            node.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        thrown = False
        exception = None
        try:
            session.execute("ALTER TABLE drop_compact_storage_test.test DROP COMPACT STORAGE")
        except Exception as e:
            exception = e
            thrown = True

        assert thrown, "No exception has been thrown"
        assert "Cannot DROP COMPACT STORAGE until all SSTables are upgraded, please run `nodetool upgradesstables` first." in str(exception)

        for node in [node1, node2]:
            node.nodetool("upgradesstables")

        thrown = False
        exception = None
        try:
            session.execute("ALTER TABLE drop_compact_storage_test.test DROP COMPACT STORAGE")
        except Exception as e:
            exception = e
            thrown = True

        assert not thrown, "Exception should not have been thrown: " + str(exception)

    def _insert_data(self, session):
        # Make sure the system_auth table will get replicated to the node that we're going to replace

        session.execute("CREATE KEYSPACE drop_compact_storage_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'};")
        session.execute("CREATE TABLE drop_compact_storage_test.test (a text PRIMARY KEY, b text, c text) WITH COMPACT STORAGE;")

        for i in range(1, 100):
            session.execute("INSERT INTO drop_compact_storage_test.test (a, b, c) VALUES ('{}', '{}', '{}');".format(i, i + 1, i + 2))