import pytest
import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester

since = pytest.mark.since


@since('4.0')
class TestPreviewRepair(Tester):

    def assert_no_repair_history(self, session):
        rows = session.execute("select * from system_distributed.repair_history")
        assert rows.current_rows == []
        rows = session.execute("select * from system_distributed.parent_repair_history")
        assert rows.current_rows == []

    @pytest.mark.no_vnodes
    def test_preview(self):
        """ Test that preview correctly detects out of sync data """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        # everything should be in sync
        result = node1.repair(options=['ks', '--preview'])
        assert "Previewed data was in sync" in result.stdout
        self.assert_no_repair_history(session)

        # make data inconsistent between nodes
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))
        node3.flush()
        time.sleep(1)
        node3.stop(gently=False)
        stmt.consistency_level = ConsistencyLevel.QUORUM

        session = self.exclusive_cql_connection(node1)
        for i in range(10):
            session.execute(stmt, (i + 10, i + 10))
        node1.flush()
        time.sleep(1)
        node1.stop(gently=False)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.exclusive_cql_connection(node2)
        for i in range(10):
            session.execute(stmt, (i + 20, i + 20))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # data should not be in sync for full and unrepaired previews
        result = node1.repair(options=['ks', '--preview'])
        assert "Total estimated streaming" in result.stdout
        assert "Previewed data was in sync" not in result.stdout

        result = node1.repair(options=['ks', '--preview', '--full'])
        assert "Total estimated streaming" in result.stdout
        assert "Previewed data was in sync" not in result.stdout

        # repaired data should be in sync anyway
        result = node1.repair(options=['ks', '--validate'])
        assert "Repaired data is in sync" in result.stdout

        self.assert_no_repair_history(session)

        # repair the data...
        node1.repair(options=['ks'])
        for node in cluster.nodelist():
            node.nodetool('compact ks tbl')

        # ...and everything should be in sync
        result = node1.repair(options=['ks', '--preview'])
        assert "Previewed data was in sync" in result.stdout

        result = node1.repair(options=['ks', '--preview', '--full'])
        assert "Previewed data was in sync" in result.stdout

        result = node1.repair(options=['ks', '--validate'])
        assert "Repaired data is in sync" in result.stdout
