import pytest

from cassandra import ConsistencyLevel
from dtest import Tester
from tools.assertions import assert_all

since = pytest.mark.since

class TestLegacySSTables(Tester):

    @since('3.0', max_version='4')
    def test_14766(self):
        """
        @jira_ticket CASSANDRA-14766

        A reproduction / regression test to illustrate CASSANDRA-14766: when
        reading a legacy 2.1 sstable with SSTableReversedIterator, it's possible
        to skip and not return the last Unfiltered in the last indexed block.

        It would lead to a missing row, if that Unfiltered was a row, or potentially
        resurrected data, if it's a tombstone.
        """
        cluster = self.cluster

        # set column_index_size_in_kb to 1 for a small reproduction sequence
        cluster.set_configuration_options(values={'column_index_size_in_kb': 1})

        # start with 2.1.20 to generate a legacy sstable
        cluster.set_install_dir(version='2.1.20')

        cluster.populate(1).start()
        self.install_nodetool_legacy_parsing()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        query = "CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};"
        session.execute(query)

        query = 'CREATE TABLE test.test (pk int, ck int, value text, PRIMARY KEY (pk, ck));'
        session.execute(query)

        # insert 4 rows to fill 2 index blocks and flush the 2.1 sstable
        stmt = session.prepare('INSERT INTO test.test (pk, ck, value) VALUES (0, ?, ?);');
        for i in range(0, 4):
            session.execute(stmt, [i, '0' * 512])
        cluster.flush()

        # stop, upgrade to current version (3.0 or 3.11), start up
        node1.stop(wait_other_notice=True)
        self.set_node_to_current_version(node1)
        node1.start()
        session = self.patient_cql_connection(node1)

        # make sure all 4 rows are there when reading backwards
        # prior to the fix, this would return 3 rows (ck = 2, 1, 0), skipping ck = 3
        assert_all(session,
                   "SELECT ck FROM test.test WHERE pk = 0 ORDER BY ck DESC;",
                   [[3], [2], [1], [0]],
                   cl=ConsistencyLevel.ONE)
