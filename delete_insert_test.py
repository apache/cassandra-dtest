import random
import threading
import uuid
import logging

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester, create_ks

logger = logging.getLogger(__name__)


class TestDeleteInsert(Tester):
    """
    Examines scenarios around deleting data and adding data back with the same key
    """
    # Generate 1000 rows in memory so we can re-use the same ones over again:
    rows = [(str(uuid.uuid1()), x, random.choice(['group1', 'group2', 'group3', 'group4'])) for x in range(1000)]

    def create_ddl(self, session, rf={'dc1': 2, 'dc2': 2}):
        create_ks(session, 'delete_insert_search_test', rf)
        session.execute('CREATE TABLE test (id uuid PRIMARY KEY, val1 text, group text)')
        session.execute('CREATE INDEX group_idx ON test (group)')

    def delete_group_rows(self, session, group):
        """Delete rows from a given group and return them"""
        rows = [r for r in self.rows if r[2] == group]
        ids = [r[0] for r in rows]
        session.execute('DELETE FROM test WHERE id in (%s)' % ', '.join(ids))
        return list(rows)

    def insert_all_rows(self, session):
        self.insert_some_rows(session, self.rows)

    def insert_some_rows(self, session, rows):
        for row in rows:
            session.execute("INSERT INTO test (id, val1, group) VALUES (%s, '%s', '%s')" % row)

    def test_delete_insert_search(self):
        cluster = self.cluster
        cluster.populate([2, 2]).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        session.consistency_level = 'LOCAL_QUORUM'

        self.create_ddl(session)
        # Create 1000 rows:
        self.insert_all_rows(session)
        # Delete all of group2:
        deleted = self.delete_group_rows(session, 'group2')
        # Put that group back:
        self.insert_some_rows(session, rows=deleted)

        # Verify that all of group2 is back, 20 times, in parallel
        # querying across all nodes:

        class ThreadedQuery(threading.Thread):

            def __init__(self, connection):
                threading.Thread.__init__(self)
                self.connection = connection

            def run(self):
                session = self.connection
                query = SimpleStatement("SELECT * FROM delete_insert_search_test.test WHERE group = 'group2'",
                                        consistency_level=ConsistencyLevel.LOCAL_QUORUM)
                rows = session.execute(query)
                assert len(list(rows)) == len(deleted), "Expecting the length of {} to be equal to the " \
                                                        "length of {}.".format(list(rows), deleted)

        threads = []
        for x in range(20):
            conn = self.cql_connection(random.choice(cluster.nodelist()))
            threads.append(ThreadedQuery(conn))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
