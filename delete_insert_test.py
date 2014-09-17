from dtest import Tester, debug
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import uuid, os, threading, random

class DeleteInsertTest(Tester):
    """
    Examines scenarios around deleting data and adding data back with the same key
    """

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

        # Generate 1000 rows in memory so we can re-use the same ones over again:
        self.groups = ['group1', 'group2', 'group3', 'group4']
        self.rows = [(str(uuid.uuid1()),x,random.choice(self.groups)) for x in range(1000)]

    def create_ddl(self, cursor, rf={'dc1':2, 'dc2':2}):
        self.create_ks(cursor, 'delete_insert_search_test', rf)
        cursor.execute('CREATE TABLE test (id uuid PRIMARY KEY, val1 text, group text)')
        cursor.execute('CREATE INDEX group_idx ON test (group)')

    def delete_all_rows(self, cursor):
        for id, val, group in self.rows:
            cursor.execute("DELETE FROM test WHERE id=%s" % u)

    def delete_group_rows(self, cursor, group):
        """Delete rows from a given group and return them"""
        rows = [r for r in self.rows if r[2]==group]
        ids = [r[0] for r in rows]
        cursor.execute('DELETE FROM test WHERE id in (%s)' % ', '.join(ids))
        return rows

    def insert_all_rows(self, cursor):
        self.insert_some_rows(cursor, self.rows)

    def insert_some_rows(self, cursor, rows):
        for row in rows:
            cursor.execute("INSERT INTO test (id, val1, group) VALUES (%s, '%s', '%s')" % row)

    def delete_insert_search_test(self):
        cluster = self.cluster
        cluster.populate([2,2]).start()
        node1 = cluster.nodelist()[0]

        cursor = self.cql_connection(node1)
        cursor.consistency_level = 'LOCAL_QUORUM'

        self.create_ddl(cursor)
        # Create 1000 rows:
        self.insert_all_rows(cursor)
        # Delete all of group2:
        deleted = self.delete_group_rows(cursor, 'group2')
        # Put that group back:
        self.insert_some_rows(cursor, rows=deleted)

        # Verify that all of group2 is back, 20 times, in parallel
        # querying across all nodes:

        class ThreadedQuery(threading.Thread):
            def __init__(self, connection):
                threading.Thread.__init__(self)
                self.connection = connection

            def run(self):
                cursor = self.connection
                query = SimpleStatement("SELECT * FROM delete_insert_search_test.test WHERE group = 'group2'", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
                rows = cursor.execute(query)
                assert len(rows) == len(deleted)

        threads = []
        for x in range(20):
            conn = self.cql_connection(random.choice(cluster.nodelist()))
            threads.append(ThreadedQuery(conn))
        for t in threads:
            t.start()
        for t in threads:
            t.join()

