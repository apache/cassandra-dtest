import logging

from cassandra import InvalidRequest

from dtest import Tester

logger = logging.getLogger(__name__)

KEYSPACE = "foo"


class TestPreparedStatements(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    def test_dropped_index(self):
        """
        Prepared statements using dropped indexes should be handled correctly
        """
        self.cluster.populate(1).start()
        node = list(self.cluster.nodes.values())[0]

        session = self.patient_cql_connection(node)
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """ % KEYSPACE)

        session.set_keyspace(KEYSPACE)
        session.execute("CREATE TABLE IF NOT EXISTS mytable (a int PRIMARY KEY, b int)")
        session.execute("CREATE INDEX IF NOT EXISTS bindex ON mytable(b)")

        insert_statement = session.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        for i in range(10):
            session.execute(insert_statement, (i, 0))

        query_statement = session.prepare("SELECT * FROM mytable WHERE b=?")
        print("Number of matching rows:", len(list(session.execute(query_statement, (0,)))))

        session.execute("DROP INDEX bindex")

        try:
            print("Executing prepared statement with dropped index...")
            session.execute(query_statement, (0,))
        except InvalidRequest as ir:
            print(ir)
        except Exception:
            raise
