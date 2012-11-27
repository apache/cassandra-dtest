from dtest import Tester
from assertions import *
from tools import *
import time

class TestSchema(Tester):

    @require('#3919')
    def remove_columndef_test(self):
        """ Remove a column definition while a node is dead """

        cluster = self.cluster
        cluster.populate(2).start()
        nodes = cluster.nodelist()
        time.sleep(.3)

        cursor = self.cql_connection(nodes[0]).cursor()
        self.create_ks(cursor, 'ks', 3)

        cursor.execute("""
            CREATE TABLE cf (
                key int PRIMARY KEY,
                c1 int,
                c2 int
            )
        """)

        # Shutdown a node and do an update
        nodes[1].stop()
        cursor.execute("ALTER TABLE cf DROP c2")
        nodes[0].compact() # compact the schema CF
        time.sleep(.5)

        # Restart the dead node and do a new (trivial) update to make sure the
        # schema are in agreement
        nodes[1].start()
        time.sleep(.5)
        cursor.execute("ALTER TABLE cf ADD c3 int")

        # Check the schema don't reference c2 anymore
        # (use the cli to be compatible with 1.0)
        cli = nodes[0].cli()
        cli.do("use ks")
        cli.do("describe ks")
        assert "Column Name: c2" not in cli.last_output(), cli.last_output()
