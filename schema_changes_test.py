from dtest import Tester
from ccmlib.cluster import Cluster
from ccmlib.node import Node
import random
import time
import os

def wait(delay=2):
    """
    An abstraction so that the sleep delays can easily be modified.
    """
    delay *= .2
    time.sleep(delay)

class TestSchemaChanges(Tester):

    def prepare_for_changes(self, cursor):
        """
        prepares for schema changes by creating a keyspace and column family.
        """
        # create a keyspace that will be used
        query = """CREATE KEYSPACE ks WITH strategy_class=SimpleStrategy AND 
                strategy_options:replication_factor=2"""
        cursor.execute(query)
        cursor.execute('USE ks')

        # make a keyspace that can be deleted
        query = """CREATE KEYSPACE ks2 WITH strategy_class=SimpleStrategy AND 
                strategy_options:replication_factor=2"""
        cursor.execute(query)

        # create a column family with an index and a row of data
        query = """
            CREATE TABLE cf (
                col1 text PRIMARY KEY,
                col2 text,
                col3 text
            );
        """
        cursor.execute(query)
        wait(1)
        cursor.execute("INSERT INTO cf (col1, col2, col3) VALUES ('a', 'b', 'c');")

        # create an index
        cursor.execute("CREATE INDEX index1 ON cf(col2)")

        # create a column family that can be deleted later.
        query = """
            CREATE TABLE cf2 (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text
            );
        """
        cursor.execute(query)


    def make_schema_changes(self, cursor):
        """
        makes several changes.

        create keyspace
        drop keyspace
        create column family
        drop column family
        update column family
        drop index
        create index (modify column family and add a key)
        """
        cursor.execute('USE ks')
        # drop keyspace
        cursor.execute('DROP KEYSPACE ks2')
        wait(2)

        # create keyspace
        query = """CREATE KEYSPACE ks3 WITH strategy_class=SimpleStrategy AND
                strategy_options:replication_factor=2"""
        cursor.execute(query)

        wait(2)
        # drop column family
        cursor.execute("DROP COLUMNFAMILY cf2")

        wait(2)

        # create column family
        query = """
            CREATE TABLE cf3 (
                col1 uuid PRIMARY KEY,
                col2 text,
                col3 text,
                col4 text
            );
        """
        cursor.execute(query)

        # alter column family
        query = """
            ALTER COLUMNFAMILY cf
            ADD col4 text;
        """
        cursor.execute(query)

        # add index
        cursor.execute("CREATE INDEX index2 ON cf(col3)")

        # remove an index
        cursor.execute("DROP INDEX index1")


    def snapshot_test(self):
        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        wait(1)
        cursor = self.cql_connection(node1).cursor()
        self.prepare_for_changes(cursor)

        wait(2)
        cluster.flush()

        wait(2)
        self.make_schema_changes(cursor)

        cluster.cleanup()



        





