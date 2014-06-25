from dtest import Tester, debug, DISABLE_VNODES
import unittest
from tools import *
from ccmlib.cluster import Cluster
from ccmlib.node import NodeError
import time
from cql import OperationalError
from cql.cassandra.ttypes import UnavailableException
import sys

class NodeUnavailable(Exception):
    pass

class TestJson(Tester):

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread'
        ]
        Tester.__init__(self, *args, **kwargs)

    def json_tools_test(self):

        debug("Starting cluster...")
        cluster = self.cluster
        cluster.populate(1).start()

        # time.sleep(1)

        debug("Version: " + cluster.version())

        debug("Getting nodes...")
        [node1] = cluster.nodelist()

        debug("Getting CQLSH...")
        cursor = self.patient_cql_connection(node1).cursor()

        debug("Inserting data...")
        self.create_ks(cursor, 'Test', 1)

        cursor.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)

        cursor.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('frodo', 'pass@', 'male', 'CA', 1985);")
        cursor.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('sam', '@pass', 'male', 'NY', 1980);")

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()

        self.assertItemsEqual(res,
           [ [ u'frodo', 1985, u'male', u'pass@', u'CA' ],
              [u'sam', 1980, u'male', u'@pass', u'NY' ] ] )

        debug("Flushing and stopping cluster...")
        node1.flush()
        cluster.stop()

        debug("Creating JSON file...")
        # file = open("schema.json", "w")
        # file.write(node1.run_sstable2json())
        # print >> file, node1.run_sstable2json()

        # with open("schema.json", "w") as f:
        #     with f as sys.stdout:
        #         node1.run_sstable2json()
        # sys.stdout = sys.__stdout__

        sys.stdout = open("schema.json", 'w')
        #
        node1.run_sstable2json()
        sys.stdout = sys.__stdout__

        # file.close()
