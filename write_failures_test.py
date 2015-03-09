import os
import stat
import glob
#import time
#import platform
import subprocess
import ccmlib

from dtest import Tester, debug

from cassandra import WriteTimeout
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.cluster import InvalidRequest

from tools import since
from assertions import assert_one, assert_none

KEYSPACE = "foo"
INVALID_MUTATION_ERROR = "Mutation failed with Mutation of"

class TestWriteFailures(Tester):
    """
    Tests for write failures in the replicas, 
    https://issues.apache.org/jira/browse/CASSANDRA-8592.
    """

    def setUp(self):
        super(TestWriteFailures, self).setUp()

        self.ignore_log_patterns = [ INVALID_MUTATION_ERROR]

        conf = {'commitlog_sync_period_in_ms': 1000}
        conf = {'commitlog_segment_size_in_mb': 1}
        
        if self.cluster.version() >= "2.1":
            conf['memtable_heap_space_in_mb'] = 512
        
        self.cluster.set_configuration_options(values=conf)
        self.cluster.populate(3).start()
        
    def tearDown(self):
        super(TestWriteFailures, self).tearDown()

    def test_cql_mutation(self):
        """
        If the replica fails to insert the mutation we should get a WriteFailure
        for protocol version >= 4 and WriteTimeout for earlier versions.
        """

        node1 = self.cluster.nodes.values()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, KEYSPACE, 3)

        session.execute(
            """
                CREATE TABLE users (
                    userid text PRIMARY KEY, 
                    messages list<text>)
            """)

        # We create an insert statement that is more than 50% of commitlog_segment_size_in_mb
        # set above to 1 MB. Therefore this mutation always fails.
        insert_text = "INSERT INTO users (userid, messages) VALUES ('user1', ["

        for i in range(0, 10000):
            insert_text += "'Blah blha blahadfaf adf adlfjaf', "

        insert_text += "'last message'])"

        insert_statement = session.prepare(insert_text)
        insert_statement.consistency_level = ConsistencyLevel.ONE

        with self.assertRaises(WriteTimeout) as cm:
            session.execute(insert_statement)
