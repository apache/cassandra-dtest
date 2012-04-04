import time
import os

from dtest import Tester, debug
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib import common as ccmcommon

try:
    TO_BRANCH = os.environ['CASSANDRA_VERSION']
except KeyError:
    TO_BRANCH = 'git:trunk'

class TestCounterUpgrade(Tester):
    """
    demonstrates this issue:
    https://issues.apache.org/jira/browse/CASSANDRA-3804
    """

    def __init__(self, *argv, **kwargs):
        super(TestCounterUpgrade, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True


    def counter_upgrade_test(self):
        cluster = self.cluster

        cluster.set_cassandra_dir(cassandra_version="1.0.8")
#        cluster.set_cassandra_dir(cassandra_version=TO_BRANCH) # Doesn't fail in this case.

        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()
        time.sleep(1)

        debug("Upgrading")
        node1.nodetool('drain')
        node1.stop()
        time.sleep(1)
        node1.set_cassandra_dir(cassandra_version=TO_BRANCH)
        node1.start()
        node1.nodetool('scrub')
        debug("Upgraded, waiting for gossip")
        time.sleep(20) # wait for gossip to notice the node is back
    
    
        debug("Creating columnfamily and adding")
        cursor = self.cql_connection(node2).cursor()
        self.create_ks(cursor, 'ks', 3)
        cursor.execute("CREATE COLUMNFAMILY counters (key varchar PRIMARY KEY) WITH comparator=UTF8Type AND default_validation=CounterColumnType")
        time.sleep(3)

        # Generally this fails before 10 rows have been added.
        for x in xrange(10000):
            debug(x)
            cursor.execute("UPDATE counters SET row = row+1 where key='a'")

