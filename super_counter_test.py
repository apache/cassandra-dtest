import time
import logging
import types
import pprint
import hashlib


from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib import common as ccmcommon

from loadmaker import LoadMaker

import pycassa
import pycassa.system_manager as system_manager

# NOTE: with nosetests, use the --nocapture flag to let logging get through.

class TestGlobalRowKeyCache(Tester):

    def functional_test(self):
        """
        Test that save and load work in the situation when you write to
        different CFs. Read 2 or 3 times to make sure the page cache doesn't
        skew the results.
        """

        NUM_SUBCOLS = 100
        NUM_ADDS = 100

        cluster = self.cluster
        cluster.set_cassandra_dir(git_branch='trunk')
#        cluster.set_cassandra_dir(cassandra_version='1.0.7')
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        time.sleep(.5)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        time.sleep(1) # wait for propagation

        sm = system_manager.SystemManager()
        sm.create_column_family('ks', 'cf', super=True, 
                default_validation_class='CounterColumnType')
        
        pool = pycassa.ConnectionPool('ks')
        cf = pycassa.ColumnFamily(pool, 'cf')

        consistency_level = getattr(pycassa.cassandra.ttypes.ConsistencyLevel, 'QUORUM')

        for subcol in xrange(NUM_SUBCOLS):
            for add in xrange(NUM_ADDS):
                cf.add('row_0', 'col_0', super_column='subcol_%d' % subcol, 
                        write_consistency_level=consistency_level)
        time.sleep(5)

        # flush everything and the problem will be mostly corrected.
#        for node in cluster.nodelist():
#            node.flush()

        print "Before restart:"
        for i in xrange(NUM_SUBCOLS):
            print cf.get('row_0', ['col_0'], super_column='subcol_%d'%i, read_consistency_level=consistency_level)['col_0'],
        print

        print "Stopping cluster"
        cluster.stop()
        time.sleep(1)
        print "Starting cluster"
        cluster.start()
        time.sleep(.5)

        pool = pycassa.ConnectionPool('ks')
        cf = pycassa.ColumnFamily(pool, 'cf')

        print "After restart:"
        from_db = []
        for i in xrange(NUM_SUBCOLS):
            val = cf.get('row_0', ['col_0'], super_column='subcol_%d'%i, read_consistency_level=consistency_level)['col_0']
            print val,
            from_db.append(val)
        print

        expected = [NUM_ADDS for i in xrange(NUM_SUBCOLS)]

        if from_db != expected:
            raise Exception("Expected a bunch of the same values out of the db. Got this: " + str(from_db))


