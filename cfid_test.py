from dtest import Tester
from pytools import since
import unittest, os, sys, time
from ccmlib.cluster import Cluster
from ccmlib import common

class TestCFID(Tester):

    @since('2.1')
    def cfid_test(self):
        """ Test through adding/dropping cf's that the path to sstables for each cf are unique and formatted correctly """
        cluster = self.cluster

        cluster.populate(1).start(wait_other_notice=True)
        [node1] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)

        for x in range(0, 5):
            self.create_cf(cursor, 'cf', gc_grace=0, key_type='int', columns={'c1': 'int'})
            cursor.execute('insert into cf (key, c1) values (1,1)')
            cursor.execute('insert into cf (key, c1) values (2,1)')
            node1.flush()
            cursor.execute('drop table ks.cf;')

        #get a list of cf directories
        try:
            cfs = os.listdir(node1.get_path() + "/data/ks")
        except OSError:
            self.fail("Path to sstables not valid.")

        #check that there are 5 unique directories
        self.assertEqual(len(cfs), 5)

        #check that these are in fact column family directories
        for dire in cfs:
            self.assertTrue(dire[0:2] == 'cf')
