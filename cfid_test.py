import os

from dtest import Tester, create_ks, create_cf


class TestCFID(Tester):

    def cfid_test(self):
        """ Test through adding/dropping cf's that the path to sstables for each cf are unique and formatted correctly """
        cluster = self.cluster

        cluster.populate(1).start(wait_other_notice=True)
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)

        for x in range(0, 5):
            create_cf(session, 'cf', gc_grace=0, key_type='int', columns={'c1': 'int'})
            session.execute('insert into cf (key, c1) values (1,1)')
            session.execute('insert into cf (key, c1) values (2,1)')
            node1.flush()
            session.execute('drop table ks.cf;')

        # get a list of cf directories
        try:
            cfs = os.listdir(node1.get_path() + "/data0/ks")
        except OSError:
            self.fail("Path to sstables not valid.")

        # check that there are 5 unique directories
        self.assertEqual(len(cfs), 5)

        # check that these are in fact column family directories
        for dire in cfs:
            self.assertTrue(dire[0:2] == 'cf')
