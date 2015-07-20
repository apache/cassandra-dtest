from dtest import Tester
from tools import putget


class TestMultiDCPutGet(Tester):

    def putget_2dc_rf1_test(self):
        """ Simple put-get test for 2 DC with one node each (RF=1) [catches #3539] """
        cluster = self.cluster
        cluster.populate([1, 1]).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', {'dc1': 1, 'dc2': 1})
        self.create_cf(session, 'cf')

        putget(cluster, session)

    def putget_2dc_rf2_test(self):
        """ Simple put-get test for 2 DC with 2 node each (RF=2) -- tests cross-DC efficient writes """
        cluster = self.cluster
        cluster.populate([2, 2]).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', {'dc1': 2, 'dc2': 2})
        self.create_cf(session, 'cf')

        putget(cluster, session)
