import logging

from dtest import Tester, create_ks, create_cf
from tools.data import putget

logger = logging.getLogger(__name__)


class TestMultiDCPutGet(Tester):

    def test_putget_2dc_rf1(self):
        """ Simple put-get test for 2 DC with one node each (RF=1) [catches #3539] """
        cluster = self.cluster
        cluster.populate([1, 1]).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, 'ks', {'dc1': 1, 'dc2': 1})
        create_cf(session, 'cf')

        putget(cluster, session)

    def test_putget_2dc_rf2(self):
        """ Simple put-get test for 2 DC with 2 node each (RF=2) -- tests cross-DC efficient writes """
        cluster = self.cluster
        cluster.populate([2, 2]).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, 'ks', {'dc1': 2, 'dc2': 2})
        create_cf(session, 'cf')

        putget(cluster, session)
