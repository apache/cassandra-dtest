import time
import pytest
import logging

from dtest import Tester, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since("1.2")
class TestCQL(Tester):

    def prepare(self):
        cluster = self.cluster

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        return session

    def test_batch_preparation(self):
        """ Test preparation of batch statement (#4202) """
        session = self.prepare()

        session.execute("""
            CREATE TABLE cf (
                k varchar PRIMARY KEY,
                c int,
            )
        """)

        query = "BEGIN BATCH INSERT INTO cf (k, c) VALUES (?, ?); APPLY BATCH"
        pq = session.prepare(query)

        session.execute(pq, ['foo', 4])
