import time

from dtest import Tester, create_ks
from tools.decorators import since


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

    def batch_preparation_test(self):
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
