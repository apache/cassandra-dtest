from dtest import Tester
from tools import since

import os, sys, time, tools
from uuid import UUID
from ccmlib.cluster import Cluster

cql_version="3.0.0"

class TestCQL(Tester):

    def prepare(self):
        cluster = self.cluster

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.patient_cql_connection(node1, version=cql_version)
        self.create_ks(cursor, 'ks', 1)
        return cursor

    @since("1.2")
    def batch_preparation_test(self):
        """ Test preparation of batch statement (#4202) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE cf (
                k varchar PRIMARY KEY,
                c int,
            )
        """)

        query = "BEGIN BATCH INSERT INTO cf (k, c) VALUES (?, ?); APPLY BATCH";
        pq = cursor.prepare(query);

        cursor.execute(pq, ['foo', 4])
