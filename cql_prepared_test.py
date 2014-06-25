from dtest import Tester
from assertions import *
from tools import *

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

        cursor = self.patient_cql_connection(node1, version=cql_version).cursor()
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

        query = "BEGIN BATCH INSERT INTO cf (k, c) VALUES (:key, :val); APPLY BATCH";
        pq = cursor.prepare_query(query);

        cursor.execute_prepared(pq, params={'key' : 'foo', 'val' : 4})
