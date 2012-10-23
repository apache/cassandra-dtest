from dtest import Tester
from assertions import *
from tools import *

import time

class TestCounters(Tester):

    def simple_increment_test(self):
        """ Simple incrementation test (Created for #3465, that wasn't a bug) """
        cluster = self.cluster

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        cursor = self.cql_connection(nodes[0]).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', validation="CounterColumnType", columns={'c': 'counter'})
        cursor.close()


        cursors = [ self.cql_connection(node, 'ks').cursor() for node in nodes ]
        nb_increment=50
        nb_counter=10

        for i in xrange(0, nb_increment):
            for c in xrange(0, nb_counter):
                cursor = cursors[(i + c) % len(nodes)]
                if cluster.version() >= '1.2':
                    cursor.execute("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level='QUORUM')
                else:
                    cursor.execute("UPDATE cf USING CONSISTENCY QUORUM SET c = c + 1 WHERE key = 'counter%i'" % c)

            cursor = cursors[i % len(nodes)]
            keys = ",".join(["'counter%i'" % c for c in xrange(0, nb_counter)])
            if cluster.version() >= '1.2':
                cursor.execute("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level='QUORUM')
            else:
                cursor.execute("SELECT key, c FROM cf USING CONSISTENCY QUORUM WHERE key IN (%s)" % keys)
            res = cursor.fetchall()
            assert len(res) == nb_counter
            for c in xrange(0, nb_counter):
                assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
                assert res[c][1] == i + 1, "Expecting counter%i = %i, got %i" % (c, i + 1, res[c][0])

    def upgrade_test(self):
        """ Test for bug of #4436 """

        cluster = self.cluster

        cluster.populate(2).start()
        nodes = cluster.nodelist()

        cql_version=None

        cursor = self.cql_connection(nodes[0], version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 2)

        cursor.execute("""
            CREATE TABLE counterTable (
                k int PRIMARY KEY,
                c counter
            ) WITH default_validation='CounterColumnType' AND compression_parameters:sstable_compression='SnappyCompressor'
        """)

        keys = range(0, 100)
        updates = 5000

        def make_updates():
            cursor = self.cql_connection(nodes[0], keyspace='ks', version=cql_version).cursor()
            upd = "UPDATE counterTable SET c = c + 1 WHERE k = %d;"
            #upd = "UPDATE counterTable SET c = c + 1 WHERE k = :k%d;"
            batch = " ".join(["BEGIN BATCH"] + [upd % x for x in keys] + ["APPLY BATCH;"])

            #query = cursor.prepare_query(batch)

            kmap = { "k%d" % i : i for i in keys }
            for i in range(0, updates):
                cursor.execute(batch)
                #cursor.execute_prepared(query, kmap)

        def check(i):
            cursor = self.cql_connection(nodes[0], keyspace='ks', version=cql_version).cursor()
            cursor.execute("SELECT * FROM counterTable")
            assert cursor.rowcount == len(keys), "Expected %d rows, got %d: %s" % (len(keys), cursor.rowcount, str(cursor.fetchall()))
            for row in cursor:
                assert row[1] == i * updates, "Unexpected value %s" % str(row)

        def rolling_restart():
            # Rolling restart
            for i in range(0, 2):
                time.sleep(.2)
                nodes[i].nodetool("drain")
                nodes[i].stop(wait_other_notice=True)
                nodes[i].start(wait_other_notice=True)
                time.sleep(.2)

        make_updates()
        check(1)
        rolling_restart()

        make_updates()
        check(2)
        rolling_restart()

        make_updates()
        check(3)
        rolling_restart()

        check(3)
