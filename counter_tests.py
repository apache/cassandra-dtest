from time import sleep
from dtest import Tester
from assertions import *
from tools import *

class TestCounters(Tester):

    def simple_increment_test(self):
        """ Simple incrementation test (Created for #3465, that wasn't a bug) """
        cluster = self.cluster

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        cursor = self.cql_connection(nodes[0]).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', validation="CounterColumnType")
        cursor.close()

        cursors = [ self.cql_connection(node, 'ks').cursor() for node in nodes ]
        nb_increment=50
        nb_counter=10

        for i in xrange(0, nb_increment):
            for c in xrange(0, nb_counter):
                cursor = cursors[(i + c) % len(nodes)]
                cursor.execute("UPDATE cf USING CONSISTENCY QUORUM SET c = c + 1 WHERE key = counter%i" % c)

            cursor = cursors[i % len(nodes)]
            keys = ",".join(["'counter%i'" % c for c in xrange(0, nb_counter)])
            cursor.execute("SELECT key, c FROM cf USING CONSISTENCY QUORUM WHERE key IN (%s)" % keys)
            res = cursor.fetchall()
            assert len(res) == nb_counter
            for c in xrange(0, nb_counter):
                assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
                assert res[c][1] == i + 1, "Expecting counter%i = %i, got %i" % (c, i + 1, res[c][0])
