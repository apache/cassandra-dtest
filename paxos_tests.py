# coding: utf-8

from dtest import Tester
from assertions import *
from cql import ProgrammingError
from tools import *

import time, threading
from ccmlib.cluster import Cluster

class TestPaxos(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={ 'row_cache_size_in_mb' : 100 })

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version="3.0.0").cursor()
        if create_keyspace:
            self.create_ks(cursor, 'ks', rf)
        return cursor

    def contention_test(self):
        """ Test threads repeatedly contending on the same row """

        verbose = False

        class Worker(threading.Thread):
            def __init__(self, wid, cursor, iterations):
                threading.Thread.__init__(self)
                self.wid = wid
                self.iterations = iterations
                self.cursor = cursor
                self.errors = 0
                self.gaveup = 0
                self.retries = 0

            def run(self):
                i = 0
                prev = 0
                while i < self.iterations:
                    done = False
                    tries = 0
                    while not done:
                        try:

                            self.cursor.execute("UPDATE test SET v = %d WHERE k = 0 IF v = %d" % (prev+1, prev))
                            res = self.cursor.fetchall()[0]
                            if verbose:
                                print "[%d]" % self.wid, "CAS", prev, "->", prev+1, ":", str(res)
                            if res[0] is True:
                                done = True
                                prev = prev + 1
                            else:
                                # The correct behavior is to always retry if we've been beaten. But
                                # in practice we put some high value of retry before gaving up just
                                # to make sure there is not something wrong that makes one thread starve
                                # to death anormally
                                if tries >= 30:
                                    if verbose:
                                        print "[%d]" % self.wid, "Too much retries, skipping iteration"
                                    self.gaveup = self.gaveup + 1
                                    done = True
                                else:
                                    tries = tries + 1 
                                    self.retries = self.retries + 1
                                    prev = res[1]
                        except Exception as e:
                            self.errors = self.errors + 1
                            done = True
                    i = i + 1


        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        cursor.execute("INSERT INTO test(k, v) VALUES (0, 0)");

        N = 8
        I = 100
        nodes = self.cluster.nodelist()
        workers = []
        for n in range(0, N):
            c = self.cql_connection(nodes[n % len(nodes)], version="3.0.0").cursor()
            c.execute("USE ks")
            workers.append(Worker(n, c, I))

        start = time.time()

        for w in workers:
            w.start()

        for w in workers:
            w.join()

        if verbose:
            runtime = time.time() - start
            print "runtime:", runtime

        cursor.execute("SELECT v FROM test WHERE k = 0", consistency_level='ALL')
        value = cursor.fetchall()[0][0]

        errors = 0
        retries = 0
        gaveup = 0
        for w in workers:
            errors = errors + w.errors
            retries = retries + w.retries
            gaveup = gaveup + w.gaveup

        assert (value == N * I) and (errors == 0) and (gaveup == 0), "value=%d, errors=%d, gaveup=%d, retries=%d" % (value, errors, gaveup, retries)

