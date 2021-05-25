import time
import pytest
import logging

from threading import Thread

from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement

from tools.assertions import assert_unavailable
from dtest import Tester, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('2.0.6')
class TestPaxos(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            create_ks(session, 'ks', rf)
        return session

    def test_replica_availability(self):
        """
        @jira_ticket CASSANDRA-8640

        Regression test for a bug (CASSANDRA-8640) that required all nodes to
        be available in order to run LWT queries, even if the query could
        complete correctly with quorum nodes available.
        """
        session = self.prepare(nodes=3, rf=3)
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")

        node1 = self.cluster.nodelist()[1]
        node2 = self.cluster.nodelist()[2]

        node2.stop()
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) IF NOT EXISTS")

        node1.stop()
        assert_unavailable(session.execute, "INSERT INTO test (k, v) VALUES (2, 2) IF NOT EXISTS")

        node1.start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (3, 3) IF NOT EXISTS")

        node2.start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (4, 4) IF NOT EXISTS")

    @pytest.mark.no_vnodes
    def test_cluster_availability(self):
        # Warning, a change in partitioner or a change in CCM token allocation
        # may require the partition keys of these inserts to be changed.
        # This must not use vnodes as it relies on assumed token values.

        session = self.prepare(nodes=3)
        node2 = self.cluster.nodelist()[1]
        node3 = self.cluster.nodelist()[2]
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")

        node3.stop()
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) IF NOT EXISTS")

        node2.stop()
        session.execute("INSERT INTO test (k, v) VALUES (3, 2) IF NOT EXISTS")

        node2.start(wait_for_binary_proto = True)
        session.execute("INSERT INTO test (k, v) VALUES (5, 5) IF NOT EXISTS")

        node3.start(wait_for_binary_proto = True)
        session.execute("INSERT INTO test (k, v) VALUES (6, 6) IF NOT EXISTS")

    def test_contention_multi_iterations(self):
        pytest.skip("Hanging the build")
        self._contention_test(8, 100)

    # Warning, this test will require you to raise the open
    # file limit on OSX. Use 'ulimit -n 1000'
    def test_contention_many_threads(self):
        self._contention_test(250, 1)

    def _contention_test(self, threads, iterations):
        """
        Test threads repeatedly contending on the same row.
        """

        verbose = False

        session = self.prepare(nodes=3)
        session.execute("CREATE TABLE test (k int, v int static, id int, PRIMARY KEY (k, id))")
        session.execute("INSERT INTO test(k, v) VALUES (0, 0)")

        class Worker(Thread):

            def __init__(self, wid, session, iterations, query):
                Thread.__init__(self)
                self.wid = wid
                self.iterations = iterations
                self.query = query
                self.session = session
                self.errors = 0
                self.retries = 0

            def run(self):
                i = 0
                prev = 0
                while i < self.iterations:
                    done = False
                    while not done:
                        try:
                            res = self.session.execute(self.query, (prev + 1, prev, self.wid))
                            if verbose:
                                print("[%3d] CAS %3d -> %3d (res: %s)" % (self.wid, prev, prev + 1, str(res)))
                            if res[0][0] is True:
                                done = True
                                prev = prev + 1
                            else:
                                self.retries = self.retries + 1
                                # There is 2 conditions, so 2 reasons to fail: if we failed because the row with our
                                # worker ID already exists, it means we timeout earlier but our update did went in,
                                # so do consider this as a success
                                prev = res[0][3]
                                if res[0][2] is not None:
                                    if verbose:
                                        print("[%3d] Update was inserted on previous try (res = %s)" % (self.wid, str(res)))
                                    done = True
                        except WriteTimeout as e:
                            if verbose:
                                print("[%3d] TIMEOUT (%s)" % (self.wid, str(e)))
                            # This means a timeout: just retry, if it happens that our update was indeed persisted,
                            # we'll figure it out on the next run.
                            self.retries = self.retries + 1
                        except Exception as e:
                            if verbose:
                                print("[%3d] ERROR: %s" % (self.wid, str(e)))
                            self.errors = self.errors + 1
                            done = True
                    i = i + 1
                    # Clean up for next iteration
                    while True:
                        try:
                            self.session.execute("DELETE FROM test WHERE k = 0 AND id = %d IF EXISTS" % self.wid)
                            break
                        except WriteTimeout as e:
                            pass

        nodes = self.cluster.nodelist()
        workers = []

        
        session = Cluster([nodes[0].ip_addr], connect_timeout=15, idle_heartbeat_interval=0,
                          execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(request_timeout=60)}).connect('ks')
        q = session.prepare("""
                BEGIN BATCH
                   UPDATE test SET v = ? WHERE k = 0 IF v = ?;
                   INSERT INTO test (k, id) VALUES (0, ?) IF NOT EXISTS;
                APPLY BATCH
            """)

        for n in range(0, threads):
            workers.append(Worker(n, session, iterations, q))

        start = time.time()

        for w in workers:
            w.start()

        for w in workers:
            w.join()

        if verbose:
            runtime = time.time() - start
            print("runtime:", runtime)

        query = SimpleStatement("SELECT v FROM test WHERE k = 0", consistency_level=ConsistencyLevel.ALL)
        rows = session.execute(query)
        value = rows[0][0]

        errors = 0
        retries = 0
        for w in workers:
            errors = errors + w.errors
            retries = retries + w.retries

        assert (value == threads * iterations) and (errors == 0), "value={}, errors={}, retries={}".format(value, errors, retries)
