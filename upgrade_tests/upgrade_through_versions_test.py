import operator
import os
import pprint
import random
import signal
import time
import uuid
from collections import defaultdict, namedtuple
from multiprocessing import Process, Queue
from Queue import Empty, Full

import psutil
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.query import SimpleStatement
from six import print_

from dtest import Tester, debug
from tools import generate_ssl_stores, known_failure, new_node
from upgrade_base import UPGRADE_TEST_RUN, switch_jdks
from upgrade_manifest import (build_upgrade_pairs, current_2_0_x,
                              current_2_1_x, current_2_2_x, current_3_0_x,
                              head_trunk, indev_2_2_x, next_2_2_x)


def data_writer(tester, to_verify_queue, verification_done_queue, rewrite_probability=0):
    """
    Process for writing/rewriting data continuously.

    Pushes to a queue to be consumed by data_checker.

    Pulls from a queue of already-verified rows written by data_checker that it can overwrite.

    Intended to be run using multiprocessing.
    """
    # 'tester' is a cloned object so we shouldn't be inappropriately sharing anything with another process
    session = tester.patient_cql_connection(tester.node1, keyspace="upgrade", protocol_version=tester.protocol_version)

    prepared = session.prepare("UPDATE cf SET v=? WHERE k=?")
    prepared.consistency_level = ConsistencyLevel.QUORUM

    def handle_sigterm(signum, frame):
        # need to close queue gracefully if possible, or the data_checker process
        # can't seem to empty the queue and test failures result.
        to_verify_queue.close()
        exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    while True:
        try:
            key = None

            if (rewrite_probability > 0) and (random.randint(0, 100) <= rewrite_probability):
                try:
                    key = verification_done_queue.get_nowait()
                except Empty:
                    # we wanted a re-write but the re-writable queue was empty. oh well.
                    pass

            key = key or uuid.uuid4()

            val = uuid.uuid4()

            session.execute(prepared, (val, key))

            to_verify_queue.put_nowait((key, val,))
        except Exception:
            debug("Error in data writer process!")
            to_verify_queue.close()
            raise


def data_checker(tester, to_verify_queue, verification_done_queue):
    """
    Process for checking data continuously.

    Pulls from a queue written to by data_writer to know what to verify.

    Pushes to a queue to tell data_writer what's been verified and could be a candidate for re-writing.

    Intended to be run using multiprocessing.
    """
    # 'tester' is a cloned object so we shouldn't be inappropriately sharing anything with another process
    session = tester.patient_cql_connection(tester.node1, keyspace="upgrade", protocol_version=tester.protocol_version)

    prepared = session.prepare("SELECT v FROM cf WHERE k=?")
    prepared.consistency_level = ConsistencyLevel.QUORUM

    def handle_sigterm(signum, frame):
        # need to close queue gracefully if possible, or the data_checker process
        # can't seem to empty the queue and test failures result.
        verification_done_queue.close()
        exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    while True:
        try:
            # here we could block, but if the writer process terminates early with an empty queue
            # we would end up blocking indefinitely
            (key, expected_val) = to_verify_queue.get_nowait()

            actual_val = session.execute(prepared, (key,))[0][0]
        except Empty:
            time.sleep(0.1)  # let's not eat CPU if the queue is empty
            continue
        except Exception:
            debug("Error in data verifier process!")
            verification_done_queue.close()
            raise
        else:
            try:
                verification_done_queue.put_nowait(key)
            except Full:
                # the rewritable queue is full, not a big deal. drop this one.
                # we keep the rewritable queue held to a modest max size
                # and allow dropping some rewritables because we don't want to
                # rewrite rows in the same sequence as originally written
                pass

        tester.assertEqual(expected_val, actual_val, "Data did not match expected value!")


def counter_incrementer(tester, to_verify_queue, verification_done_queue, rewrite_probability=0):
    """
    Process for incrementing counters continuously.

    Pushes to a queue to be consumed by counter_checker.

    Pulls from a queue of already-verified rows written by data_checker that it can increment again.

    Intended to be run using multiprocessing.
    """
    # 'tester' is a cloned object so we shouldn't be inappropriately sharing anything with another process
    session = tester.patient_cql_connection(tester.node1, keyspace="upgrade", protocol_version=tester.protocol_version)

    prepared = session.prepare("UPDATE countertable SET c = c + 1 WHERE k1=?")
    prepared.consistency_level = ConsistencyLevel.QUORUM

    def handle_sigterm(signum, frame):
        # need to close queue gracefully if possible, or the data_checker process
        # can't seem to empty the queue and test failures result.
        to_verify_queue.close()
        exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    while True:
        try:
            key = None
            count = 0  # this will get set to actual last known count if we do a re-write

            if (rewrite_probability > 0) and (random.randint(0, 100) <= rewrite_probability):
                try:
                    key, count = verification_done_queue.get_nowait()
                except Empty:
                    # we wanted a re-write but the re-writable queue was empty. oh well.
                    pass

            key = key or uuid.uuid4()

            session.execute(prepared, (key))

            to_verify_queue.put_nowait((key, count + 1,))
        except Exception:
            debug("Error in counter incrementer process!")
            to_verify_queue.close()
            raise


def counter_checker(tester, to_verify_queue, verification_done_queue):
    """
    Process for checking counters continuously.

    Pulls from a queue written to by counter_incrementer to know what to verify.

    Pushes to a queue to tell counter_incrementer what's been verified and could be a candidate for incrementing again.

    Intended to be run using multiprocessing.
    """
    # 'tester' is a cloned object so we shouldn't be inappropriately sharing anything with another process
    session = tester.patient_cql_connection(tester.node1, keyspace="upgrade", protocol_version=tester.protocol_version)

    prepared = session.prepare("SELECT c FROM countertable WHERE k1=?")
    prepared.consistency_level = ConsistencyLevel.QUORUM

    def handle_sigterm(signum, frame):
        # need to close queue gracefully if possible, or the data_checker process
        # can't seem to empty the queue and test failures result.
        verification_done_queue.close()
        exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    while True:
        try:
            # here we could block, but if the writer process terminates early with an empty queue
            # we would end up blocking indefinitely
            (key, expected_count) = to_verify_queue.get_nowait()

            actual_count = session.execute(prepared, (key,))[0][0]
        except Empty:
            time.sleep(0.1)  # let's not eat CPU if the queue is empty
            continue
        except Exception:
            debug("Error in counter verifier process!")
            verification_done_queue.close()
            raise
        else:
            tester.assertEqual(expected_count, actual_count, "Data did not match expected value!")

            try:
                verification_done_queue.put_nowait((key, actual_count))
            except Full:
                # the rewritable queue is full, not a big deal. drop this one.
                # we keep the rewritable queue held to a modest max size
                # and allow dropping some rewritables because we don't want to
                # rewrite rows in the same sequence as originally written
                pass


class UpgradeTester(Tester):
    """
    Upgrades a 3-node Murmur3Partitioner cluster through versions specified in test_version_metas.
    """
    test_version_metas = None  # set on init to know which versions to use
    subprocs = None  # holds any subprocesses, for status checking and cleanup
    extra_config = None  # holds a non-mutable structure that can be cast as dict()
    __test__ = False  # this is a base class only

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            r'RejectedExecutionException.*ThreadPoolExecutor has shut down',
            r'Cannot update data center or rack from.*for live host'  # occurs due to test/ccm writing topo on down nodes
        ]
        self.subprocs = []
        Tester.__init__(self, *args, **kwargs)

    def setUp(self):
        debug("Upgrade test beginning, setting CASSANDRA_VERSION to {}, and jdk to {}. (Prior values will be restored after test)."
              .format(self.test_version_metas[0].version, self.test_version_metas[0].java_version))
        os.environ['CASSANDRA_VERSION'] = self.test_version_metas[0].version
        switch_jdks(self.test_version_metas[0].java_version)

        super(UpgradeTester, self).setUp()
        debug("Versions to test (%s): %s" % (type(self), str([v.version for v in self.test_version_metas])))

    def init_config(self):
        self.init_default_config()

        if self.extra_config is not None:
            debug("Setting extra configuration options:\n{}".format(
                pprint.pformat(dict(self.extra_config), indent=4))
            )
            self.cluster.set_configuration_options(
                values=dict(self.extra_config)
            )

    def parallel_upgrade_test(self):
        """
        Test upgrading cluster all at once (requires cluster downtime).
        """
        self.upgrade_scenario()

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11393',
                   flaky=True)
    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11763',
                   flaky=True)
    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11765',
                   flaky=True)
    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11767',
                   flaky=True)
    def rolling_upgrade_test(self):
        """
        Test rolling upgrade of the cluster, so we have mixed versions part way through.
        """
        self.upgrade_scenario(rolling=True)

    def parallel_upgrade_with_internode_ssl_test(self):
        """
        Test upgrading cluster all at once (requires cluster downtime), with internode ssl.
        """
        self.upgrade_scenario(internode_ssl=True)

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11763',
                   flaky=True)
    def rolling_upgrade_with_internode_ssl_test(self):
        """
        Rolling upgrade test using internode ssl.
        """
        self.upgrade_scenario(rolling=True, internode_ssl=True)

    def upgrade_scenario(self, populate=True, create_schema=True, rolling=False, after_upgrade_call=(), internode_ssl=False):
        # Record the rows we write as we go:
        self.row_values = set()
        cluster = self.cluster
        if cluster.version() >= '3.0':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true',
                                               'enable_scripted_user_defined_functions': 'true'})
        elif cluster.version() >= '2.2':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true'})

        if internode_ssl:
            debug("***using internode ssl***")
            generate_ssl_stores(self.test_path)
            self.cluster.enable_internode_ssl(self.test_path)

        if populate:
            # Start with 3 node cluster
            debug('Creating cluster (%s)' % self.test_version_metas[0].version)
            cluster.populate(3)
            [node.start(use_jna=True, wait_for_binary_proto=True) for node in cluster.nodelist()]
        else:
            debug("Skipping cluster creation (should already be built)")

        # add nodes to self for convenience
        for i, node in enumerate(cluster.nodelist(), 1):
            node_name = 'node' + str(i)
            setattr(self, node_name, node)

        if create_schema:
            if rolling:
                self._create_schema_for_rolling()
            else:
                self._create_schema()
        else:
            debug("Skipping schema creation (should already be built)")
        time.sleep(5)  # sigh...

        self._log_current_ver(self.test_version_metas[0])

        if rolling:
            # start up processes to write and verify data
            write_proc, verify_proc, verification_queue = self._start_continuous_write_and_verify(wait_for_rowcount=5000)
            increment_proc, incr_verify_proc, incr_verify_queue = self._start_continuous_counter_increment_and_verify(wait_for_rowcount=5000)

            # upgrade through versions
            for version_meta in self.test_version_metas[1:]:
                for num, node in enumerate(self.cluster.nodelist()):
                    # sleep (sigh) because driver needs extra time to keep up with topo and make quorum possible
                    # this is ok, because a real world upgrade would proceed much slower than this programmatic one
                    # additionally this should provide more time for timeouts and other issues to crop up as well, which we could
                    # possibly "speed past" in an overly fast upgrade test
                    time.sleep(60)

                    self.upgrade_to_version(version_meta, partial=True, nodes=(node,))

                    self._check_on_subprocs(self.subprocs)
                    debug('Successfully upgraded %d of %d nodes to %s' %
                          (num + 1, len(self.cluster.nodelist()), version_meta.version))

                self.cluster.set_install_dir(version=version_meta.version)

            # Stop write processes
            write_proc.terminate()
            increment_proc.terminate()
            # wait for the verification queue's to empty (and check all rows) before continuing
            self._wait_until_queue_condition('writes pending verification', verification_queue, operator.le, 0, max_wait_s=1200)
            self._wait_until_queue_condition('counters pending verification', incr_verify_queue, operator.le, 0, max_wait_s=1200)
            self._check_on_subprocs([verify_proc, incr_verify_proc])  # make sure the verification processes are running still

            self._terminate_subprocs()
        # not a rolling upgrade, do everything in parallel:
        else:
            # upgrade through versions
            for version_meta in self.test_version_metas[1:]:
                self._write_values()
                self._increment_counters()

                self.upgrade_to_version(version_meta)
                self.cluster.set_install_dir(version=version_meta.version)

                self._check_values()
                self._check_counters()
                self._check_select_count()

            # run custom post-upgrade callables
        for call in after_upgrade_call:
            call()

            debug('All nodes successfully upgraded to %s' % version_meta)
            self._log_current_ver(version_meta)

        cluster.stop()

    def tearDown(self):
        # just to be super sure we get cleaned up
        self._terminate_subprocs()

        super(UpgradeTester, self).tearDown()

    def _check_on_subprocs(self, subprocs):
        """
        Check on given subprocesses.

        If any are not alive, we'll go ahead and terminate any remaining alive subprocesses since this test is going to fail.
        """
        subproc_statuses = [s.is_alive() for s in subprocs]
        if not all(subproc_statuses):
            message = "A subprocess has terminated early. Subprocess statuses: "
            for s in subprocs:
                message += "{name} (is_alive: {aliveness}), ".format(name=s.name, aliveness=s.is_alive())
            message += "attempting to terminate remaining subprocesses now."
            self._terminate_subprocs()
            raise RuntimeError(message)

    def _terminate_subprocs(self):
        for s in self.subprocs:
            if s.is_alive():
                try:
                    psutil.Process(s.pid).kill()  # with fire damnit
                except Exception:
                    debug("Error terminating subprocess. There could be a lingering process.")
                    pass

    def upgrade_to_version(self, version_meta, partial=False, nodes=None):
        """
        Upgrade Nodes - if *partial* is True, only upgrade those nodes
        that are specified by *nodes*, otherwise ignore *nodes* specified
        and upgrade all nodes.
        """
        debug('Upgrading {nodes} to {version}'.format(nodes=[n.name for n in nodes] if nodes is not None else 'all nodes', version=version_meta.version))
        switch_jdks(version_meta.java_version)
        debug("JAVA_HOME: " + os.environ.get('JAVA_HOME'))
        if not partial:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        for node in nodes:
            node.set_install_dir(version=version_meta.version)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))

        # hacky? yes. We could probably extend ccm to allow this publicly.
        # the topology file needs to be written before any nodes are started
        # otherwise they won't be grouped into dc's properly for multi-dc tests
        self.cluster._Cluster__update_topology_files()

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, version_meta.version))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            node.nodetool('upgradesstables -a')

    def _log_current_ver(self, current_version_meta):
        """
        Logs where we currently are in the upgrade path, surrounding the current branch/tag, like ***sometag***
        """
        vers = [m.version for m in self.test_version_metas]
        curr_index = vers.index(current_version_meta.version)
        debug(
            "Current upgrade path: {}".format(
                vers[:curr_index] + ['***' + current_version_meta.version + '***'] + vers[curr_index + 1:]))

    def _create_schema_for_rolling(self):
        """
        Slightly different schema variant for testing rolling upgrades with quorum reads/writes.
        """
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)

        session.execute("CREATE KEYSPACE upgrade WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};")

        session.execute('use upgrade')
        session.execute('CREATE TABLE cf ( k uuid PRIMARY KEY, v uuid )')
        session.execute('CREATE INDEX vals ON cf (v)')

        session.execute("""
            CREATE TABLE countertable (
                k1 uuid,
                c counter,
                PRIMARY KEY (k1)
                );""")

    def _create_schema(self):
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)

        session.execute("CREATE KEYSPACE upgrade WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};")

        session.execute('use upgrade')
        session.execute('CREATE TABLE cf ( k int PRIMARY KEY, v text )')
        session.execute('CREATE INDEX vals ON cf (v)')

        session.execute("""
            CREATE TABLE countertable (
                k1 text,
                k2 int,
                c counter,
                PRIMARY KEY (k1, k2)
                );""")

    def _write_values(self, num=100):
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade")
        for i in xrange(num):
            x = len(self.row_values) + 1
            session.execute("UPDATE cf SET v='%d' WHERE k=%d" % (x, x))
            self.row_values.add(x)

    def _check_values(self, consistency_level=ConsistencyLevel.ALL):
        for node in self.cluster.nodelist():
            session = self.patient_cql_connection(node, protocol_version=self.protocol_version)
            session.execute("use upgrade")
            for x in self.row_values:
                query = SimpleStatement("SELECT k,v FROM cf WHERE k=%d" % x, consistency_level=consistency_level)
                result = session.execute(query)
                k, v = result[0]
                self.assertEqual(x, k)
                self.assertEqual(str(x), v)

    def _wait_until_queue_condition(self, label, queue, opfunc, required_len, max_wait_s=600):
        """
        Waits up to max_wait_s for queue size to return True when evaluated against a condition function from the operator module.

        Label is just a string identifier for easier debugging.

        On Mac OS X may not be able to check queue size, in which case it will not block.

        If time runs out, raises RuntimeError.
        """
        wait_end_time = time.time() + max_wait_s

        while time.time() < wait_end_time:
            try:
                qsize = queue.qsize()
            except NotImplementedError:
                debug("Queue size may not be checkable on Mac OS X. Test will continue without waiting.")
                break
            if opfunc(qsize, required_len):
                debug("{} queue size ({}) is '{}' to {}. Continuing.".format(label, qsize, opfunc.__name__, required_len))
                break

            if divmod(round(time.time()), 30)[1] == 0:
                debug("{} queue size is at {}, target is to reach '{}' {}".format(label, qsize, opfunc.__name__, required_len))

            time.sleep(0.1)
            continue
        else:
            raise RuntimeError("Ran out of time waiting for queue size ({}) to be '{}' to {}. Aborting.".format(qsize, opfunc.__name__, required_len))

    def _start_continuous_write_and_verify(self, wait_for_rowcount=0, max_wait_s=600):
        """
        Starts a writer process, a verifier process, a queue to track writes,
        and a queue to track successful verifications (which are rewrite candidates).

        wait_for_rowcount provides a number of rows to write before unblocking and continuing.

        Returns the writer process, verifier process, and the to_verify_queue.
        """
        # queue of writes to be verified
        to_verify_queue = Queue()
        # queue of verified writes, which are update candidates
        verification_done_queue = Queue(maxsize=500)

        writer = Process(target=data_writer, args=(self, to_verify_queue, verification_done_queue, 25))
        # daemon subprocesses are killed automagically when the parent process exits
        writer.daemon = True
        self.subprocs.append(writer)
        writer.start()

        if wait_for_rowcount > 0:
            self._wait_until_queue_condition('rows written (but not verified)', to_verify_queue, operator.ge, wait_for_rowcount, max_wait_s=max_wait_s)

        verifier = Process(target=data_checker, args=(self, to_verify_queue, verification_done_queue))
        # daemon subprocesses are killed automagically when the parent process exits
        verifier.daemon = True
        self.subprocs.append(verifier)
        verifier.start()

        return writer, verifier, to_verify_queue

    def _start_continuous_counter_increment_and_verify(self, wait_for_rowcount=0, max_wait_s=600):
        """
        Starts a counter incrementer process, a verifier process, a queue to track writes,
        and a queue to track successful verifications (which are re-increment candidates).

        Returns the writer process, verifier process, and the to_verify_queue.
        """
        # queue of writes to be verified
        to_verify_queue = Queue()
        # queue of verified writes, which are update candidates
        verification_done_queue = Queue(maxsize=500)

        incrementer = Process(target=data_writer, args=(self, to_verify_queue, verification_done_queue, 25))
        # daemon subprocesses are killed automagically when the parent process exits
        incrementer.daemon = True
        self.subprocs.append(incrementer)
        incrementer.start()

        if wait_for_rowcount > 0:
            self._wait_until_queue_condition('counters incremented (but not verified)', to_verify_queue, operator.ge, wait_for_rowcount, max_wait_s=max_wait_s)

        count_verifier = Process(target=data_checker, args=(self, to_verify_queue, verification_done_queue))
        # daemon subprocesses are killed automagically when the parent process exits
        count_verifier.daemon = True
        self.subprocs.append(count_verifier)
        count_verifier.start()

        return incrementer, count_verifier, to_verify_queue

    def _increment_counters(self, opcount=25000):
        debug("performing {opcount} counter increments".format(opcount=opcount))
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade;")

        update_counter_query = ("UPDATE countertable SET c = c + 1 WHERE k1='{key1}' and k2={key2}")

        self.expected_counts = {}
        for i in range(10):
            self.expected_counts[uuid.uuid4()] = defaultdict(int)

        fail_count = 0

        for i in range(opcount):
            key1 = random.choice(self.expected_counts.keys())
            key2 = random.randint(1, 10)
            try:
                query = SimpleStatement(update_counter_query.format(key1=key1, key2=key2), consistency_level=ConsistencyLevel.ALL)
                session.execute(query)
            except WriteTimeout:
                fail_count += 1
            else:
                self.expected_counts[key1][key2] += 1
            if fail_count > 100:
                break

        assert fail_count < 100, "Too many counter increment failures"

    def _check_counters(self):
        debug("Checking counter values...")
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade;")

        for key1 in self.expected_counts.keys():
            for key2 in self.expected_counts[key1].keys():
                expected_value = self.expected_counts[key1][key2]

                query = SimpleStatement("SELECT c from countertable where k1='{key1}' and k2={key2};".format(key1=key1, key2=key2),
                                        consistency_level=ConsistencyLevel.ONE)
                results = session.execute(query)

                if results is not None:
                    actual_value = results[0][0]
                else:
                    # counter wasn't found
                    actual_value = None

                assert actual_value == expected_value, "Counter not at expected value. Got %s, expected %s" % (actual_value, expected_value)

    def _check_select_count(self, consistency_level=ConsistencyLevel.ALL):
        debug("Checking SELECT COUNT(*)")
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade;")

        expected_num_rows = len(self.row_values)

        countquery = SimpleStatement("SELECT COUNT(*) FROM cf;", consistency_level=consistency_level)
        result = session.execute(countquery)

        if result is not None:
            actual_num_rows = result[0][0]
            self.assertEqual(actual_num_rows, expected_num_rows, "SELECT COUNT(*) returned %s when expecting %s" % (actual_num_rows, expected_num_rows))
        else:
            self.fail("Count query did not return")


class BootstrapMixin(object):
    """
    Can be mixed into UpgradeTester or a subclass thereof to add bootstrap tests.

    Using this class is not currently feasible on lengthy upgrade paths, as each
    version bump adds a node and this will eventually exhaust resources.
    """
    def _bootstrap_new_node(self):
        # Check we can bootstrap a new node on the upgraded cluster:
        debug("Adding a node to the cluster")
        nnode = new_node(self.cluster, remote_debug_port=str(2000 + len(self.cluster.nodes)))
        nnode.start(use_jna=True, wait_other_notice=True, wait_for_binary_proto=True)
        self._write_values()
        self._increment_counters()
        self._check_values()
        self._check_counters()

    def _bootstrap_new_node_multidc(self):
        # Check we can bootstrap a new node on the upgraded cluster:
        debug("Adding a node to the cluster")
        nnode = new_node(self.cluster, remote_debug_port=str(2000 + len(self.cluster.nodes)), data_center='dc2')

        nnode.start(use_jna=True, wait_other_notice=True, wait_for_binary_proto=True)
        self._write_values()
        self._increment_counters()
        self._check_values()
        self._check_counters()

    def bootstrap_test(self):
        # try and add a new node
        self.upgrade_scenario(after_upgrade_call=(self._bootstrap_new_node,))

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11394',
                   flaky=True)
    def bootstrap_multidc_test(self):
        # try and add a new node
        # multi dc, 2 nodes in each dc
        cluster = self.cluster

        if cluster.version() >= '3.0':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true',
                                               'enable_scripted_user_defined_functions': 'true'})
        elif cluster.version() >= '2.2':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true'})

        cluster.populate([2, 2])
        [node.start(use_jna=True, wait_for_binary_proto=True) for node in self.cluster.nodelist()]
        self._multidc_schema_create()
        self.upgrade_scenario(populate=False, create_schema=False, after_upgrade_call=(self._bootstrap_new_node_multidc,))

    def _multidc_schema_create(self):
        session = self.patient_cql_connection(self.cluster.nodelist()[0], protocol_version=self.protocol_version)

        if self.cluster.version() >= '1.2':
            # DDL for C* 1.2+
            session.execute("CREATE KEYSPACE upgrade WITH replication = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':2};")
        else:
            # DDL for C* 1.1
            session.execute("""CREATE KEYSPACE upgrade WITH strategy_class = 'NetworkTopologyStrategy'
            AND strategy_options:'dc1':1
            AND strategy_options:'dc2':2;
            """)

        session.execute('use upgrade')
        session.execute('CREATE TABLE cf ( k int PRIMARY KEY , v text )')
        session.execute('CREATE INDEX vals ON cf (v)')

        session.execute("""
            CREATE TABLE countertable (
                k1 text,
                k2 int,
                c counter,
                PRIMARY KEY (k1, k2)
                );""")


def create_upgrade_class(clsname, version_metas, protocol_version,
                         bootstrap_test=False, extra_config=None):
    """
    Dynamically creates a test subclass for testing the given versions.

    'clsname' is the name of the new class.
    'protocol_version' is an int.
    'bootstrap_test' is a boolean, if True bootstrap testing will be included. Default False.
    'version_list' is a list of versions ccm will recognize, to be upgraded in order.
    'extra_config' is tuple of config options that can (eventually) be cast as a dict,
    e.g. (('partitioner', org.apache.cassandra.dht.Murmur3Partitioner''))
    """
    if extra_config is None:
        extra_config = (('partitioner', 'org.apache.cassandra.dht.Murmur3Partitioner'),)

    if bootstrap_test:
        parent_classes = (UpgradeTester, BootstrapMixin)
    else:
        parent_classes = (UpgradeTester,)

    # short names for debug output
    parent_class_names = [cls.__name__ for cls in parent_classes]

    print_("Creating test class {} ".format(clsname))
    print_("  for C* versions: {} ".format(version_metas))
    print_("  using protocol: v{}, and parent classes: {}".format(protocol_version, parent_class_names))
    print_("  to run these tests alone, use `nosetests {}.py:{}`".format(__name__, clsname))

    newcls = type(
        clsname,
        parent_classes,
        {'test_version_metas': version_metas, '__test__': UPGRADE_TEST_RUN, 'protocol_version': protocol_version, 'extra_config': extra_config}
    )

    if clsname in globals():
        raise RuntimeError("Class by name already exists!")

    globals()[clsname] = newcls
    return newcls


MultiUpgrade = namedtuple('MultiUpgrade', ('name', 'version_metas', 'protocol_version', 'extra_config'))

MULTI_UPGRADES = (
    # Proto v1 upgrades (v1 supported on 2.0, 2.1, 2.2)
    MultiUpgrade(name='ProtoV1Upgrade_AllVersions_EndsAt_indev_2_2_x',
                 version_metas=[current_2_0_x, current_2_1_x, indev_2_2_x], protocol_version=1, extra_config=None),
    MultiUpgrade(name='ProtoV1Upgrade_AllVersions_RandomPartitioner_EndsAt_indev_2_2_x',
                 version_metas=[current_2_0_x, current_2_1_x, indev_2_2_x], protocol_version=1,
                 extra_config=(
                     ('partitioner', 'org.apache.cassandra.dht.RandomPartitioner'),
                 )),
    MultiUpgrade(name='ProtoV1Upgrade_AllVersions_EndsAt_next_2_2_x',
                 version_metas=[current_2_0_x, current_2_1_x, next_2_2_x], protocol_version=1, extra_config=None),
    MultiUpgrade(name='ProtoV1Upgrade_AllVersions_RandomPartitioner_EndsAt_indev_2_2_x',
                 version_metas=[current_2_0_x, current_2_1_x, next_2_2_x], protocol_version=1,
                 extra_config=(
                     ('partitioner', 'org.apache.cassandra.dht.RandomPartitioner'),
                 )),

    # Proto v2 upgrades (v2 is supported on 2.0, 2.1, 2.2)
    MultiUpgrade(name='ProtoV2Upgrade_AllVersions_EndsAt_indev_2_2_x',
                 version_metas=[current_2_0_x, current_2_1_x, indev_2_2_x], protocol_version=2, extra_config=None),
    MultiUpgrade(name='ProtoV2Upgrade_AllVersions_RandomPartitioner_EndsAt_indev_2_2_x',
                 version_metas=[current_2_0_x, current_2_1_x, indev_2_2_x], protocol_version=2,
                 extra_config=(
                     ('partitioner', 'org.apache.cassandra.dht.RandomPartitioner'),
                 )),
    MultiUpgrade(name='ProtoV2Upgrade_AllVersions_EndsAt_next_2_2x',
                 version_metas=[current_2_0_x, current_2_1_x, next_2_2_x], protocol_version=2, extra_config=None),
    MultiUpgrade(name='ProtoV2Upgrade_AllVersions_RandomPartitioner_EndsAt_next_2_2_x',
                 version_metas=[current_2_0_x, current_2_1_x, next_2_2_x], protocol_version=2,
                 extra_config=(
                     ('partitioner', 'org.apache.cassandra.dht.RandomPartitioner'),
                 )),

    # Proto v3 upgrades (v3 is supported on 2.1, 2.2, 3.0, 3.1, trunk)
    MultiUpgrade(name='ProtoV3Upgrade_AllVersions_EndsAt_Trunk_HEAD',
                 version_metas=[current_2_1_x, current_2_2_x, current_3_0_x, head_trunk], protocol_version=3, extra_config=None),
    MultiUpgrade(name='ProtoV3Upgrade_AllVersions_RandomPartitioner_EndsAt_Trunk_HEAD',
                 version_metas=[current_2_1_x, current_2_2_x, current_3_0_x, head_trunk], protocol_version=3,
                 extra_config=(
                     ('partitioner', 'org.apache.cassandra.dht.RandomPartitioner'),
                 )),

    # Proto v4 upgrades (v4 is supported on 2.2, 3.0, 3.1, trunk)
    MultiUpgrade(name='ProtoV4Upgrade_AllVersions_EndsAt_Trunk_HEAD',
                 version_metas=[current_2_2_x, current_3_0_x, head_trunk], protocol_version=4, extra_config=None),
    MultiUpgrade(name='ProtoV4Upgrade_AllVersions_RandomPartitioner_EndsAt_Trunk_HEAD',
                 version_metas=[current_2_2_x, current_3_0_x, head_trunk], protocol_version=4,
                 extra_config=(
                     ('partitioner', 'org.apache.cassandra.dht.RandomPartitioner'),
                 )),
)

for upgrade in MULTI_UPGRADES:
    # if any version_metas are None, this means they are verions not to be tested currently
    if all(upgrade.version_metas):
        create_upgrade_class(upgrade.name, [m for m in upgrade.version_metas], protocol_version=upgrade.protocol_version, extra_config=upgrade.extra_config)


for pair in build_upgrade_pairs():
    create_upgrade_class(
        'Test' + pair.name,
        [pair.starting_meta, pair.upgrade_meta],
        protocol_version=pair.starting_meta.max_proto_v,
        bootstrap_test=True
    )


# FOR CUSTOM/LOCAL UPGRADE PATH TESTING:
#    Define UPGRADE_PATH in your env as a comma-separated list of versions
#    Versions can be any format CCM works with, including: git:cassandra-2.1, 2.1.12, local:somebranch, etc.
#    Set your desired protocol version in your env's PROTOCOL_VERSION
if os.environ.get('UPGRADE_PATH'):
    create_upgrade_class(
        'UserDefinedUpgradeTest',
        os.environ.get('UPGRADE_PATH').split(','),
        bootstrap_test=True,
        protocol_version=int(os.environ.get('PROTOCOL_VERSION', 3))
    )
