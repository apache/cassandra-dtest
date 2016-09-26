import Queue
import sys
import threading
import time
from collections import OrderedDict, namedtuple
from copy import deepcopy

from cassandra import ConsistencyLevel, consistency_value_to_name
from cassandra.query import SimpleStatement
from nose.plugins.attrib import attr
from nose.tools import assert_greater_equal

from tools.assertions import assert_length_equal, assert_none, assert_unavailable
from dtest import DISABLE_VNODES, MultiError, Tester, debug, create_ks, create_cf
from tools.data import (create_c1c2_table, insert_c1c2, insert_columns,
                        query_c1c2, rows_to_list)
from tools.decorators import known_failure, since

ExpectedConsistency = namedtuple('ExpectedConsistency', ('num_write_nodes', 'num_read_nodes', 'is_strong'))


class TestHelper(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)
        self.lock = threading.Lock()

    def log(self, message):
        with self.lock:
            debug(message)

    def _is_local(self, cl):
        return (cl == ConsistencyLevel.LOCAL_QUORUM or
                cl == ConsistencyLevel.LOCAL_ONE or
                cl == ConsistencyLevel.LOCAL_SERIAL)

    def _is_conditional(self, cl):
        return (cl == ConsistencyLevel.SERIAL or
                cl == ConsistencyLevel.LOCAL_SERIAL)

    def _required_nodes(self, cl, rf_factors, dc):
        """
        Return the number of nodes required by this consistency level
        in the current data center, specified by the dc parameter,
        given a list of replication factors, one per dc.
        """
        return {
            ConsistencyLevel.ANY: 1,
            ConsistencyLevel.ONE: 1,
            ConsistencyLevel.TWO: 2,
            ConsistencyLevel.THREE: 3,
            ConsistencyLevel.QUORUM: sum(rf_factors) / 2 + 1,
            ConsistencyLevel.ALL: sum(rf_factors),
            ConsistencyLevel.LOCAL_QUORUM: rf_factors[dc] / 2 + 1,
            ConsistencyLevel.EACH_QUORUM: rf_factors[dc] / 2 + 1,
            ConsistencyLevel.SERIAL: sum(rf_factors) / 2 + 1,
            ConsistencyLevel.LOCAL_SERIAL: rf_factors[dc] / 2 + 1,
            ConsistencyLevel.LOCAL_ONE: 1,
        }[cl]

    def get_expected_consistency(self, idx, rf_factors, write_cl, read_cl):
            """
            Given a node index, identify to which data center we are connecting and return
            the expected consistency: number of nodes we write to, read from, and whether
            we should have strong consistency, that is whether R + W > N
            """
            nodes = [self.nodes] if isinstance(self.nodes, int) else self.nodes

            def get_data_center():
                """
                :return: the data center corresponding to this node
                """
                dc = 0
                for i in xrange(1, len(nodes)):
                    if idx < sum(nodes[:i]):
                        break
                    dc += 1
                return dc

            data_center = get_data_center()
            if write_cl == ConsistencyLevel.EACH_QUORUM:
                write_nodes = sum([self._required_nodes(write_cl, rf_factors, i) for i in range(0, len(nodes))])
            else:
                write_nodes = self._required_nodes(write_cl, rf_factors, data_center)

            read_nodes = self._required_nodes(read_cl, rf_factors, data_center)
            is_strong = read_nodes + write_nodes > sum(rf_factors)

            return ExpectedConsistency(num_write_nodes=write_nodes,
                                       num_read_nodes=read_nodes,
                                       is_strong=is_strong)

    def _should_succeed(self, cl, rf_factors, num_nodes_alive, current):
        """
        Return true if the read or write operation should succeed based on
        the consistency level requested, the replication factors and the
        number of nodes alive in each data center.
        """
        if self._is_local(cl):
            return num_nodes_alive[current] >= self._required_nodes(cl, rf_factors, current)
        elif cl == ConsistencyLevel.EACH_QUORUM:
            for i in xrange(0, len(rf_factors)):
                if num_nodes_alive[i] < self._required_nodes(cl, rf_factors, i):
                    return False
            return True
        else:
            return sum(num_nodes_alive) >= self._required_nodes(cl, rf_factors, current)

    def _start_cluster(self, save_sessions=False, requires_local_reads=False):
        cluster = self.cluster
        nodes = self.nodes
        rf = self.rf

        configuration_options = {'hinted_handoff_enabled': False}

        # If we must read from the local replica first, then the dynamic snitch poses a problem
        # because occasionally it may think that another replica is preferable even if the
        # coordinator is a replica
        if requires_local_reads:
            configuration_options['dynamic_snitch'] = False

        cluster.set_configuration_options(values=configuration_options)
        cluster.populate(nodes)

        if requires_local_reads and isinstance(nodes, int):
            # Changing the snitch to PropertyFileSnitch even in the
            # single dc tests ensures that StorageProxy sorts the replicas eligible
            # for reading by proximity to the local host, essentially picking the
            # local host for local reads, see IEndpointSnitch.sortByProximity() and
            # StorageProxy.getLiveSortedEndpoints(), which is called by the AbstractReadExecutor
            # to determine the target replicas. The default case, a SimpleSnitch wrapped in
            # a dynamic snitch, may rarely choose a different replica.
            debug('Changing snitch for single dc case')
            for node in cluster.nodelist():
                node.data_center = 'dc1'
            cluster.set_configuration_options(values={
                'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})

        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        self.ksname = 'mytestks'
        session = self.patient_exclusive_cql_connection(cluster.nodelist()[0])

        create_ks(session, self.ksname, rf)
        self.create_tables(session, requires_local_reads)

        if save_sessions:
            self.sessions = []
            self.sessions.append(session)
            for node in cluster.nodelist()[1:]:
                self.sessions.append(self.patient_exclusive_cql_connection(node, self.ksname))

    def create_tables(self, session, requires_local_reads):
        self.create_users_table(session, requires_local_reads)
        self.create_counters_table(session, requires_local_reads)
        session.cluster.control_connection.wait_for_schema_agreement(wait_time=60)

    def truncate_tables(self, session):
        statement = SimpleStatement("TRUNCATE users", ConsistencyLevel.ALL)
        session.execute(statement)
        statement = SimpleStatement("TRUNCATE counters", ConsistencyLevel.ALL)
        session.execute(statement)

    def create_users_table(self, session, requires_local_reads):
        create_cmd = """
            CREATE TABLE users (
                userid int PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            ) WITH COMPACT STORAGE"""

        if requires_local_reads:
            create_cmd += " AND " + self.get_local_reads_properties()

        session.execute(create_cmd)

    @staticmethod
    def get_local_reads_properties():
        """
        If we must read from the local replica first, then we should disable read repair and
        speculative retry, see CASSANDRA-12092
        """
        return " dclocal_read_repair_chance = 0 AND read_repair_chance = 0 AND speculative_retry =  'NONE'"

    def insert_user(self, session, userid, age, consistency, serial_consistency=None):
        text = "INSERT INTO users (userid, firstname, lastname, age) VALUES ({}, 'first{}', 'last{}', {}) {}"\
            .format(userid, userid, userid, age, "IF NOT EXISTS" if serial_consistency else "")
        statement = SimpleStatement(text, consistency_level=consistency, serial_consistency_level=serial_consistency)
        session.execute(statement)

    def update_user(self, session, userid, age, consistency, serial_consistency=None, prev_age=None):
        text = "UPDATE users SET age = {} WHERE userid = {}".format(age, userid)
        if serial_consistency and prev_age:
            text = text + " IF age = {}".format(prev_age)
        statement = SimpleStatement(text, consistency_level=consistency, serial_consistency_level=serial_consistency)
        session.execute(statement)

    def delete_user(self, session, userid, consistency):
        statement = SimpleStatement("DELETE FROM users where userid = {}".format(userid), consistency_level=consistency)
        session.execute(statement)

    def query_user(self, session, userid, age, consistency, check_ret=True):
        statement = SimpleStatement("SELECT userid, age FROM users where userid = {}".format(userid), consistency_level=consistency)
        res = session.execute(statement)
        expected = [[userid, age]] if age else []
        ret = rows_to_list(res) == expected
        if check_ret:
            self.assertTrue(ret, "Got {} from {}, expected {} at {}".format(rows_to_list(res), session.cluster.contact_points, expected, consistency_value_to_name(consistency)))
        return ret

    def create_counters_table(self, session, requires_local_reads):
        create_cmd = """
            CREATE TABLE counters (
                id int PRIMARY KEY,
                c counter
            )"""

        if requires_local_reads:
            create_cmd += " WITH " + self.get_local_reads_properties()

        session.execute(create_cmd)

    def update_counter(self, session, id, consistency, serial_consistency=None):
        text = "UPDATE counters SET c = c + 1 WHERE id = {}".format(id)
        statement = SimpleStatement(text, consistency_level=consistency, serial_consistency_level=serial_consistency)
        session.execute(statement)
        return statement

    def query_counter(self, session, id, val, consistency, check_ret=True):
        statement = SimpleStatement("SELECT * from counters WHERE id = {}".format(id), consistency_level=consistency)
        ret = rows_to_list(session.execute(statement))
        if check_ret:
            self.assertEqual(ret[0][1], val, "Got {} from {}, expected {} at {}".format(ret[0][1],
                                                                                        session.cluster.contact_points,
                                                                                        val,
                                                                                        consistency_value_to_name(consistency)))
        return ret[0][1] if ret else 0


class TestAvailability(TestHelper):
    """
    Test that we can read and write depending on the number of nodes that are alive and the consistency levels.
    """

    def _test_simple_strategy(self, combinations):
        """
        Helper test function for a single data center: invoke _test_insert_query_from_node() for each node
        and each combination, progressively stopping nodes.
        """
        cluster = self.cluster
        nodes = self.nodes
        rf = self.rf

        num_alive = nodes
        for node in xrange(nodes):
            debug('Testing node {} in single dc with {} nodes alive'.format(node, num_alive))
            session = self.patient_exclusive_cql_connection(cluster.nodelist()[node], self.ksname)
            for combination in combinations:
                self._test_insert_query_from_node(session, 0, [rf], [num_alive], *combination)

            self.cluster.nodelist()[node].stop()
            num_alive -= 1

    def _test_network_topology_strategy(self, combinations):
        """
        Helper test function for multiple data centers, invoke _test_insert_query_from_node() for each node
        in each dc and each combination, progressively stopping nodes.
        """
        cluster = self.cluster
        nodes = self.nodes
        rf = self.rf

        nodes_alive = deepcopy(nodes)
        rf_factors = rf.values()

        for i in xrange(0, len(nodes)):  # for each dc
            self.log('Testing dc {} with rf {} and {} nodes alive'.format(i, rf_factors[i], nodes_alive))
            for n in xrange(nodes[i]):  # for each node in this dc
                self.log('Testing node {} in dc {} with {} nodes alive'.format(n, i, nodes_alive))
                node = n + sum(nodes[:i])
                session = self.patient_exclusive_cql_connection(cluster.nodelist()[node], self.ksname)
                for combination in combinations:
                    self._test_insert_query_from_node(session, i, rf_factors, nodes_alive, *combination)

                self.cluster.nodelist()[node].stop(wait_other_notice=True)
                nodes_alive[i] -= 1

    def _test_insert_query_from_node(self, session, dc_idx, rf_factors, num_nodes_alive, write_cl, read_cl, serial_cl=None, check_ret=True):
        """
        Test availability for read and write via the session passed in as a parameter.
        """
        self.log("Connected to %s for %s/%s/%s" %
                 (session.cluster.contact_points, consistency_value_to_name(write_cl), consistency_value_to_name(read_cl), consistency_value_to_name(serial_cl)))

        start = 0
        end = 100
        age = 30

        if self._should_succeed(write_cl, rf_factors, num_nodes_alive, dc_idx):
            for n in xrange(start, end):
                self.insert_user(session, n, age, write_cl, serial_cl)
        else:
            assert_unavailable(self.insert_user, session, end, age, write_cl, serial_cl)

        if self._should_succeed(read_cl, rf_factors, num_nodes_alive, dc_idx):
            for n in xrange(start, end):
                self.query_user(session, n, age, read_cl, check_ret)
        else:
            assert_unavailable(self.query_user, session, end, age, read_cl, check_ret)

    def test_simple_strategy(self):
        """
        Test for a single datacenter, using simple replication strategy.
        """
        self.nodes = 3
        self.rf = 3

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.TWO),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.THREE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_ONE, None, False),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_SERIAL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL),
        ]

        self._test_simple_strategy(combinations)

    @since("3.0")
    def test_simple_strategy_each_quorum(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a single datacenter, using simple replication strategy, only
        the each quorum reads.
        """
        self.nodes = 3
        self.rf = 3

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self._test_simple_strategy(combinations)

    @attr("resource-intensive")
    def test_network_topology_strategy(self):
        """
        Test for multiple datacenters, using network topology replication strategy.
        """
        self.nodes = [3, 3, 3]
        self.rf = OrderedDict([('dc1', 3), ('dc2', 3), ('dc3', 3)])

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.TWO),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.THREE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE, None, False),
            (ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_ONE, None, False),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_SERIAL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL),
        ]

        self._test_network_topology_strategy(combinations)

    @attr("resource-intensive")
    @since("3.0")
    def test_network_topology_strategy_each_quorum(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for multiple datacenters, using network topology strategy, only
        the each quorum reads.
        """
        self.nodes = [3, 3, 3]
        self.rf = OrderedDict([('dc1', 3), ('dc2', 3), ('dc3', 3)])

        self._start_cluster()

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self._test_network_topology_strategy(combinations)


class TestAccuracy(TestHelper):
    """
    Test that we can consistently read back what we wrote depending on the write and read consistency levels.
    """

    class Validation:

        def __init__(self, outer, sessions, nodes, rf_factors, start, end, write_cl, read_cl, serial_cl=None):
            self.outer = outer
            self.sessions = sessions
            self.nodes = nodes
            self.rf_factors = rf_factors
            self.start = start
            self.end = end
            self.write_cl = write_cl
            self.read_cl = read_cl
            self.serial_cl = serial_cl

            outer.log('Testing accuracy with WRITE/READ/SERIAL consistency set to {}/{}/{} (keys : {} to {})'
                      .format(consistency_value_to_name(write_cl), consistency_value_to_name(read_cl), consistency_value_to_name(serial_cl), start, end - 1))

        def get_expected_consistency(self, idx):
            return self.outer.get_expected_consistency(idx, self.rf_factors, self.write_cl, self.read_cl)

        def validate_users(self):
            """
            First validation function: update the users table sending different values to different sessions
            and check that when strong_consistency is true (R + W > N) we read back the latest value from all sessions.
            If strong_consistency is false we instead check that we read back the latest value from at least
            the number of nodes we wrote to.
            """
            outer = self.outer
            sessions = self.sessions
            start = self.start
            end = self.end
            write_cl = self.write_cl
            read_cl = self.read_cl
            serial_cl = self.serial_cl

            def check_all_sessions(idx, n, val):
                expected_consistency = self.get_expected_consistency(idx)
                num = 0
                for s in sessions:
                    if outer.query_user(s, n, val, read_cl, check_ret=expected_consistency.is_strong):
                        num += 1
                assert_greater_equal(num, expected_consistency.num_write_nodes,
                                     "Failed to read value from sufficient number of nodes,"
                                     " required {} but got {} - [{}, {}]"
                                     .format(expected_consistency.num_write_nodes, num, n, val))

            for n in xrange(start, end):
                age = 30
                for s in range(0, len(sessions)):
                    outer.insert_user(sessions[s], n, age, write_cl, serial_cl)
                    check_all_sessions(s, n, age)
                    if serial_cl is None:
                        age += 1
                for s in range(0, len(sessions)):
                    outer.update_user(sessions[s], n, age, write_cl, serial_cl, age - 1)
                    check_all_sessions(s, n, age)
                    age += 1
                outer.delete_user(sessions[0], n, write_cl)
                check_all_sessions(s, n, None)

        def validate_counters(self):
            """
            Second validation function: update the counters table sending different values to different sessions
            and check that when strong_consistency is true (R + W > N) we read back the latest value from all sessions.
            If strong_consistency is false we instead check that we read back the latest value from at least
            the number of nodes we wrote to.
            """
            outer = self.outer
            sessions = self.sessions
            start = self.start
            end = self.end
            write_cl = self.write_cl
            read_cl = self.read_cl
            serial_cl = self.serial_cl

            def check_all_sessions(idx, n, val):
                expected_consistency = self.get_expected_consistency(idx)
                results = []
                for s in sessions:
                    results.append(outer.query_counter(s, n, val, read_cl, check_ret=expected_consistency.is_strong))

                assert_greater_equal(results.count(val), expected_consistency.num_write_nodes,
                                     "Failed to read value from sufficient number of nodes, required {} nodes to have a"
                                     " counter value of {} at key {}, instead got these values: {}"
                                     .format(expected_consistency.num_write_nodes, val, n, results))

            for n in xrange(start, end):
                c = 1
                for s in range(0, len(sessions)):
                    outer.update_counter(sessions[s], n, write_cl, serial_cl)
                    check_all_sessions(s, n, c)
                    # Update the counter again at CL ALL to make sure all nodes are on the same page
                    # since a counter update requires a read
                    outer.update_counter(sessions[s], n, ConsistencyLevel.ALL)
                    c += 2  # the counter was updated twice

    def _run_test_function_in_parallel(self, valid_fcn, nodes, rf_factors, combinations):
        """
        Run a test function in parallel.
        """

        requires_local_reads = False
        for combination in combinations:
            for i, _ in enumerate(nodes):
                expected_consistency = self.get_expected_consistency(i, rf_factors, combination[0], combination[1])
                if not expected_consistency.is_strong:
                    # if at least one combination does not reach strong consistency, in order to validate weak
                    # consistency we require local reads, see CASSANDRA-12092 for details.
                    requires_local_reads = True
                    break

            if requires_local_reads:
                break

        self._start_cluster(save_sessions=True, requires_local_reads=requires_local_reads)

        input_queue = Queue.Queue()
        exceptions_queue = Queue.Queue()

        def run():
            while not input_queue.empty():
                try:
                    v = TestAccuracy.Validation(self, self.sessions, nodes, rf_factors, *input_queue.get(block=False))
                    valid_fcn(v)
                except Queue.Empty:
                    pass
                except:
                    exceptions_queue.put(sys.exc_info())

        start = 0
        num_keys = 50
        for combination in combinations:
            input_queue.put((start, start + num_keys) + combination)
            start += num_keys

        threads = []
        for n in range(0, 8):
            t = threading.Thread(target=run)
            t.setDaemon(True)
            t.start()
            threads.append(t)

        self.log("Waiting for workers to complete")
        while exceptions_queue.empty():
            time.sleep(0.1)
            if len(filter(lambda t: t.isAlive(), threads)) == 0:
                break

        if not exceptions_queue.empty():
            _, exceptions, tracebacks = zip(*exceptions_queue.queue)
            raise MultiError(exceptions=exceptions, tracebacks=tracebacks)

    @attr("resource-intensive")
    def test_simple_strategy_users(self):
        """
        Test for a single datacenter, users table, only the each quorum reads.
        """
        self.nodes = 5
        self.rf = 3

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
            # These are multi-DC consistency levels that should default to quorum calls
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
        ]

        self.log("Testing single dc, users")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, [self.nodes], [self.rf], combinations)

    @attr("resource-intensive")
    @since("3.0")
    def test_simple_strategy_each_quorum_users(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a single datacenter, users table, only the each quorum reads.
        """
        self.nodes = 5
        self.rf = 3

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self.log("Testing single dc, users, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, [self.nodes], [self.rf], combinations)

    @attr("resource-intensive")
    def test_network_topology_strategy_users(self):
        """
        Test for multiple datacenters, users table.
        """
        self.nodes = [3, 3]
        self.rf = OrderedDict([('dc1', 3), ('dc2', 3)])

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ANY, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_SERIAL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL),
        ]

        self.log("Testing multiple dcs, users")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, self.nodes, self.rf.values(), combinations),

    @attr("resource-intensive")
    @since("3.0")
    def test_network_topology_strategy_each_quorum_users(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a multiple datacenters, users table, only the each quorum
        reads.
        """
        self.nodes = [3, 3]
        self.rf = OrderedDict([('dc1', 3), ('dc2', 3)])

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self.log("Testing multiple dcs, users, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_users, self.nodes, self.rf.values(), combinations)

    def test_simple_strategy_counters(self):
        """
        Test for a single datacenter, counters table.
        """
        self.nodes = 3
        self.rf = 3

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
            # These are multi-DC consistency levels that should default to quorum calls
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
        ]

        self.log("Testing single dc, counters")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, [self.nodes], [self.rf], combinations)

    @since("3.0")
    def test_simple_strategy_each_quorum_counters(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for a single datacenter, counters table, only the each quorum
        reads.
        """
        self.nodes = 3
        self.rf = 3

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self.log("Testing single dc, counters, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, [self.nodes], [self.rf], combinations)

    @attr("resource-intensive")
    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12093',
                   flaky=True)
    def test_network_topology_strategy_counters(self):
        """
        Test for multiple datacenters, counters table.
        """
        self.nodes = [3, 3]
        self.rf = OrderedDict([('dc1', 3), ('dc2', 3)])

        combinations = [
            (ConsistencyLevel.ALL, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.ALL, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ALL),
            (ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.QUORUM),
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.ONE),
            (ConsistencyLevel.TWO, ConsistencyLevel.TWO),
            (ConsistencyLevel.ONE, ConsistencyLevel.THREE),
            (ConsistencyLevel.THREE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.ONE),
            (ConsistencyLevel.ONE, ConsistencyLevel.TWO),
            (ConsistencyLevel.TWO, ConsistencyLevel.ONE),
        ]

        self.log("Testing multiple dcs, counters")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, self.nodes, self.rf.values(), combinations),

    @attr("resource-intensive")
    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12094',
                   flaky=True)
    @since("3.0")
    def test_network_topology_strategy_each_quorum_counters(self):
        """
        @jira_ticket CASSANDRA-10584
        Test for multiple datacenters, counters table, only the each quorum
        reads.
        """
        self.nodes = [3, 3]
        self.rf = OrderedDict([('dc1', 3), ('dc2', 3)])

        combinations = [
            (ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM),
            (ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.EACH_QUORUM),
        ]

        self.log("Testing multiple dcs, counters, each quorum reads")
        self._run_test_function_in_parallel(TestAccuracy.Validation.validate_counters, self.nodes, self.rf.values(), combinations),


class TestConsistency(Tester):

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12475',
                   flaky=True)
    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12141',
                   flaky=True)
    def short_read_test(self):
        """
        @jira_ticket CASSANDRA-9460
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)

        cluster.populate(3).start(wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0)

        normal_key = 'normal'
        reversed_key = 'reversed'

        # Repeat this test 10 times to make it more easy to spot a null pointer exception caused by a race, see CASSANDRA-9460
        for k in xrange(10):
            # insert 9 columns in two rows
            insert_columns(self, session, normal_key, 9)
            insert_columns(self, session, reversed_key, 9)

            # Delete 3 first columns (and 3 last columns, for the reversed version) with a different node dead each time
            for node, column_number_to_delete in zip(range(1, 4), range(3)):
                self.stop_node(node)
                self.delete(node, normal_key, column_number_to_delete)
                self.delete(node, reversed_key, 8 - column_number_to_delete)
                self.restart_node(node)

            # Query 3 firsts columns in normal order
            session = self.patient_cql_connection(node1, 'ks')
            query = SimpleStatement(
                'SELECT c, v FROM cf WHERE key=\'k{}\' LIMIT 3'.format(normal_key),
                consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))
            res = rows
            assert_length_equal(res, 3)

            # value 0, 1 and 2 have been deleted
            for i in xrange(1, 4):
                self.assertEqual('value{}'.format(i + 2), res[i - 1][1])

            # Query 3 firsts columns in reverse order
            session = self.patient_cql_connection(node1, 'ks')
            query = SimpleStatement(
                'SELECT c, v FROM cf WHERE key=\'k{}\' ORDER BY c DESC LIMIT 3'.format(reversed_key),
                consistency_level=ConsistencyLevel.QUORUM)
            rows = list(session.execute(query))
            res = rows
            assert_length_equal(res, 3)

            # value 6, 7 and 8 have been deleted
            for i in xrange(0, 3):
                self.assertEqual('value{}'.format(5 - i), res[i][1])

            session.execute('TRUNCATE cf')

    def short_read_delete_test(self):
        """ Test short reads ultimately leaving no columns alive [#4000] """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)

        cluster.populate(2).start(wait_other_notice=True)
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0)
        # insert 2 columns in one row
        insert_columns(self, session, 0, 2)

        # Delete the row while first node is dead
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')

        query = SimpleStatement('DELETE FROM cf WHERE key=\'k0\'', consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        node1.start(wait_other_notice=True)

        # Query first column
        session = self.patient_cql_connection(node1, 'ks')

        assert_none(session, "SELECT c, v FROM cf WHERE key=\'k0\' LIMIT 1", cl=ConsistencyLevel.QUORUM)

    def short_read_quorum_delete_test(self):
        """
        @jira_ticket CASSANDRA-8933
        """
        cluster = self.cluster
        # Consider however 3 nodes A, B, C (RF=3), and following sequence of operations (all done at QUORUM):

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)

        cluster.populate(3).start(wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY(id, v)) WITH read_repair_chance = 0.0")
        # we write 1 and 2 in a partition: all nodes get it.
        session.execute(SimpleStatement("INSERT INTO t (id, v) VALUES (0, 1)", consistency_level=ConsistencyLevel.ALL))
        session.execute(SimpleStatement("INSERT INTO t (id, v) VALUES (0, 2)", consistency_level=ConsistencyLevel.ALL))

        # we delete 1: only A and C get it.
        node2.flush()
        node2.stop(wait_other_notice=True)
        session.execute(SimpleStatement("DELETE FROM t WHERE id = 0 AND v = 1", consistency_level=ConsistencyLevel.QUORUM))
        node2.start(wait_other_notice=True)

        # we delete 2: only B and C get it.
        node1.flush()
        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2, 'ks')
        session.execute(SimpleStatement("DELETE FROM t WHERE id = 0 AND v = 2", consistency_level=ConsistencyLevel.QUORUM))
        node1.start(wait_other_notice=True)
        session = self.patient_cql_connection(node1, 'ks')

        # we read the first row in the partition (so with a LIMIT 1) and A and B answer first.
        node3.flush()
        node3.stop(wait_other_notice=True)
        assert_none(session, "SELECT * FROM t WHERE id = 0 LIMIT 1", cl=ConsistencyLevel.QUORUM)

    def readrepair_test(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})

        if DISABLE_VNODES:
            cluster.populate(2).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate(2, tokens=tokens).start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session, read_repair=1.0)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ONE)

        node2.start(wait_other_notice=True)

        # query everything to cause RR
        for n in xrange(0, 10000):
            query_c1c2(session, n, ConsistencyLevel.QUORUM)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been repaired
        session = self.patient_cql_connection(node2, keyspace='ks')
        for n in xrange(0, 10000):
            query_c1c2(session, n, ConsistencyLevel.ONE)

    def quorum_available_during_failure_test(self):
        CL = ConsistencyLevel.QUORUM
        RF = 3

        debug("Creating a ring")
        cluster = self.cluster
        if DISABLE_VNODES:
            cluster.populate(3).start()
        else:
            tokens = cluster.balanced_tokens(3)
            cluster.populate(3, tokens=tokens).start()
        node1, node2, node3 = cluster.nodelist()

        debug("Set to talk to node 2")
        session = self.patient_cql_connection(node2)
        create_ks(session, 'ks', RF)
        create_c1c2_table(self, session)

        debug("Generating some data")
        insert_c1c2(session, n=100, consistency=CL)

        debug("Taking down node1")
        node1.stop(wait_other_notice=True)

        debug("Reading back data.")
        for n in xrange(100):
            query_c1c2(session, n, CL)

    def stop_node(self, node_number):
        to_stop = self.cluster.nodes["node%d" % node_number]
        to_stop.flush()
        to_stop.stop(wait_other_notice=True)

    def delete(self, stopped_node_number, key, column):
        next_node = self.cluster.nodes["node%d" % (((stopped_node_number + 1) % 3) + 1)]
        session = self.patient_cql_connection(next_node, 'ks')

        # delete data for normal key
        query = 'BEGIN BATCH '
        query = query + 'DELETE FROM cf WHERE key=\'k%s\' AND c=\'c%06d\'; ' % (key, column)
        query = query + 'DELETE FROM cf WHERE key=\'k%s\' AND c=\'c2\'; ' % (key,)
        query = query + 'APPLY BATCH;'
        simple_query = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(simple_query)

    def restart_node(self, node_number):
        stopped_node = self.cluster.nodes["node%d" % node_number]
        stopped_node.start(wait_for_binary_proto=True, wait_other_notice=True)
