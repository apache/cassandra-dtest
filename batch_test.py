import sys
import time
import pytest
import logging

from cassandra import ConsistencyLevel, Timeout, Unavailable
from cassandra.query import SimpleStatement

from dtest import Tester, create_ks, mk_bman_path
from tools.assertions import (assert_all, assert_invalid, assert_one,
                              assert_unavailable)
from tools.jmxutils import (JolokiaAgent, make_mbean)

since = pytest.mark.since
ported_to_in_jvm = pytest.mark.ported_to_in_jvm
logger = logging.getLogger(__name__)


class TestBatch(Tester):

    def test_empty_batch_throws_no_error(self):
        """
        @jira_ticket CASSANDRA-10711
        """
        session = self.prepare()
        session.execute("""
            BEGIN BATCH
            APPLY BATCH;
        """)
        for node in self.cluster.nodelist():
            assert 0 == len(node.grep_log_for_errors())

    def test_counter_batch_accepts_counter_mutations(self):
        """ Test that counter batch accepts counter mutations """
        session = self.prepare()
        session.execute("""
            BEGIN COUNTER BATCH
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://bar.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 2 and url = 'http://baz.com'
            APPLY BATCH
        """)
        assert_all(session, "SELECT total FROM clicks", [[1], [1], [1]])

    def test_counter_batch_rejects_regular_mutations(self):
        """ Test that counter batch rejects non-counter mutations """
        session = self.prepare()
        err = "Cannot include non-counter statement in a counter batch"

        assert_invalid(session, """
            BEGIN COUNTER BATCH
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://bar.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 2 and url = 'http://baz.com'
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            APPLY BATCH
            """, matching=err)

    def test_logged_batch_accepts_regular_mutations(self):
        """ Test that logged batch accepts regular mutations """
        session = self.prepare()
        session.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        assert_all(session, "SELECT * FROM users", [[1, 'Will', 'Turner'], [0, 'Jack', 'Sparrow']])

    @since('3.0')
    def test_logged_batch_gcgs_below_threshold_single_table(self):
        """ Test that logged batch accepts regular mutations """
        session = self.prepare()

        # Single table
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")
        session.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        node1 = self.cluster.nodelist()[0]
        warning = node1.grep_log(r"Executing a LOGGED BATCH on table \[ks.users\], configured with a "
                                 "gc_grace_seconds of 0. The gc_grace_seconds is used to TTL "
                                 "batchlog entries, so setting gc_grace_seconds too low on tables "
                                 "involved in an atomic batch might cause batchlog entries to expire "
                                 "before being replayed.")
        logger.debug(warning)
        assert 1 == len(warning), "Cannot find the gc_grace_seconds warning message."

    @since('3.0')
    def test_logged_batch_gcgs_below_threshold_multi_table(self):
        """ Test that logged batch accepts regular mutations """
        session = self.prepare()
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")
        session.execute("""
            CREATE TABLE views (
                userid int,
                url text,
                PRIMARY KEY (userid, url)
             ) WITH gc_grace_seconds = 0;
         """)
        session.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO views (userid, url) VALUES (1, 'Will')
            APPLY BATCH
        """)
        node1 = self.cluster.nodelist()[0]
        warning = node1.grep_log(r"Executing a LOGGED BATCH on tables \[ks.views, ks.users\], configured with a "
                                 "gc_grace_seconds of 0. The gc_grace_seconds is used to TTL "
                                 "batchlog entries, so setting gc_grace_seconds too low on tables "
                                 "involved in an atomic batch might cause batchlog entries to expire "
                                 "before being replayed.")
        logger.debug(warning)
        assert 1 == len(warning), "Cannot find the gc_grace_seconds warning message."

    @since('3.0')
    def test_unlogged_batch_gcgs_below_threshold_should_not_print_warning(self):
        """ Test that logged batch accepts regular mutations """
        session = self.prepare()
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")
        session.execute("""
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        node1 = self.cluster.nodelist()[0]
        warning = node1.grep_log("setting a too low gc_grace_seconds on tables involved in an atomic batch")
        logger.debug(warning)
        assert 0 == len(warning), "Cannot find the gc_grace_seconds warning message."

    def test_logged_batch_rejects_counter_mutations(self):
        """ Test that logged batch rejects counter mutations """
        session = self.prepare()
        err = "Cannot include a counter statement in a logged batch"

        assert_invalid(session, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            APPLY BATCH
            """, matching=err)

    def test_unlogged_batch_accepts_regular_mutations(self):
        """ Test that unlogged batch accepts regular mutations """
        session = self.prepare()
        session.execute("""
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (2, 'Elizabeth', 'Swann')
            APPLY BATCH
        """)
        assert_all(session, "SELECT * FROM users", [[0, 'Jack', 'Sparrow'], [2, 'Elizabeth', 'Swann']])

    def test_unlogged_batch_rejects_counter_mutations(self):
        """ Test that unlogged batch rejects counter mutations """
        session = self.prepare()
        err = "Counter and non-counter mutations cannot exist in the same batch"

        assert_invalid(session, """
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (2, 'Elizabeth', 'Swann')
            UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'
            APPLY BATCH
            """, matching=err)

    def test_logged_batch_throws_uae(self):
        """ Test that logged batch throws UAE if there aren't enough live nodes """
        session = self.prepare(nodes=3)
        [node.stop(wait_other_notice=True) for node in self.cluster.nodelist()[1:]]
        session.consistency_level = ConsistencyLevel.ONE
        assert_unavailable(session.execute, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)

    def test_logged_batch_doesnt_throw_uae(self):
        """ Test that logged batch DOES NOT throw UAE if there are at least 2 live nodes """
        session = self.prepare(nodes=3)
        self.cluster.nodelist()[-1].stop(wait_other_notice=True)
        query = SimpleStatement("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, consistency_level=ConsistencyLevel.ONE)
        session.execute(query)

        self.cluster.nodelist()[-1].start(wait_for_binary_proto=True)
        assert_all(session, "SELECT * FROM users", [[1, 'Will', 'Turner'], [0, 'Jack', 'Sparrow']],
                   cl=ConsistencyLevel.ALL)

    def test_acknowledged_by_batchlog_not_set_when_batchlog_write_fails(self):
        """ Test that acknowledged_by_batchlog is False if batchlog can't be written """
        session = self.prepare(nodes=3, compression=False)
        # kill 2 of the 3 nodes (all the batchlog write candidates).
        [node.stop(gently=False) for node in self.cluster.nodelist()[1:]]
        self.assert_timedout(session, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, ConsistencyLevel.ONE, received_responses=0)

    def test_acknowledged_by_batchlog_set_when_batchlog_write_succeeds(self):
        """ Test that acknowledged_by_batchlog is True if batchlog can be written """
        session = self.prepare(nodes=3, compression=False)
        # kill one of the nodes so that batchlog will be written, but the write will fail.
        self.cluster.nodelist()[-1].stop(gently=False)
        self.assert_timedout(session, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, ConsistencyLevel.THREE, received_responses=2)

    def test_batch_uses_proper_timestamp(self):
        """ Test that each statement will be executed with provided BATCH timestamp """
        session = self.prepare()
        session.execute("""
            BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        query = "SELECT id, writetime(firstname), writetime(lastname) FROM users"
        assert_all(session, query, [[1, 1111111111111111, 1111111111111111], [0, 1111111111111111, 1111111111111111]])

    def test_only_one_timestamp_is_valid(self):
        """ Test that TIMESTAMP must not be used in the statements within the batch. """
        session = self.prepare()
        assert_invalid(session, """
            BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') USING TIMESTAMP 2
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, matching="Timestamp must be set either on BATCH or individual statements")

    def test_each_statement_in_batch_uses_proper_timestamp(self):
        """ Test that each statement will be executed with its own timestamp """
        session = self.prepare()
        session.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner') USING TIMESTAMP 1111111111111112
            APPLY BATCH
        """)

        query = "SELECT id, writetime(firstname), writetime(lastname) FROM users"
        assert_all(session, query, [[1, 1111111111111112, 1111111111111112], [0, 1111111111111111, 1111111111111111]])

    def test_multi_table_batch_for_10554(self):
        """ Test a batch on 2 tables having different columns, restarting the node afterwards, to reproduce CASSANDRA-10554 """
        session = self.prepare()

        # prepare() adds users and clicks but clicks is a counter table, so adding a random other table for this test.
        session.execute("""
            CREATE TABLE dogs (
                dogid int PRIMARY KEY,
                dogname text,
             );
         """)

        session.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO dogs (dogid, dogname) VALUES (0, 'Pluto')
            APPLY BATCH
        """)

        assert_one(session, "SELECT * FROM users", [0, 'Jack', 'Sparrow'])
        assert_one(session, "SELECT * FROM dogs", [0, 'Pluto'])

        # Flush and restart the node as it's how 10554 reproduces
        node1 = self.cluster.nodelist()[0]
        node1.flush()
        node1.stop()
        node1.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1, keyspace='ks')

        assert_one(session, "SELECT * FROM users", [0, 'Jack', 'Sparrow'])
        assert_one(session, "SELECT * FROM dogs", [0, 'Pluto'])

    @ported_to_in_jvm('4.0')
    @since('3.0', max_version='3.x')
    def test_logged_batch_compatibility_1(self):
        """
        @jira_ticket CASSANDRA-9673, test that logged batches still work with a mixed version cluster.

        Here we have one 3.0/3.x node and two 2.2 nodes and we send the batch request to the 3.0 node.
        """
        self._logged_batch_compatibility_test(0, 1, 'github:apache/cassandra-2.2', 2, 4)

    @ported_to_in_jvm('4.0')
    @since('3.0', max_version='3.x')
    def test_batchlog_replay_compatibility_1(self):
        """
        @jira_ticket CASSANDRA-9673, test that logged batches still work with a mixed version cluster.

        Here we have one 3.0/3.x node and two 2.2 nodes and we send the batch request to the 3.0 node.
        """
        self._batchlog_replay_compatibility_test(0, 1, 'github:apache/cassandra-2.2', 2, 4)

    @since('3.0', max_version='3.x')
    @pytest.mark.skipif(sys.platform == 'win32', reason='Windows production support only on 2.2+')
    def test_logged_batch_compatibility_2(self):
        """
        @jira_ticket CASSANDRA-9673, test that logged batches still work with a mixed version cluster.

        Here we have one 3.0/3.x node and two 2.1 nodes and we send the batch request to the 3.0 node.
        """
        self._logged_batch_compatibility_test(0, 1, 'github:apache/cassandra-2.1', 2, 3)

    @since('3.0', max_version='3.x')
    @pytest.mark.skipif(sys.platform == 'win32', reason='Windows production support only on 2.2+')
    def test_logged_batch_compatibility_3(self):
        """
        @jira_ticket CASSANDRA-9673, test that logged batches still work with a mixed version cluster.

        Here we have two 3.0/3.x nodes and one 2.1 node and we send the batch request to the 3.0 node.
        """
        self._logged_batch_compatibility_test(0, 2, 'github:apache/cassandra-2.1', 1, 3)

    @ported_to_in_jvm('4.0')
    @since('3.0', max_version='3.x')
    def test_logged_batch_compatibility_4(self):
        """
        @jira_ticket CASSANDRA-9673, test that logged batches still work with a mixed version cluster.

        Here we have two 3.0/3.x nodes and one 2.2 node and we send the batch request to the 2.2 node.
        """
        self._logged_batch_compatibility_test(2, 2, 'github:apache/cassandra-2.2', 1, 4)

    @ported_to_in_jvm('4.0')
    @since('3.0', max_version='3.x')
    def test_batchlog_replay_compatibility_4(self):
        """
        @jira_ticket CASSANDRA-9673, test that logged batches still work with a mixed version cluster.

        Here we have two 3.0/3.x nodes and one 2.2 node and we send the batch request to the 2.2 node.
        """
        self._batchlog_replay_compatibility_test(2, 2, 'github:apache/cassandra-2.2', 1, 4)

    @since('3.0', max_version='3.x')
    @pytest.mark.skipif(sys.platform == 'win32', reason='Windows production support only on 2.2+')
    def test_logged_batch_compatibility_5(self):
        """
        @jira_ticket CASSANDRA-9673, test that logged batches still work with a mixed version cluster.

        Here we have two 3.0/3.x nodes and one 2.1 node and we send the batch request to the 2.1 node.
        """
        self._logged_batch_compatibility_test(2, 2, 'github:apache/cassandra-2.1', 1, 3)

    def _logged_batch_compatibility_test(self, coordinator_idx, current_nodes, previous_version, previous_nodes, protocol_version):
        session = self.prepare_mixed(coordinator_idx, current_nodes, previous_version, previous_nodes, protocol_version=protocol_version)
        query = SimpleStatement("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, consistency_level=ConsistencyLevel.ALL)
        session.execute(query)
        rows = session.execute("SELECT id, firstname, lastname FROM users")
        res = sorted(rows)
        assert [[0, 'Jack', 'Sparrow'], [1, 'Will', 'Turner']], [list(res[0]) == list(res[1])]

    def _batchlog_replay_compatibility_test(self, coordinator_idx, current_nodes, previous_version, previous_nodes, protocol_version):
        session = self.prepare_mixed(coordinator_idx, current_nodes, previous_version, previous_nodes,
                                     protocol_version=protocol_version, install_byteman=True)

        coordinator = self.cluster.nodelist()[coordinator_idx]
        coordinator.byteman_submit([mk_bman_path('fail_after_batchlog_write.btm')])
        logger.debug("Injected byteman scripts to enable batchlog replay {}".format(coordinator.name))

        query = """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """
        session.execute(query)

        # batchlog replay skips over all entries that are younger than
        # 2 * write_request_timeout_in_ms ms: 1x timeout for all mutations to be written,
        # and another 1x timeout for batch remove mutation to be received.
        delay = 2 * coordinator.get_conf_option('write_request_timeout_in_ms') / 1000.0 + 1
        logger.debug('Sleeping for {}s for the batches to not be skipped'.format(delay))
        time.sleep(delay)

        total_batches_replayed = 0
        blm = make_mbean('db', type='BatchlogManager')

        for n in self.cluster.nodelist():
            if n == coordinator:
                continue

            with JolokiaAgent(n) as jmx:
                logger.debug('Forcing batchlog replay for {}'.format(n.name))
                jmx.execute_method(blm, 'forceBatchlogReplay')
                batches_replayed = jmx.read_attribute(blm, 'TotalBatchesReplayed')
                logger.debug('{} batches replayed on node {}'.format(batches_replayed, n.name))
                total_batches_replayed += batches_replayed

        assert total_batches_replayed >= 2

        for node in self.cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node, protocol_version=protocol_version)
            rows = sorted(session.execute('SELECT id, firstname, lastname FROM ks.users'))
            assert [[0, 'Jack', 'Sparrow'], [1, 'Will', 'Turner']], [list(rows[0]) == list(rows[1])]

    def assert_timedout(self, session, query, cl, acknowledged_by=None,
                        received_responses=None):
        try:
            statement = SimpleStatement(query, consistency_level=cl)
            session.execute(statement, timeout=None)
        except Timeout as e:
            if received_responses is not None:
                msg = "Expecting received_responses to be {}, got: {}".format(
                    received_responses, e.received_responses,)
                assert e.received_responses == received_responses, msg
        except Unavailable as e:
            if received_responses is not None:
                msg = "Expecting alive_replicas to be {}, got: {}".format(
                    received_responses, e.alive_replicas,)
                assert e.alive_replicas == received_responses, msg
        except Exception as e:
            assert False, "Expecting TimedOutException, got:" + str(e)
        else:
            assert False, "Expecting TimedOutException but no exception was raised"

    def prepare(self, nodes=1, compression=True, version=None, protocol_version=None, install_byteman=False):
        if version:
            self.cluster.set_install_dir(version=version)
            logger.debug("Set cassandra dir to {}".format(self.cluster.get_install_dir()))

        self.cluster.populate(nodes, install_byteman=install_byteman)
        self.cluster.start()

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        self.create_schema(session, nodes)
        return session

    def create_schema(self, session, rf):
        logger.debug('Creating schema...')
        create_ks(session, 'ks', rf)

        session.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
             );
         """)

        session.execute("""
            CREATE TABLE users (
                id int,
                firstname text,
                lastname text,
                PRIMARY KEY (id)
             );
         """)

        time.sleep(.5)

    def prepare_mixed(self, coordinator_idx, current_nodes, previous_version, previous_nodes, compression=True,
                      protocol_version=None, install_byteman=False):
        logger.debug("Testing with {} node(s) at version '{}', {} node(s) at current version"
              .format(previous_nodes, previous_version, current_nodes))

        # start a cluster using the previous version
        self.prepare(previous_nodes + current_nodes, compression, previous_version, protocol_version=protocol_version,
                     install_byteman=install_byteman)

        # then upgrade the current nodes to the current version but not the previous nodes
        for i in range(current_nodes):
            node = self.cluster.nodelist()[i]
            self.upgrade_node(node)

        session = self.patient_exclusive_cql_connection(self.cluster.nodelist()[coordinator_idx],
                                                        protocol_version=protocol_version)
        session.execute('USE ks')
        return session

    def upgrade_node(self, node):
        """
        Upgrade a node to the current version
        """
        logger.debug('Upgrading {} to the current version'.format(node.name))
        logger.debug('Shutting down {}'.format(node.name))
        node.stop(wait_other_notice=False)
        self.set_node_to_current_version(node)
        logger.debug("Set cassandra dir for {} to {}".format(node.name, node.get_install_dir()))
        # Restart nodes on new version
        logger.debug('Starting {} on new version ({})'.format(node.name, node.get_cassandra_version()))
        node.start(wait_for_binary_proto=True, jvm_args=['-Dcassandra.disable_max_protocol_auto_override=true'])
