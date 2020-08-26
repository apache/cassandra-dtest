import os
import random
import re
import time
import uuid
import pytest
import logging

from flaky import flaky

from cassandra import InvalidRequest
from cassandra.concurrent import (execute_concurrent,
                                  execute_concurrent_with_args)
from cassandra.protocol import ConfigurationException
from cassandra.query import BatchStatement, SimpleStatement

from dtest import Tester, create_ks, create_cf
from tools.assertions import assert_bootstrap_state, assert_invalid, assert_none, assert_one, assert_row_count, \
    assert_length_equal, assert_all
from tools.data import block_until_index_is_built, rows_to_list
from tools.misc import new_node

since = pytest.mark.since
logger = logging.getLogger(__name__)

class TestSecondaryIndexes(Tester):

    @staticmethod
    def _index_sstables_files(node, keyspace, table, index):
        files = []
        for data_dir in node.data_directories():
            data_dir = os.path.join(data_dir, keyspace)
            base_tbl_dir = os.path.join(data_dir, [s for s in os.listdir(data_dir) if s.startswith(table)][0])
            index_sstables_dir = os.path.join(base_tbl_dir, '.' + index)
            files.extend(os.listdir(index_sstables_dir))
        return set(files)

    def test_data_created_before_index_not_returned_in_where_query(self):
        """
        @jira_ticket CASSANDRA-3367
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)

        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        create_cf(session, 'users', columns=columns)

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")

        # create index
        session.execute("CREATE INDEX gender_key ON users (gender);")
        session.execute("CREATE INDEX state_key ON users (state);")
        session.execute("CREATE INDEX birth_year_key ON users (birth_year);")

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        assert_row_count(session, "users", 4)

        assert_row_count(session, "users", 2, "state='TX'")

        assert_row_count(session, "users", 1, "state='CA'")

    def test_low_cardinality_indexes(self):
        """
        Checks that low-cardinality secondary index subqueries are executed
        concurrently
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        session.max_trace_wait = 120
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.cf (a text PRIMARY KEY, b text);")
        session.execute("CREATE INDEX b_index ON ks.cf (b);")
        num_rows = 100
        for i in range(num_rows):
            indexed_value = i % (num_rows // 3)
            # use the same indexed value three times
            session.execute("INSERT INTO ks.cf (a, b) VALUES ('{a}', '{b}');"
                            .format(a=i, b=indexed_value))

        cluster.flush()

        def check_trace_events(trace):
            # we should see multiple requests get enqueued prior to index scan
            # execution happening

            # Look for messages like:
            #         Submitting range requests on 769    ranges with a concurrency of 769    (0.0070312 rows per range expected)
            regex = r"Submitting range requests on [0-9]+ ranges with a concurrency of (\d+) \(([0-9.]+) rows per range expected\)"

            for event in trace.events:
                desc = event.description
                match = re.match(regex, desc)
                if match:
                    concurrency = int(match.group(1))
                    expected_per_range = float(match.group(2))
                    assert concurrency > 1, "Expected more than 1 concurrent range request, got %d" % concurrency
                    assert expected_per_range > 0
                    break
            else:
                self.fail("Didn't find matching trace event")

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1';")
        result = session.execute(query, trace=True)
        assert 3 == len(list(result))
        check_trace_events(result.get_query_trace())

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1' LIMIT 100;")
        result = session.execute(query, trace=True)
        assert 3 == len(list(result))
        check_trace_events(result.get_query_trace())

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1' LIMIT 3;")
        result = session.execute(query, trace=True)
        assert 3 == len(list(result))
        check_trace_events(result.get_query_trace())

        for limit in (1, 2):
            result = list(session.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT %d;" % (limit,)))
            assert limit == len(result)

    @flaky(3)
    def test_6924_dropping_ks(self):
        """
        @jira_ticket CASSANDRA-6924
        @jira_ticket CASSANDRA-11729

        Data inserted immediately after dropping and recreating a
        keyspace with an indexed column familiy is not included
        in the index.

        This test can be flaky due to concurrency issues during
        schema updates. See CASSANDRA-11729 for an explanation.
        """
        # Reproducing requires at least 3 nodes:
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        # We have to wait up to RING_DELAY + 1 seconds for the MV Builder task
        # to complete, to prevent schema concurrency issues with the drop
        # keyspace calls that come later. See CASSANDRA-11729.
        if self.cluster.version() > '3.0':
            self.cluster.wait_for_any_log('Completed submission of build tasks for any materialized views',
                                          timeout=35, filename='debug.log')

        # This only occurs when dropping and recreating with
        # the same name, so loop through this test a few times:
        for i in range(10):
            logger.debug("round %s" % i)
            try:
                session.execute("DROP KEYSPACE ks")
            except (ConfigurationException, InvalidRequest):
                pass

            create_ks(session, 'ks', 1)
            session.execute("CREATE TABLE ks.cf (key text PRIMARY KEY, col1 text);")
            session.execute("CREATE INDEX on ks.cf (col1);")

            for r in range(10):
                stmt = "INSERT INTO ks.cf (key, col1) VALUES ('%s','asdf');" % r
                session.execute(stmt)

            self.wait_for_schema_agreement(session)

            rows = session.execute("select count(*) from ks.cf WHERE col1='asdf'")
            count = rows[0][0]
            assert count == 10

    @flaky
    def test_6924_dropping_cf(self):
        """
        @jira_ticket CASSANDRA-6924

        Data inserted immediately after dropping and recreating an
        indexed column family is not included in the index.
        """
        # Reproducing requires at least 3 nodes:
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        create_ks(session, 'ks', 1)

        # This only occurs when dropping and recreating with
        # the same name, so loop through this test a few times:
        for i in range(10):
            logger.debug("round %s" % i)
            try:
                session.execute("DROP COLUMNFAMILY ks.cf")
            except InvalidRequest:
                pass

            session.execute("CREATE TABLE ks.cf (key text PRIMARY KEY, col1 text);")
            session.execute("CREATE INDEX on ks.cf (col1);")

            for r in range(10):
                stmt = "INSERT INTO ks.cf (key, col1) VALUES ('%s','asdf');" % r
                session.execute(stmt)

            self.wait_for_schema_agreement(session)

            rows = session.execute("select count(*) from ks.cf WHERE col1='asdf'")
            count = rows[0][0]
            assert count == 10

    def test_8280_validate_indexed_values(self):
        """
        @jira_ticket CASSANDRA-8280

        Reject inserts & updates where values of any indexed
        column is > 64k
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        create_ks(session, 'ks', 1)

        self.insert_row_with_oversize_value("CREATE TABLE %s(a int, b int, c text, PRIMARY KEY (a))",
                                            "CREATE INDEX ON %s(c)",
                                            "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)",
                                            session)

        self.insert_row_with_oversize_value("CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b))",
                                            "CREATE INDEX ON %s(b)",
                                            "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)",
                                            session)

        self.insert_row_with_oversize_value("CREATE TABLE %s(a text, b int, c int, PRIMARY KEY ((a, b)))",
                                            "CREATE INDEX ON %s(a)",
                                            "INSERT INTO %s (a, b, c) VALUES (?, 0, 0)",
                                            session)

    @since("2.0", max_version="3.X")
    def test_8280_validate_indexed_values_compact(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        create_ks(session, 'ks', 1)
        self.insert_row_with_oversize_value("CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) WITH COMPACT STORAGE",
                                            "CREATE INDEX ON %s(b)",
                                            "INSERT INTO %s (a, b) VALUES (0, ?)",
                                            session)

    def insert_row_with_oversize_value(self, create_table_cql, create_index_cql, insert_cql, session):
        """ Validate two variations of the supplied insert statement, first
        as it is and then again transformed into a conditional statement
        """
        table_name = "table_" + str(int(round(time.time() * 1000)))
        session.execute(create_table_cql % table_name)
        session.execute(create_index_cql % table_name)
        value = "X" * 65536
        self._assert_invalid_request(session, insert_cql % table_name, value)
        self._assert_invalid_request(session, (insert_cql % table_name) + ' IF NOT EXISTS', value)

    def _assert_invalid_request(self, session, insert_cql, value):
        """ Perform two executions of the supplied statement, as a
        single statement and again as part of a batch
        """
        prepared = session.prepare(insert_cql)
        self._execute_and_fail(lambda: session.execute(prepared, [value]), insert_cql)
        batch = BatchStatement()
        batch.add(prepared, [value])
        self._execute_and_fail(lambda: session.execute(batch), insert_cql)

    def _execute_and_fail(self, operation, cql_string):
        try:
            operation()
            self.fail("Expecting query {} to be invalid".format(cql_string))
        except AssertionError as e:
            raise e
        except InvalidRequest:
            pass

    def wait_for_schema_agreement(self, session):
        if not session.cluster.control_connection.wait_for_schema_agreement(wait_time=120):
            raise AssertionError("Failed to reach schema agreement")

    @since('3.0')
    def test_manual_rebuild_index(self):
        """
        asserts that new sstables are written when rebuild_index is called from nodetool
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        node1.stress(['write', 'n=50K', 'no-warmup'])
        session.execute("use keyspace1;")
        lookup_value = session.execute('select "C0" from standard1 limit 1')[0].C0
        session.execute('CREATE INDEX ix_c0 ON standard1("C0");')

        block_until_index_is_built(node1, session, 'keyspace1', 'standard1', 'ix_c0')

        stmt = session.prepare('select * from standard1 where "C0" = ?')
        assert 1 == len(list(session.execute(stmt, [lookup_value])))
        before_files = self._index_sstables_files(node1, 'keyspace1', 'standard1', 'ix_c0')

        node1.nodetool("rebuild_index keyspace1 standard1 ix_c0")
        block_until_index_is_built(node1, session, 'keyspace1', 'standard1', 'ix_c0')

        after_files = self._index_sstables_files(node1, 'keyspace1', 'standard1', 'ix_c0')
        assert before_files != after_files
        assert 1 == len(list(session.execute(stmt, [lookup_value])))

        # verify that only the expected row is present in the build indexes table
        assert 1 == len(list(session.execute("""SELECT * FROM system."IndexInfo";""")))

    @since('4.0')
    def test_failing_manual_rebuild_index(self):
        """
        @jira_ticket CASSANDRA-10130

        Tests the management of index status during manual index rebuilding failures.
        """

        cluster = self.cluster
        cluster.populate(1, install_byteman=True).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'k', 1)
        session.execute("CREATE TABLE k.t (k int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX idx ON k.t(v)")
        session.execute("INSERT INTO k.t(k, v) VALUES (0, 1)")
        session.execute("INSERT INTO k.t(k, v) VALUES (2, 3)")

        # Verify that the index is marked as built and it can answer queries and accept writes
        assert_one(session, """SELECT table_name, index_name FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_length_equal(node.grep_log('became queryable after successful build'), 1)
        assert_length_equal(node.grep_log('registered and writable'), 1)
        session.execute("INSERT INTO k.t(k, v) VALUES (1, 1)")
        assert_all(session, "SELECT k FROM k.t WHERE v = 1", [[0], [1]], ignore_order=True)

        # Simulate a failing index rebuild
        before_files = self._index_sstables_files(node, 'k', 't', 'idx')
        mark = node.mark_log()
        node.byteman_submit(['./byteman/index_build_failure.btm'])
        with pytest.raises(Exception):
            node.nodetool("rebuild_index k t idx")
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that the index is not built, not marked as built, and it still can answer queries and accept writes
        assert before_files == after_files
        assert_none(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""")
        assert_length_equal(node.grep_log('became queryable', from_mark=mark), 0)
        assert_length_equal(node.grep_log('became writable', from_mark=mark), 0)
        session.execute("INSERT INTO k.t(k, v) VALUES (2, 1)")
        assert_all(session, "SELECT k FROM k.t WHERE v = 1", [[0], [1], [2]], ignore_order=True)

        # Restart the node to trigger the scheduled index rebuild
        before_files = after_files
        node.nodetool('drain')
        node.stop()
        mark = node.mark_log()
        cluster.start()
        session = self.patient_cql_connection(node)
        session.execute("USE k")
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that the index is rebuilt, marked as built, and it still can answer queries and accept writes
        assert before_files != after_files
        assert_one(session, """SELECT table_name, index_name FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_length_equal(node.grep_log('became queryable after successful build', from_mark=mark), 1)
        assert_length_equal(node.grep_log('registered and writable', from_mark=mark), 1)
        session.execute("INSERT INTO k.t(k, v) VALUES (3, 1)")
        assert_all(session, "SELECT k FROM k.t WHERE v = 1", [[0], [1], [2], [3]], ignore_order=True)

        # Simulate another failing index rebuild
        before_files = after_files
        mark = node.mark_log()
        node.byteman_submit(['./byteman/index_build_failure.btm'])
        with pytest.raises(Exception):
            node.nodetool("rebuild_index k t idx")
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that the index is not built, not marked as built, and it still can answer queries and accept writes
        assert before_files == after_files
        assert_none(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""")
        assert_length_equal(node.grep_log('became queryable', from_mark=mark), 0)
        assert_length_equal(node.grep_log('became writable', from_mark=mark), 0)
        session.execute("INSERT INTO k.t(k, v) VALUES (4, 1)")
        assert_all(session, "SELECT k FROM k.t WHERE v = 1", [[0], [1], [2], [3], [4]], ignore_order=True)

        # Successfully rebuild the index
        before_files = after_files
        node.nodetool("rebuild_index k t idx")
        cluster.wait_for_compactions()
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')

        # Verify that the index is rebuilt, marked as built, and it still can answer queries and accept writes
        assert before_files != after_files
        assert_one(session, """SELECT table_name, index_name FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_length_equal(node.grep_log('became queryable', from_mark=mark), 0)
        assert_length_equal(node.grep_log('became writable', from_mark=mark), 0)
        session.execute("INSERT INTO k.t(k, v) VALUES (5, 1)")
        assert_all(session, "SELECT k FROM k.t WHERE v = 1", [[0], [1], [2], [3], [4], [5]], ignore_order=True)

    @since('4.0')
    def test_drop_index_while_building(self):
        """
        asserts that indexes deleted before they have been completely build are invalidated and not built after restart
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        # Create some thousands of rows to guarantee a long index building
        node.stress(['write', 'n=50K', 'no-warmup'])
        session.execute("USE keyspace1")

        # Create an index and immediately drop it, without waiting for index building
        session.execute('CREATE INDEX idx ON standard1("C0")')
        session.execute('DROP INDEX idx')
        cluster.wait_for_compactions()

        # Check that the index is not marked as built nor queryable
        assert_none(session, """SELECT * FROM system."IndexInfo" WHERE table_name='keyspace1'""")
        assert_invalid(session,
                       'SELECT * FROM standard1 WHERE "C0" = 0x00',
                       'Cannot execute this query as it might involve data filtering')

        # Restart the node to trigger any eventual unexpected index rebuild
        node.nodetool('drain')
        node.stop()
        cluster.start()
        session = self.patient_cql_connection(node)
        session.execute("USE keyspace1")

        # The index should remain not built nor queryable after restart
        assert_none(session, """SELECT * FROM system."IndexInfo" WHERE table_name='keyspace1'""")
        assert_invalid(session,
                       'SELECT * FROM standard1 WHERE "C0" = 0x00',
                       'Cannot execute this query as it might involve data filtering')

    @since('4.0')
    def test_index_is_not_rebuilt_at_restart(self):
        """
        @jira_ticket CASSANDRA-13725

        Tests the index is not rebuilt at restart if already built.
        """

        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, 'k', 1)
        session.execute("CREATE TABLE k.t (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO k.t(k, v) VALUES (0, 1)")

        logger.debug("Create the index")
        session.execute("CREATE INDEX idx ON k.t(v)")
        block_until_index_is_built(node, session, 'k', 't', 'idx')
        before_files = self._index_sstables_files(node, 'k', 't', 'idx')

        logger.debug("Verify the index is marked as built and it can be queried")
        assert_one(session, """SELECT table_name, index_name FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_one(session, "SELECT * FROM k.t WHERE v = 1", [0, 1])

        logger.debug("Restart the node and verify the index build is not submitted")
        node.stop()
        node.start(wait_for_binary_proto=True)
        after_files = self._index_sstables_files(node, 'k', 't', 'idx')
        assert before_files == after_files

        logger.debug("Verify the index is still marked as built and it can be queried")
        session = self.patient_cql_connection(node)
        assert_one(session, """SELECT table_name, index_name FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_one(session, "SELECT * FROM k.t WHERE v = 1", [0, 1])

    def test_multi_index_filtering_query(self):
        """
        asserts that having multiple indexes that cover all predicates still requires ALLOW FILTERING to also be present
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("USE ks;")
        session.execute("CREATE TABLE tbl (id uuid primary key, c0 text, c1 text, c2 text);")
        session.execute("CREATE INDEX ix_tbl_c0 ON tbl(c0);")
        session.execute("CREATE INDEX ix_tbl_c1 ON tbl(c1);")
        session.execute("INSERT INTO tbl (id, c0, c1, c2) values (uuid(), 'a', 'b', 'c');")
        session.execute("INSERT INTO tbl (id, c0, c1, c2) values (uuid(), 'a', 'b', 'c');")
        session.execute("INSERT INTO tbl (id, c0, c1, c2) values (uuid(), 'q', 'b', 'c');")
        session.execute("INSERT INTO tbl (id, c0, c1, c2) values (uuid(), 'a', 'e', 'f');")
        session.execute("INSERT INTO tbl (id, c0, c1, c2) values (uuid(), 'a', 'e', 'f');")

        rows = list(session.execute("SELECT * FROM tbl WHERE c0 = 'a';"))
        assert 4 == len(rows)

        stmt = "SELECT * FROM tbl WHERE c0 = 'a' AND c1 = 'b';"
        assert_invalid(session, stmt, "Cannot execute this query as it might involve data filtering and thus may have "
                                      "unpredictable performance. If you want to execute this query despite the "
                                      "performance unpredictability, use ALLOW FILTERING")

        rows = list(session.execute("SELECT * FROM tbl WHERE c0 = 'a' AND c1 = 'b' ALLOW FILTERING;"))
        assert 2 == len(rows)

    @since('3.0')
    def test_only_coordinator_chooses_index_for_query(self):
        """
        Checks that the index to use is selected (once) on the coordinator and
        included in the serialized command sent to the replicas.
        @jira_ticket CASSANDRA-10215
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node3)
        session.max_trace_wait = 120
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.cf (a text PRIMARY KEY, b text);")
        session.execute("CREATE INDEX b_index ON ks.cf (b);")
        num_rows = 100
        for i in range(num_rows):
            indexed_value = i % (num_rows // 3)
            # use the same indexed value three times
            session.execute("INSERT INTO ks.cf (a, b) VALUES ('{a}', '{b}');"
                            .format(a=i, b=indexed_value))

        cluster.flush()

        def check_trace_events(trace, regex, expected_matches, on_failure):
            """
            Check for the presence of certain trace events. expected_matches should be a list of
            tuple(source, min_count, max_count) indicating that of all the trace events for the
            the source, the supplied regex should match at least min_count trace messages & at
            most max_count messages. E.g. [(127.0.0.1, 1, 10), (127.0.0.2, 0, 0)]
            indicates that the regex should match at least 1, but no more than 10 events emitted
            by node1, and that no messages emitted by node2 should match.
            """
            match_counts = {}
            for event_source, min_matches, max_matches in expected_matches:
                match_counts[event_source] = 0

            for event in trace.events:
                desc = event.description
                match = re.match(regex, desc)
                if match:
                    if event.source in match_counts:
                        match_counts[event.source] += 1
            for event_source, min_matches, max_matches in expected_matches:
                if match_counts[event_source] < min_matches or match_counts[event_source] > max_matches:
                    on_failure(trace, regex, expected_matches, match_counts, event_source, min_matches, max_matches)

        def halt_on_failure(trace, regex, expected_matches, match_counts, event_source, min_expected, max_expected):
            self.fail("Expected to find between {min} and {max} trace events matching {pattern} from {source}, "
                      "but actually found {actual}. (Full counts: {all})"
                      .format(min=min_expected, max=max_expected, pattern=regex, source=event_source,
                              actual=match_counts[event_source], all=match_counts))

        def retry_on_failure(trace, regex, expected_matches, match_counts, event_source, min_expected, max_expected):
            logger.debug("Trace event inspection did not match expected, sleeping before re-fetching trace events. "
                  "Expected: {expected} Actual: {actual}".format(expected=expected_matches, actual=match_counts))
            time.sleep(2)
            trace.populate(max_wait=2.0)
            check_trace_events(trace, regex, expected_matches, halt_on_failure)

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1';")
        result = session.execute(query, trace=True)
        assert 3 == len(list(result))

        trace = result.get_query_trace()

        # we have forced node3 to act as the coordinator for
        # all requests by using an exclusive connection, so
        # only node3 should select the index to use
        check_trace_events(trace,
                           "Index mean cardinalities are b_index:[0-9]*. Scanning with b_index.",
                           [("127.0.0.1", 0, 0), ("127.0.0.2", 0, 0), ("127.0.0.3", 1, 1)],
                           retry_on_failure)
        # check that the index is used on each node, really we only care that the matching
        # message appears on every node, so the max count is not important
        check_trace_events(trace,
                           "Executing read on ks.cf using index b_index",
                           [("127.0.0.1", 1, 200), ("127.0.0.2", 1, 200), ("127.0.0.3", 1, 200)],
                           retry_on_failure)

    @pytest.mark.vnodes
    def test_query_indexes_with_vnodes(self):
        """
        Verifies correct query behaviour in the presence of vnodes
        @jira_ticket CASSANDRA-11104
        """
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.compact_table (a int PRIMARY KEY, b int);")
        session.execute("CREATE INDEX keys_index ON ks.compact_table (b);")
        session.execute("CREATE TABLE ks.regular_table (a int PRIMARY KEY, b int)")
        session.execute("CREATE INDEX composites_index on ks.regular_table (b)")

        for node in cluster.nodelist():
            block_until_index_is_built(node, session, 'ks', 'regular_table', 'composites_index')

        insert_args = [(i, i % 2) for i in range(100)]
        execute_concurrent_with_args(session,
                                     session.prepare("INSERT INTO ks.compact_table (a, b) VALUES (?, ?)"),
                                     insert_args)
        execute_concurrent_with_args(session,
                                     session.prepare("INSERT INTO ks.regular_table (a, b) VALUES (?, ?)"),
                                     insert_args)

        res = session.execute("SELECT * FROM ks.compact_table WHERE b = 0")
        assert len(rows_to_list(res)) == 50
        res = session.execute("SELECT * FROM ks.regular_table WHERE b = 0")
        assert len(rows_to_list(res)) == 50


class TestSecondaryIndexesOnCollections(Tester):

    def test_tuple_indexes(self):
        """
        Checks that secondary indexes on tuples work for querying
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'tuple_index_test', 1)
        session.execute("use tuple_index_test")
        session.execute("""
            CREATE TABLE simple_with_tuple (
                id uuid primary key,
                normal_col int,
                single_tuple tuple<int>,
                double_tuple tuple<int, int>,
                triple_tuple tuple<int, int, int>,
                nested_one tuple<int, tuple<int, int>>
            )""")
        cmds = [("""insert into simple_with_tuple
                        (id, normal_col, single_tuple, double_tuple, triple_tuple, nested_one)
                    values
                        (uuid(), {0}, ({0}), ({0},{0}), ({0},{0},{0}), ({0},({0},{0})))""".format(n), ())
                for n in range(50)]

        results = execute_concurrent(session, cmds * 5, raise_on_first_error=True, concurrency=200)

        for (success, result) in results:
            assert success, "didn't get success on insert: {0}".format(result)

        session.execute("CREATE INDEX idx_single_tuple ON simple_with_tuple(single_tuple);")
        session.execute("CREATE INDEX idx_double_tuple ON simple_with_tuple(double_tuple);")
        session.execute("CREATE INDEX idx_triple_tuple ON simple_with_tuple(triple_tuple);")
        session.execute("CREATE INDEX idx_nested_tuple ON simple_with_tuple(nested_one);")
        time.sleep(10)

        # check if indexes work on existing data
        for n in range(50):
            assert 5 == len(list(session.execute("select * from simple_with_tuple where single_tuple = ({0});".format(n))))
            assert 0 == len(list(session.execute("select * from simple_with_tuple where single_tuple = (-1);".format(n))))
            assert 5 == len(list(session.execute("select * from simple_with_tuple where double_tuple = ({0},{0});".format(n))))
            assert 0 == len(list(session.execute("select * from simple_with_tuple where double_tuple = ({0},-1);".format(n))))
            assert 5 == len(list(session.execute("select * from simple_with_tuple where triple_tuple = ({0},{0},{0});".format(n))))
            assert 0 == len(list(session.execute("select * from simple_with_tuple where triple_tuple = ({0},{0},-1);".format(n))))
            assert 5 == len(list(session.execute("select * from simple_with_tuple where nested_one = ({0},({0},{0}));".format(n))))
            assert 0 == len(list(session.execute("select * from simple_with_tuple where nested_one = ({0},({0},-1));".format(n))))

        # check if indexes work on new data inserted after index creation
        results = execute_concurrent(session, cmds * 3, raise_on_first_error=True, concurrency=200)
        for (success, result) in results:
            assert success, "didn't get success on insert: {0}".format(result)
        time.sleep(5)
        for n in range(50):
            assert 8 == len(list(session.execute("select * from simple_with_tuple where single_tuple = ({0});".format(n))))
            assert 8 == len(list(session.execute("select * from simple_with_tuple where double_tuple = ({0},{0});".format(n))))
            assert 8 == len(list(session.execute("select * from simple_with_tuple where triple_tuple = ({0},{0},{0});".format(n))))
            assert 8 == len(list(session.execute("select * from simple_with_tuple where nested_one = ({0},({0},{0}));".format(n))))

        # check if indexes work on mutated data
        for n in range(5):
            rows = session.execute("select * from simple_with_tuple where single_tuple = ({0});".format(n))
            for row in rows:
                session.execute("update simple_with_tuple set single_tuple = (-999) where id = {0}".format(row.id))

            rows = session.execute("select * from simple_with_tuple where double_tuple = ({0},{0});".format(n))
            for row in rows:
                session.execute("update simple_with_tuple set double_tuple = (-999,-999) where id = {0}".format(row.id))

            rows = session.execute("select * from simple_with_tuple where triple_tuple = ({0},{0},{0});".format(n))
            for row in rows:
                session.execute("update simple_with_tuple set triple_tuple = (-999,-999,-999) where id = {0}".format(row.id))

            rows = session.execute("select * from simple_with_tuple where nested_one = ({0},({0},{0}));".format(n))
            for row in rows:
                session.execute("update simple_with_tuple set nested_one = (-999,(-999,-999)) where id = {0}".format(row.id))

        for n in range(5):
            assert 0 == len(list(session.execute("select * from simple_with_tuple where single_tuple = ({0});".format(n))))
            assert 0 == len(list(session.execute("select * from simple_with_tuple where double_tuple = ({0},{0});".format(n))))
            assert 0 == len(list(session.execute("select * from simple_with_tuple where triple_tuple = ({0},{0},{0});".format(n))))
            assert 0 == len(list(session.execute("select * from simple_with_tuple where nested_one = ({0},({0},{0}));".format(n))))

        assert 40 == len(list(session.execute("select * from simple_with_tuple where single_tuple = (-999);")))
        assert 40 == len(list(session.execute("select * from simple_with_tuple where double_tuple = (-999,-999);")))
        assert 40 == len(list(session.execute("select * from simple_with_tuple where triple_tuple = (-999,-999,-999);")))
        assert 40 == len(list(session.execute("select * from simple_with_tuple where nested_one = (-999,(-999,-999));")))

    def test_list_indexes(self):
        """
        Checks that secondary indexes on lists work for querying.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'list_index_search', 1)

        stmt = ("CREATE TABLE list_index_search.users ("
                "user_id uuid PRIMARY KEY,"
                "email text,"
                "uuids list<uuid>"
                ");")
        session.execute(stmt)

        # add index and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on list_index_search.users (uuids);"
        session.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = list(session.execute(stmt))
        assert 0 == len(row)

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO list_index_search.users (user_id, email)"
                "values ({user_id}, 'test@example.com')"
                ).format(user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = list(session.execute(stmt))
        assert 0 == len(row)

        _id = uuid.uuid4()
        # alter the row to add a single item to the indexed list
        stmt = ("UPDATE list_index_search.users set uuids = [{id}] where user_id = {user_id}"
                ).format(id=_id, user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=_id)
        row = list(session.execute(stmt))
        assert 1 == len(row)

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4()  # this uuid will be on all records

        log = []

        for i in range(50000):
            user_uuid = uuid.uuid4()
            unshared_uuid = uuid.uuid4()

            # give each record a unique email address using the int index
            stmt = ("INSERT INTO list_index_search.users (user_id, email, uuids)"
                    "values ({user_uuid}, '{prefix}@example.com', [{s_uuid}, {u_uuid}])"
                    ).format(user_uuid=user_uuid, prefix=i, s_uuid=shared_uuid, u_uuid=unshared_uuid)
            session.execute(stmt)

            log.append(
                {'user_id': user_uuid,
                 'email': str(i) + '@example.com',
                 'unshared_uuid': unshared_uuid}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from list_index_search.users where uuids contains {shared_uuid}").format(shared_uuid=shared_uuid)
        rows = list(session.execute(stmt))
        result = [row for row in rows]
        assert 50000 == len(result)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM list_index_search.users where uuids contains {unshared_uuid}"
                    ).format(unshared_uuid=log_entry['unshared_uuid'])
            rows = list(session.execute(stmt))

            assert 1 == len(rows)

            db_user_id, db_email, db_uuids = rows[0]

            assert db_user_id == log_entry['user_id']
            assert db_email == log_entry['email']
            assert str(db_uuids[0]) == str(shared_uuid)
            assert str(db_uuids[1]) == str(log_entry['unshared_uuid'])

    def test_set_indexes(self):
        """
        Checks that secondary indexes on sets work for querying.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'set_index_search', 1)

        stmt = ("CREATE TABLE set_index_search.users ("
                "user_id uuid PRIMARY KEY,"
                "email text,"
                "uuids set<uuid>);")
        session.execute(stmt)

        # add index and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on set_index_search.users (uuids);"
        session.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = list(session.execute(stmt))
        assert 0 == len(row)

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO set_index_search.users (user_id, email) values ({user_id}, 'test@example.com')"
                ).format(user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = list(session.execute(stmt))
        assert 0 == len(row)

        _id = uuid.uuid4()
        # alter the row to add a single item to the indexed set
        stmt = ("UPDATE set_index_search.users set uuids = {{{id}}} where user_id = {user_id}").format(id=_id, user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=_id)
        row = list(session.execute(stmt))
        assert 1 == len(row)

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4()  # this uuid will be on all records

        log = []

        for i in range(50000):
            user_uuid = uuid.uuid4()
            unshared_uuid = uuid.uuid4()

            # give each record a unique email address using the int index
            stmt = ("INSERT INTO set_index_search.users (user_id, email, uuids)"
                    "values ({user_uuid}, '{prefix}@example.com', {{{s_uuid}, {u_uuid}}})"
                    ).format(user_uuid=user_uuid, prefix=i, s_uuid=shared_uuid, u_uuid=unshared_uuid)
            session.execute(stmt)

            log.append(
                {'user_id': user_uuid,
                 'email': str(i) + '@example.com',
                 'unshared_uuid': unshared_uuid}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from set_index_search.users where uuids contains {shared_uuid}").format(shared_uuid=shared_uuid)
        rows = session.execute(stmt)
        result = [row for row in rows]
        assert 50000 == len(result)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM set_index_search.users where uuids contains {unshared_uuid}"
                    ).format(unshared_uuid=log_entry['unshared_uuid'])
            rows = list(session.execute(stmt))

            assert 1 == len(rows)

            db_user_id, db_email, db_uuids = rows[0]

            assert db_user_id == log_entry['user_id']
            assert db_email == log_entry['email']
            assert shared_uuid in db_uuids
            assert log_entry['unshared_uuid'] in db_uuids

    @since('3.0')
    def test_multiple_indexes_on_single_map_column(self):
        """
        verifying functionality of multiple unique secondary indexes on a single column
        @jira_ticket CASSANDRA-7771
        @since 3.0
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'map_double_index', 1)
        session.execute("""
                CREATE TABLE map_tbl (
                    id uuid primary key,
                    amap map<text, int>
                )
            """)
        session.execute("CREATE INDEX map_keys ON map_tbl(keys(amap))")
        session.execute("CREATE INDEX map_values ON map_tbl(amap)")
        session.execute("CREATE INDEX map_entries ON map_tbl(entries(amap))")

        # multiple indexes on a single column are allowed but identical duplicate indexes are not
        assert_invalid(session,
                       "CREATE INDEX map_values_2 ON map_tbl(amap)",
                       'Index map_values_2 is a duplicate of existing index map_values')

        session.execute("INSERT INTO map_tbl (id, amap) values (uuid(), {'foo': 1, 'bar': 2});")
        session.execute("INSERT INTO map_tbl (id, amap) values (uuid(), {'faz': 1, 'baz': 2});")

        value_search = list(session.execute("SELECT * FROM map_tbl WHERE amap CONTAINS 1"))
        assert 2 == len(value_search), "incorrect number of rows when querying on map values"

        key_search = list(session.execute("SELECT * FROM map_tbl WHERE amap CONTAINS KEY 'foo'"))
        assert 1 == len(key_search), "incorrect number of rows when querying on map keys"

        entries_search = list(session.execute("SELECT * FROM map_tbl WHERE amap['foo'] = 1"))
        assert 1 == len(entries_search), "incorrect number of rows when querying on map entries"

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["map_double_index"].tables["map_tbl"]
        assert 3 == len(table_meta.indexes)
        assert {'map_keys', 'map_values', 'map_entries'} == set(table_meta.indexes.keys())
        assert 3 == len(session.cluster.metadata.keyspaces["map_double_index"].indexes)

        assert 'map_keys' in table_meta.export_as_string()
        assert 'map_values' in table_meta.export_as_string()
        assert 'map_entries' in table_meta.export_as_string()

        session.execute("DROP TABLE map_tbl")
        session.cluster.refresh_schema_metadata()
        assert 0 == len(session.cluster.metadata.keyspaces["map_double_index"].indexes)

    @pytest.mark.no_offheap_memtables
    def test_map_indexes(self):
        """
        Checks that secondary indexes on maps work for querying on both keys and values
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'map_index_search', 1)

        stmt = ("CREATE TABLE map_index_search.users ("
                "user_id uuid PRIMARY KEY,"
                "email text,"
                "uuids map<uuid, uuid>);")
        session.execute(stmt)

        # add index on keys and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on map_index_search.users (KEYS(uuids));"
        session.execute(stmt)

        stmt = "SELECT * from map_index_search.users where uuids contains key {some_uuid}".format(some_uuid=uuid.uuid4())
        rows = list(session.execute(stmt))
        assert 0 == len(rows)

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO map_index_search.users (user_id, email)"
                "values ({user_id}, 'test@example.com')"
                ).format(user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}").format(some_uuid=uuid.uuid4())
        rows = list(session.execute(stmt))
        assert 0 == len(rows)

        _id = uuid.uuid4()

        # alter the row to add a single item to the indexed map
        stmt = ("UPDATE map_index_search.users set uuids = {{{id}:{user_id}}} where user_id = {user_id}"
                ).format(id=_id, user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}").format(some_uuid=_id)
        rows = list(session.execute(stmt))
        assert 1 == len(rows)

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4()  # this uuid will be on all records

        log = []
        for i in range(50000):
            user_uuid = uuid.uuid4()
            unshared_uuid1 = uuid.uuid4()
            unshared_uuid2 = uuid.uuid4()

            # give each record a unique email address using the int index, add unique ids for keys and values
            stmt = ("INSERT INTO map_index_search.users (user_id, email, uuids)"
                    "values ({user_uuid}, '{prefix}@example.com', {{{u_uuid1}:{u_uuid2}, {s_uuid}:{s_uuid}}})"
                    ).format(user_uuid=user_uuid, prefix=i, s_uuid=shared_uuid, u_uuid1=unshared_uuid1, u_uuid2=unshared_uuid2)
            session.execute(stmt)

            log.append(
                {'user_id': user_uuid,
                 'email': str(i) + '@example.com',
                 'unshared_uuid1': unshared_uuid1,
                 'unshared_uuid2': unshared_uuid2}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from map_index_search.users where uuids contains key {shared_uuid}"
                ).format(shared_uuid=shared_uuid)
        rows = session.execute(stmt)
        result = [row for row in rows]
        assert 50000 == len(result)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index on keys
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM map_index_search.users where uuids contains key {unshared_uuid1}"
                    ).format(unshared_uuid1=log_entry['unshared_uuid1'])
            row = session.execute(stmt)

            result = list(row)
            assert 1 == len(result)

            db_user_id, db_email, db_uuids = result[0]

            assert db_user_id == log_entry['user_id']
            assert db_email == log_entry['email']

            assert shared_uuid in db_uuids
            assert log_entry['unshared_uuid1'] in db_uuids

        # attempt to add an index on map values as well (should fail pre 3.0)
        stmt = "CREATE INDEX user_uuids_values on map_index_search.users (uuids);"
        if self.cluster.version() < '3.0':
            if self.cluster.version() >= '2.2':
                matching = r"Cannot create index on values\(uuids\): an index on keys\(uuids\) already exists and indexing a map on more than one dimension at the same time is not currently supported"
            else:
                matching = "Cannot create index on uuids values, an index on uuids keys already exists and indexing a map on both keys and values at the same time is not currently supported"
            assert_invalid(session, stmt, matching)
        else:
            session.execute(stmt)

        if self.cluster.version() < '3.0':
            # since cannot have index on map keys and values remove current index on keys
            stmt = "DROP INDEX user_uuids;"
            session.execute(stmt)

            # add index on values (will index rows added prior)
            stmt = "CREATE INDEX user_uuids_values on map_index_search.users (uuids);"
            session.execute(stmt)

        block_until_index_is_built(node1, session, 'map_index_search', 'users', 'user_uuids_values')

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        time.sleep(10)

        # since we already inserted unique ids for values as well, check that appropriate records are found
        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM map_index_search.users where uuids contains {unshared_uuid2}"
                    ).format(unshared_uuid2=log_entry['unshared_uuid2'])

            rows = list(session.execute(stmt))
            assert 1 == len(rows), rows

            db_user_id, db_email, db_uuids = rows[0]
            assert db_user_id == log_entry['user_id']
            assert db_email == log_entry['email']

            assert shared_uuid in db_uuids
            assert log_entry['unshared_uuid2'] in list(db_uuids.values())


class TestUpgradeSecondaryIndexes(Tester):

    @since('2.1', max_version='2.1.x')
    def test_read_old_sstables_after_upgrade(self):
        """ from 2.1 the location of sstables changed (CASSANDRA-5202), but existing sstables continue
        to be read from the old location. Verify that this works for index sstables as well as regular
        data column families (CASSANDRA-9116)
        """
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="2.0.12")
        if "memtable_allocation_type" in cluster._config_options:
            cluster._config_options.__delitem__("memtable_allocation_type")
        cluster.populate(1).start()

        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'index_upgrade', 1)
        session.execute("CREATE TABLE index_upgrade.table1 (k int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON index_upgrade.table1(v)")
        session.execute("INSERT INTO index_upgrade.table1 (k,v) VALUES (0,0)")

        query = "SELECT * FROM index_upgrade.table1 WHERE v=0"
        assert_one(session, query, [0, 0])

        # Upgrade to the 2.1.x version
        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        logger.debug("Upgrading to current version")
        self.set_node_to_current_version(node1)
        node1.start(wait_other_notice=True)

        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        logger.debug(cluster.cassandra_version())
        assert_one(session, query, [0, 0])

    def upgrade_to_version(self, tag, nodes=None):
        logger.debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            logger.debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            logger.debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            logger.debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
            # node.nodetool('upgradesstables -a')


@since('3.10')
class TestPreJoinCallback(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = [
            # ignore all streaming errors during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred',
            r'\[Stream.*\] Streaming error occurred',
            r'\[Stream.*\] Remote peer 127.0.0.\d failed stream session',
            r'\[Stream.*\] Remote peer /127.0.0.\d:7000 failed stream session',
            r'Error while waiting on bootstrap to complete. Bootstrap will have to be restarted.'
        ]

    def _base_test(self, joinFn):
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start(wait_other_notice=True)

        # Create a table with 2i
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        session.execute("CREATE INDEX c2_idx ON cf (c2);")

        keys = 10000
        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        # Run the join function to test
        joinFn(cluster, tokens[1])

    def test_bootstrap(self):
        def bootstrap(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(wait_for_binary_proto=True)
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(bootstrap)

    def test_resume(self):
        def resume(cluster, token):
            node1 = cluster.nodes['node1']
            # set up byteman on node1 to inject a failure when streaming to node2
            node1.stop(wait=True)
            node1.byteman_port = '8100'
            node1.import_config_files()
            node1.start(wait_for_binary_proto=True)

            if cluster.version() < '4.0':
                node1.byteman_submit(['./byteman/pre4.0/inject_failure_streaming_to_node2.btm'])
            else:
                node1.byteman_submit(['./byteman/4.0/inject_failure_streaming_to_node2.btm'])

            node2 = new_node(cluster)

            yaml_opts = {'initial_token': token}
            if cluster.version() < '4.0':
                yaml_opts['streaming_socket_timeout_in_ms'] = 1000

            node2.set_configuration_options(values=yaml_opts)
            node2.start(wait_other_notice=True, wait_for_binary_proto=False)
            node2.watch_log_for('Some data streaming failed. Use nodetool to check bootstrap state and resume.')

            node2.nodetool("bootstrap resume")
            node2.watch_log_for('Starting listening for CQL clients')
            assert_bootstrap_state(self, node2, 'COMPLETED')
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(resume)

    def test_manual_join(self):
        def manual_join(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(join_ring=False, wait_for_binary_proto=True, wait_other_notice=240)
            assert node2.grep_log('Not joining ring as requested')
            assert not node2.grep_log('Executing pre-join')

            node2.nodetool("join")
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(manual_join)

    def test_write_survey(self):
        def write_survey_and_join(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(jvm_args=["-Dcassandra.write_survey=true"], wait_for_binary_proto=True)
            assert node2.grep_log('Startup complete, but write survey mode is active, not becoming an active ring member.')
            assert not node2.grep_log('Executing pre-join')

            node2.nodetool("join")
            assert node2.grep_log('Leaving write survey mode and joining ring at operator request')
            assert node2.grep_log('Executing pre-join post-bootstrap tasks')

        self._base_test(write_survey_and_join)
