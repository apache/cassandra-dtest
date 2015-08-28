import random
import re
import time
import uuid

from dtest import Tester, debug
from tools import since
from assertions import assert_invalid, assert_one
from cassandra import InvalidRequest
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.protocol import ConfigurationException


class TestSecondaryIndexes(Tester):

    def bug3367_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(session, 'users', columns=columns)

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

        result = session.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)

        result = session.execute("SELECT * FROM users WHERE state='TX';")
        assert len(result) == 2, "Expecting 2 users, got" + str(result)

        result = session.execute("SELECT * FROM users WHERE state='CA';")
        assert len(result) == 1, "Expecting 1 users, got" + str(result)

    @since('2.1')
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
            indexed_value = i % (num_rows / 3)
            # use the same indexed value three times
            session.execute("INSERT INTO ks.cf (a, b) VALUES ('%d', '%d');" % (i, indexed_value))

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
                    self.assertTrue(concurrency > 1, "Expected more than 1 concurrent range request, got %d" % concurrency)
                    self.assertTrue(expected_per_range > 0)
                    break
            else:
                self.fail("Didn't find matching trace event")

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1';")
        result = session.execute(query, trace=True)
        self.assertEqual(3, len(result))
        check_trace_events(query.trace)

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1' LIMIT 100;")
        result = session.execute(query, trace=True)
        self.assertEqual(3, len(result))
        check_trace_events(query.trace)

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1' LIMIT 3;")
        result = session.execute(query, trace=True)
        self.assertEqual(3, len(result))
        check_trace_events(query.trace)

        for limit in (1, 2):
            result = session.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT %d;" % (limit,))
            self.assertEqual(limit, len(result))

    @since('2.1')
    def test_6924_dropping_ks(self):
        """Tests CASSANDRA-6924

        Data inserted immediately after dropping and recreating a
        keyspace with an indexed column familiy is not included
        in the index.
        """
        # Reproducing requires at least 3 nodes:
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        conn = self.patient_cql_connection(node1)
        session = conn

        # This only occurs when dropping and recreating with
        # the same name, so loop through this test a few times:
        for i in range(10):
            debug("round %s" % i)
            try:
                session.execute("DROP KEYSPACE ks")
            except ConfigurationException:
                pass

            self.create_ks(session, 'ks', 1)
            session.execute("CREATE TABLE ks.cf (key text PRIMARY KEY, col1 text);")
            session.execute("CREATE INDEX on ks.cf (col1);")

            for r in range(10):
                stmt = "INSERT INTO ks.cf (key, col1) VALUES ('%s','asdf');" % r
                session.execute(stmt)

            self.wait_for_schema_agreement(session)

            rows = session.execute("select count(*) from ks.cf WHERE col1='asdf'")
            count = rows[0][0]
            self.assertEqual(count, 10)

    @since('2.1')
    def test_6924_dropping_cf(self):
        """Tests CASSANDRA-6924

        Data inserted immediately after dropping and recreating an
        indexed column family is not included in the index.
        """
        # Reproducing requires at least 3 nodes:
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        conn = self.patient_cql_connection(node1)
        session = conn
        self.create_ks(session, 'ks', 1)

        # This only occurs when dropping and recreating with
        # the same name, so loop through this test a few times:
        for i in range(10):
            debug("round %s" % i)
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
            self.assertEqual(count, 10)

    @since('2.0')
    def test_8280_validate_indexed_values(self):
        """Tests CASSANDRA-8280

        Reject inserts & updates where values of any indexed
        column is > 64k
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        conn = self.patient_cql_connection(node1)
        session = conn
        self.create_ks(session, 'ks', 1)

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
            assert False, "Expecting query %s to be invalid" % cql_string
        except AssertionError as e:
            raise e
        except InvalidRequest:
            pass

    def wait_for_schema_agreement(self, session):
        rows = session.execute("SELECT schema_version FROM system.local")
        local_version = rows[0]

        all_match = True
        rows = session.execute("SELECT schema_version FROM system.peers")
        for peer_version in rows:
            if peer_version != local_version:
                all_match = False
                break

        if all_match:
            return
        else:
            time.sleep(0.10)
            self.wait_for_schema_agreement(session)

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
        session = self.patient_cql_connection(node1)
        session.max_trace_wait = 120
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        session.execute("CREATE TABLE ks.cf (a text PRIMARY KEY, b text);")
        session.execute("CREATE INDEX b_index ON ks.cf (b);")
        num_rows = 100
        for i in range(num_rows):
            indexed_value = i % (num_rows / 3)
            # use the same indexed value three times
            session.execute("INSERT INTO ks.cf (a, b) VALUES ('{a}', '{b}');"
                            .format(a=i, b=indexed_value))

        cluster.flush()

        def check_trace_events(trace, regex, expected_matches):
            """
            Check for the presence of certain trace events. exact_matches should be a list of
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
                    self.fail("Expected to find between {min} and {max} trace events matching {pattern} from {source}, "
                              "but actually found {actual}. (Full counts: {all})"
                              .format(min=min_matches, max=max_matches, pattern=regex, source=event_source,
                                      actual=match_counts[event_source], all=match_counts))

        query = SimpleStatement("SELECT * FROM ks.cf WHERE b='1';")
        result = session.execute(query, trace=True)
        self.assertEqual(3, len(result))
        # node2 is the coordinator for the query
        check_trace_events(query.trace,
                           "Index mean cardinalities are b_index:[0-9]*. Scanning with b_index.",
                           [("127.0.0.1", 0, 0), ("127.0.0.2", 1, 1), ("127.0.0.3", 0, 0)])
        # check that the index is used on each node, really we only care that the matching
        # message appears on every node, so the max count is not important
        check_trace_events(query.trace,
                           "Executing read on ks.cf using index b_index",
                           [("127.0.0.1", 1, 200), ("127.0.0.2", 1, 200), ("127.0.0.3", 1, 200)])


class TestSecondaryIndexesOnCollections(Tester):
    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    @since('2.1')
    def test_list_indexes(self):
        """
        Checks that secondary indexes on lists work for querying.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'list_index_search', 1)

        stmt = ("CREATE TABLE list_index_search.users ("
                "user_id uuid PRIMARY KEY,"
                "email text,"
                "uuids list<uuid>"
                ");")
        session.execute(stmt)

        # no index present yet, make sure there's an error trying to query column
        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}"
                ).format(some_uuid=uuid.uuid4())

        if self.cluster.version() < "3":
            assert_invalid(session, stmt, 'No secondary indexes on the restricted columns support the provided operators')
        else:
            assert_invalid(session, stmt, 'No supported secondary index found for the non primary key columns restrictions')

        # add index and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on list_index_search.users (uuids);"
        session.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = session.execute(stmt)
        self.assertEqual(0, len(row))

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO list_index_search.users (user_id, email)"
                "values ({user_id}, 'test@example.com')"
                ).format(user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = session.execute(stmt)
        self.assertEqual(0, len(row))

        _id = uuid.uuid4()
        # alter the row to add a single item to the indexed list
        stmt = ("UPDATE list_index_search.users set uuids = [{id}] where user_id = {user_id}"
                ).format(id=_id, user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=_id)
        row = session.execute(stmt)
        self.assertEqual(1, len(row))

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
                 'email': str(i)+'@example.com',
                 'unshared_uuid': unshared_uuid}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from list_index_search.users where uuids contains {shared_uuid}").format(shared_uuid=shared_uuid)
        rows = session.execute(stmt)
        result = [row for row in rows]
        self.assertEqual(50000, len(result))

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM list_index_search.users where uuids contains {unshared_uuid}"
                    ).format(unshared_uuid=log_entry['unshared_uuid'])
            rows = session.execute(stmt)

            self.assertEqual(1, len(rows))

            db_user_id, db_email, db_uuids = rows[0]

            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])
            self.assertEqual(str(db_uuids[0]), str(shared_uuid))
            self.assertEqual(str(db_uuids[1]), str(log_entry['unshared_uuid']))

    @since('2.1')
    def test_set_indexes(self):
        """
        Checks that secondary indexes on sets work for querying.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'set_index_search', 1)

        stmt = ("CREATE TABLE set_index_search.users ("
                "user_id uuid PRIMARY KEY,"
                "email text,"
                "uuids set<uuid>);")
        session.execute(stmt)

        # no index present yet, make sure there's an error trying to query column
        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        if self.cluster.version() < "3":
            assert_invalid(session, stmt, 'No secondary indexes on the restricted columns support the provided operators')
        else:
            assert_invalid(session, stmt, 'No supported secondary index found for the non primary key columns restrictions')

        # add index and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on set_index_search.users (uuids);"
        session.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = session.execute(stmt)
        self.assertEqual(0, len(row))

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO set_index_search.users (user_id, email) values ({user_id}, 'test@example.com')"
                ).format(user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        row = session.execute(stmt)
        self.assertEqual(0, len(row))

        _id = uuid.uuid4()
        # alter the row to add a single item to the indexed set
        stmt = ("UPDATE set_index_search.users set uuids = {{{id}}} where user_id = {user_id}").format(id=_id, user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=_id)
        row = session.execute(stmt)
        self.assertEqual(1, len(row))

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
                 'email': str(i)+'@example.com',
                 'unshared_uuid': unshared_uuid}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from set_index_search.users where uuids contains {shared_uuid}").format(shared_uuid=shared_uuid)
        rows = session.execute(stmt)
        result = [row for row in rows]
        self.assertEqual(50000, len(result))

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM set_index_search.users where uuids contains {unshared_uuid}"
                    ).format(unshared_uuid=log_entry['unshared_uuid'])
            rows = session.execute(stmt)

            self.assertEqual(1, len(rows))

            db_user_id, db_email, db_uuids = rows[0]

            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])
            self.assertTrue(shared_uuid in db_uuids)
            self.assertTrue(log_entry['unshared_uuid'] in db_uuids)

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
        self.create_ks(session, 'map_double_index', 1)
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

        value_search = session.execute("SELECT * FROM map_tbl WHERE amap CONTAINS 1")
        self.assertEqual(2, len(value_search), "incorrect number of rows when querying on map values")

        key_search = session.execute("SELECT * FROM map_tbl WHERE amap CONTAINS KEY 'foo'")
        self.assertEqual(1, len(key_search), "incorrect number of rows when querying on map keys")

        entries_search = session.execute("SELECT * FROM map_tbl WHERE amap['foo'] = 1")
        self.assertEqual(1, len(entries_search), "incorrect number of rows when querying on map entries")

        session.cluster.refresh_schema_metadata()
        table_meta = session.cluster.metadata.keyspaces["map_double_index"].tables["map_tbl"]
        self.assertEqual(3, len(table_meta.indexes))
        self.assertItemsEqual(['map_keys', 'map_values', 'map_entries'], table_meta.indexes)
        self.assertEqual(3, len(session.cluster.metadata.keyspaces["map_double_index"].indexes))

        self.assertTrue('map_keys' in table_meta.export_as_string())
        self.assertTrue('map_values' in table_meta.export_as_string())
        self.assertTrue('map_entries' in table_meta.export_as_string())

        session.execute("DROP TABLE map_tbl")
        session.cluster.refresh_schema_metadata()
        self.assertEqual(0, len(session.cluster.metadata.keyspaces["map_double_index"].indexes))

    @since('2.1')
    def test_map_indexes(self):
        """
        Checks that secondary indexes on maps work for querying on both keys and values
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'map_index_search', 1)

        stmt = ("CREATE TABLE map_index_search.users ("
                "user_id uuid PRIMARY KEY,"
                "email text,"
                "uuids map<uuid, uuid>);")
        session.execute(stmt)

        no_index_error = ('No secondary indexes on the restricted columns support the provided operators'
                          if self.cluster.version() < '3' else
                          'No supported secondary index found for the non primary key columns restrictions')
        # no index present yet, make sure there's an error trying to query column
        stmt = ("SELECT * from map_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        assert_invalid(session, stmt, no_index_error)

        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}"
                ).format(some_uuid=uuid.uuid4())
        assert_invalid(session, stmt, no_index_error)

        # add index on keys and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on map_index_search.users (KEYS(uuids));"
        session.execute(stmt)

        stmt = "SELECT * from map_index_search.users where uuids contains key {some_uuid}".format(some_uuid=uuid.uuid4())
        rows = session.execute(stmt)
        self.assertEqual(0, len(rows))

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO map_index_search.users (user_id, email)"
                "values ({user_id}, 'test@example.com')"
                ).format(user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}").format(some_uuid=uuid.uuid4())
        rows = session.execute(stmt)
        self.assertEqual(0, len(rows))

        _id = uuid.uuid4()

        # alter the row to add a single item to the indexed map
        stmt = ("UPDATE map_index_search.users set uuids = {{{id}:{user_id}}} where user_id = {user_id}"
                ).format(id=_id, user_id=user1_uuid)
        session.execute(stmt)

        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}").format(some_uuid=_id)
        rows = session.execute(stmt)
        self.assertEqual(1, len(rows))

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
                 'email': str(i)+'@example.com',
                 'unshared_uuid1': unshared_uuid1,
                 'unshared_uuid2': unshared_uuid2}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from map_index_search.users where uuids contains key {shared_uuid}"
                ).format(shared_uuid=shared_uuid)
        rows = session.execute(stmt)
        result = [row for row in rows]
        self.assertEqual(50000, len(result))

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index on keys
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM map_index_search.users where uuids contains key {unshared_uuid1}"
                    ).format(unshared_uuid1=log_entry['unshared_uuid1'])
            row = session.execute(stmt)

            rows = self.assertEqual(1, len(row))

            db_user_id, db_email, db_uuids = row[0]

            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])

            self.assertTrue(shared_uuid in db_uuids)
            self.assertTrue(log_entry['unshared_uuid1'] in db_uuids)

        # attempt to add an index on map values as well (should fail pre 3.0)
        stmt = "CREATE INDEX user_uuids_values on map_index_search.users (uuids);"
        if self.cluster.version() < '3.0':
            if self.cluster.version() >= '2.2':
                matching = "Cannot create index on values\(uuids\): an index on keys\(uuids\) already exists and indexing a map on more than one dimension at the same time is not currently supported"
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

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        time.sleep(10)

        # since we already inserted unique ids for values as well, check that appropriate recors are found
        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM map_index_search.users where uuids contains {unshared_uuid2}"
                    ).format(unshared_uuid2=log_entry['unshared_uuid2'])

            rows = session.execute(stmt)
            self.assertEqual(1, len(rows))

            db_user_id, db_email, db_uuids = rows[0]
            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])

            self.assertTrue(shared_uuid in db_uuids)
            self.assertTrue(log_entry['unshared_uuid2'] in db_uuids.values())


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
        self.create_ks(session, 'index_upgrade', 1)
        session.execute("CREATE TABLE index_upgrade.table1 (k int PRIMARY KEY, v int)")
        session.execute("CREATE INDEX ON index_upgrade.table1(v)")
        session.execute("INSERT INTO index_upgrade.table1 (k,v) VALUES (0,0)")

        query = "SELECT * FROM index_upgrade.table1 WHERE v=0"
        assert_one(session, query, [0, 0])

        # Upgrade to the 2.1.x version
        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        debug("Upgrading to current version")
        self.set_node_to_current_version(node1)
        node1.start(wait_other_notice=True)

        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        debug(cluster.cassandra_version())
        assert_one(session, query, [0, 0])

    def upgrade_to_version(self, tag, nodes=None):
        debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
            # node.nodetool('upgradesstables -a')
