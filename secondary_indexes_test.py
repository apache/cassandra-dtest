import random
import re
import time
import uuid

from cql import ProgrammingError
from dtest import Tester, TracingCursor, debug
from tools import since


class TestSecondaryIndexes(Tester):

    def bug3367_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)

        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(cursor, 'users', columns=columns)

        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")

        # create index
        cursor.execute("CREATE INDEX gender_key ON users (gender);")
        cursor.execute("CREATE INDEX state_key ON users (state);")
        cursor.execute("CREATE INDEX birth_year_key ON users (birth_year);")

        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        cursor.execute("SELECT * FROM users;")
        result = cursor.fetchall()
        assert len(result) == 4, "Expecting 4 users, got" + str(result)

        cursor.execute("SELECT * FROM users WHERE state='TX';")
        result = cursor.fetchall()
        assert len(result) == 2, "Expecting 2 users, got" + str(result)

        cursor.execute("SELECT * FROM users WHERE state='CA';")
        result = cursor.fetchall()
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

        conn = self.patient_cql_connection(node1, version='3.0.0')
        cursor = conn.cursor()
        cursor.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '1'};")
        cursor.execute("CREATE TABLE ks.cf (a text PRIMARY KEY, b text);")
        cursor.execute("CREATE INDEX b_index ON ks.cf (b);")
        num_rows = 100
        for i in range(num_rows):
            indexed_value = i % (num_rows / 3)
            # use the same indexed value three times
            cursor.execute("INSERT INTO ks.cf (a, b) VALUES ('%d', '%d');" % (i, indexed_value))

        cluster.flush()

        conn.cursorclass = TracingCursor
        cursor = conn.cursor()

        def check_trace_events():
            # we should see multiple requests get enqueued prior to index scan
            # execution happening
            trace = cursor.get_last_trace()

            # Look for messages like "Submitting
            regex = r"Submitting (\d+) concurrent range requests; ([0-9.]+) rows per range expected"
            for (_, _, desc, _, _, _) in trace:
                match = re.match(regex, desc)
                if match:
                    concurrency = int(match.group(1))
                    expected_per_range = float(match.group(2))
                    self.assertTrue(concurrency > 1, "Expected more than 1 concurrent range request, got %d" % concurrency)
                    self.assertTrue(expected_per_range > 0)
                    break
            else:
                self.fail("Didn't find matching trace event")

        cursor.execute("SELECT * FROM ks.cf WHERE b='1';")
        result = cursor.fetchall()
        self.assertEqual(3, len(result))
        check_trace_events()

        cursor.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT 100;")
        result = cursor.fetchall()
        self.assertEqual(3, len(result))
        check_trace_events()

        cursor.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT 3;")
        result = cursor.fetchall()
        self.assertEqual(3, len(result))
        check_trace_events()

        for limit in (1, 2):
            cursor.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT %d;" % (limit,))
            result = cursor.fetchall()
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
        cursor = conn.cursor()

        #This only occurs when dropping and recreating with
        #the same name, so loop through this test a few times:
        for i in range(10):
            debug("round %s" % i)
            try:
                cursor.execute("DROP KEYSPACE ks")
            except ProgrammingError:
                pass

            self.create_ks(cursor, 'ks', 1)
            cursor.execute("CREATE TABLE ks.cf (key text PRIMARY KEY, col1 text);")
            cursor.execute("CREATE INDEX on ks.cf (col1);")

            for r in range(10):
                stmt = "INSERT INTO ks.cf (key, col1) VALUES ('%s','asdf');" % r
                cursor.execute(stmt)

            self.wait_for_schema_agreement(cursor)

            cursor.execute("select count(*) from ks.cf WHERE col1='asdf'")
            count = cursor.fetchone()[0]
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
        cursor = conn.cursor()
        self.create_ks(cursor, 'ks', 1)

        #This only occurs when dropping and recreating with
        #the same name, so loop through this test a few times:
        for i in range(10):
            debug("round %s" % i)
            try:
                cursor.execute("DROP COLUMNFAMILY ks.cf")
            except ProgrammingError:
                pass

            cursor.execute("CREATE TABLE ks.cf (key text PRIMARY KEY, col1 text);")
            cursor.execute("CREATE INDEX on ks.cf (col1);")

            for r in range(10):
                stmt = "INSERT INTO ks.cf (key, col1) VALUES ('%s','asdf');" % r
                cursor.execute(stmt)

            self.wait_for_schema_agreement(cursor)

            cursor.execute("select count(*) from ks.cf WHERE col1='asdf'")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 10)

    def wait_for_schema_agreement(self, cursor):
        cursor.execute("SELECT schema_version FROM system.local")
        local_version = cursor.fetchone()

        all_match = True
        cursor.execute("SELECT schema_version FROM system.peers")
        for peer_version in cursor.fetchall():
            if peer_version != local_version:
                all_match = False
                break

        if all_match:
            return
        else:
            time.sleep(0.10)
            self.wait_for_schema_agreement(cursor)


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
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'list_index_search', 1)

        stmt = ("CREATE TABLE list_index_search.users ("
               "user_id uuid PRIMARY KEY,"
               "email text,"
               "uuids list<uuid>"
              ");")
        cursor.execute(stmt)

        # no index present yet, make sure there's an error trying to query column
        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}"
            ).format(some_uuid=uuid.uuid4())
        with self.assertRaisesRegexp(ProgrammingError, 'No indexed columns present in by-columns clause'):
            cursor.execute(stmt)

        # add index and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on list_index_search.users (uuids);"
        cursor.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO list_index_search.users (user_id, email)"
              "values ({user_id}, 'test@example.com')"
            ).format(user_id=user1_uuid)
        cursor.execute(stmt)

        time.sleep(5)
        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        _id = uuid.uuid4()
        # alter the row to add a single item to the indexed list
        stmt = ("UPDATE list_index_search.users set uuids = [{id}] where user_id = {user_id}"
            ).format(id=_id, user_id=user1_uuid)
        cursor.execute(stmt)

        stmt = ("SELECT * from list_index_search.users where uuids contains {some_uuid}").format(some_uuid=_id)
        cursor.execute(stmt)
        self.assertEqual(1, cursor.rowcount)

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4() # this uuid will be on all records

        log = []

        for i in range(50000):
            user_uuid = uuid.uuid4()
            unshared_uuid = uuid.uuid4()

            # give each record a unique email address using the int index
            stmt = ("INSERT INTO list_index_search.users (user_id, email, uuids)"
                  "values ({user_uuid}, '{prefix}@example.com', [{s_uuid}, {u_uuid}])"
               ).format(user_uuid=user_uuid, prefix=i, s_uuid=shared_uuid, u_uuid=unshared_uuid)
            cursor.execute(stmt)

            log.append(
                {'user_id': user_uuid,
                 'email':str(i)+'@example.com',
                 'unshared_uuid':unshared_uuid}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from list_index_search.users where uuids contains {shared_uuid}").format(shared_uuid=shared_uuid)
        cursor.execute(stmt)
        self.assertEqual(50000, cursor.rowcount)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM list_index_search.users where uuids contains {unshared_uuid}"
                ).format(unshared_uuid=log_entry['unshared_uuid'])
            cursor.execute(stmt)

            self.assertEqual(1, cursor.rowcount)

            db_user_id, db_email, db_uuids = cursor.fetchone()

            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])
            self.assertEqual(str(db_uuids[0]), str(shared_uuid))
            self.assertEqual(str(db_uuids[1]), str(log_entry['unshared_uuid']))

    def test_set_indexes(self):
        """
        Checks that secondary indexes on sets work for querying.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'set_index_search', 1)

        stmt = ("CREATE TABLE set_index_search.users ("
               "user_id uuid PRIMARY KEY,"
               "email text,"
               "uuids set<uuid>);")
        cursor.execute(stmt)

        # no index present yet, make sure there's an error trying to query column
        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        with self.assertRaisesRegexp(ProgrammingError, 'No indexed columns present in by-columns clause'):
            cursor.execute(stmt)

        # add index and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on set_index_search.users (uuids);"
        cursor.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO set_index_search.users (user_id, email) values ({user_id}, 'test@example.com')"
            ).format(user_id=user1_uuid)
        cursor.execute(stmt)

        time.sleep(5)
        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        _id = uuid.uuid4()
        # alter the row to add a single item to the indexed set
        stmt = ("UPDATE set_index_search.users set uuids = {{{id}}} where user_id = {user_id}").format(id=_id, user_id=user1_uuid)
        cursor.execute(stmt)

        stmt = ("SELECT * from set_index_search.users where uuids contains {some_uuid}").format(some_uuid=_id)
        cursor.execute(stmt)
        self.assertEqual(1, cursor.rowcount)

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4() # this uuid will be on all records

        log = []

        for i in range(50000):
            user_uuid = uuid.uuid4()
            unshared_uuid = uuid.uuid4()

            # give each record a unique email address using the int index
            stmt = ("INSERT INTO set_index_search.users (user_id, email, uuids)"
                  "values ({user_uuid}, '{prefix}@example.com', {{{s_uuid}, {u_uuid}}})"
                ).format(user_uuid=user_uuid, prefix=i, s_uuid=shared_uuid, u_uuid=unshared_uuid)
            cursor.execute(stmt)

            log.append(
                {'user_id': user_uuid,
                 'email':str(i)+'@example.com',
                 'unshared_uuid':unshared_uuid}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from set_index_search.users where uuids contains {shared_uuid}").format(shared_uuid=shared_uuid)
        cursor.execute(stmt)
        self.assertEqual(50000, cursor.rowcount)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM set_index_search.users where uuids contains {unshared_uuid}"
                ).format(unshared_uuid=log_entry['unshared_uuid'])
            cursor.execute(stmt)

            self.assertEqual(1, cursor.rowcount)

            db_user_id, db_email, db_uuids = cursor.fetchone()

            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])
            self.assertTrue(shared_uuid in db_uuids)
            self.assertTrue(log_entry['unshared_uuid'] in db_uuids)

    def test_map_indexes(self):
        """
        Checks that secondary indexes on maps work for querying on both keys and values
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'map_index_search', 1)

        stmt = ("CREATE TABLE map_index_search.users ("
               "user_id uuid PRIMARY KEY,"
               "email text,"
               "uuids map<uuid, uuid>);")
        cursor.execute(stmt)

        # no index present yet, make sure there's an error trying to query column
        stmt = ("SELECT * from map_index_search.users where uuids contains {some_uuid}").format(some_uuid=uuid.uuid4())
        with self.assertRaisesRegexp(ProgrammingError, 'No indexed columns present in by-columns clause'):
            cursor.execute(stmt)

        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}"
            ).format(some_uuid=uuid.uuid4())
        with self.assertRaisesRegexp(ProgrammingError, 'No indexed columns present in by-columns clause'):
            cursor.execute(stmt)

        # add index on keys and query again (even though there are no rows in the table yet)
        stmt = "CREATE INDEX user_uuids on map_index_search.users (KEYS(uuids));"
        cursor.execute(stmt)

        stmt = "SELECT * from map_index_search.users where uuids contains key {some_uuid}".format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = ("INSERT INTO map_index_search.users (user_id, email)"
              "values ({user_id}, 'test@example.com')"
            ).format(user_id=user1_uuid)
        cursor.execute(stmt)

        time.sleep(5)
        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}").format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        _id = uuid.uuid4()

        # alter the row to add a single item to the indexed map
        stmt = ("UPDATE map_index_search.users set uuids = {{{id}:{user_id}}} where user_id = {user_id}"
            ).format(id=_id, user_id=user1_uuid)
        cursor.execute(stmt)

        stmt = ("SELECT * from map_index_search.users where uuids contains key {some_uuid}").format(some_uuid=_id)
        cursor.execute(stmt)
        self.assertEqual(1, cursor.rowcount)

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4() # this uuid will be on all records

        log = []

        for i in range(50000):
            user_uuid = uuid.uuid4()
            unshared_uuid1 = uuid.uuid4()
            unshared_uuid2 = uuid.uuid4()

            # give each record a unique email address using the int index, add unique ids for keys and values
            stmt = ("INSERT INTO map_index_search.users (user_id, email, uuids)"
                  "values ({user_uuid}, '{prefix}@example.com', {{{u_uuid1}:{u_uuid2}, {s_uuid}:{s_uuid}}})"
                ).format(user_uuid=user_uuid, prefix=i, s_uuid=shared_uuid, u_uuid1=unshared_uuid1, u_uuid2=unshared_uuid2)
            cursor.execute(stmt)

            log.append(
                {'user_id': user_uuid,
                 'email':str(i)+'@example.com',
                 'unshared_uuid1':unshared_uuid1,
                 'unshared_uuid2':unshared_uuid2}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = ("SELECT * from map_index_search.users where uuids contains key {shared_uuid}"
            ).format(shared_uuid=shared_uuid)
        cursor.execute(stmt)
        self.assertEqual(50000, cursor.rowcount)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index on keys
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM map_index_search.users where uuids contains key {unshared_uuid1}"
                ).format(unshared_uuid1=log_entry['unshared_uuid1'])
            cursor.execute(stmt)

            self.assertEqual(1, cursor.rowcount)

            db_user_id, db_email, db_uuids = cursor.fetchone()

            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])

            self.assertTrue(shared_uuid in db_uuids)
            self.assertTrue(log_entry['unshared_uuid1'] in db_uuids)

        # attempt to add an index on map values as well (should fail)
        stmt = "CREATE INDEX user_uuids on map_index_search.users (uuids);"
        with self.assertRaisesRegexp(ProgrammingError, """Bad Request: Cannot create index on uuids values, an index on uuids keys already exists and indexing a map on both keys and values at the same time is not currently supported"""):
            cursor.execute(stmt)

        # since cannot have index on map keys and values remove current index on keys
        stmt = "DROP INDEX user_uuids;"
        cursor.execute(stmt)  
        
        # add index on values (will index rows added prior)
        stmt = "CREATE INDEX user_uids on map_index_search.users (uuids);"
        cursor.execute(stmt)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        time.sleep(10)

        # since we already inserted unique ids for values as well, check that appropriate recors are found
        for log_entry in log[:1000]:
            stmt = ("SELECT user_id, email, uuids FROM map_index_search.users where uuids contains {unshared_uuid2}"
                ).format(unshared_uuid2=log_entry['unshared_uuid2'])

            cursor.execute(stmt)
            self.assertEqual(1, cursor.rowcount)

            db_user_id, db_email, db_uuids = cursor.fetchone()
            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])

            self.assertTrue(shared_uuid in db_uuids)
            self.assertTrue(log_entry['unshared_uuid2'] in db_uuids.values())
