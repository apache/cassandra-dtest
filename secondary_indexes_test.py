import pdb
import random
import time
import uuid

from cql import ProgrammingError
from dtest import Tester, TracingCursor
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

        conn = self.patient_cql_connection(node1)
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

        def check_request_order():
            # we should see multiple requests get enqueued prior to index scan
            # execution happening
            trace = cursor.get_last_trace()
            relevant_events = [(desc, ip) for (_, _, desc, ip, _, _) in trace
                               if 'Enqueuing request' in desc or
                               ('Executing indexed scan' in desc and ip == node1.address())]

            self.assertTrue('Enqueuing' in relevant_events[0][0], str(relevant_events[0]))
            self.assertTrue('Enqueuing' in relevant_events[1][0], str(relevant_events[0]))
            self.assertTrue('Executing indexed scan' in relevant_events[-1][0], str(relevant_events[-1]))

        cursor.execute("SELECT * FROM ks.cf WHERE b='1';")
        result = cursor.fetchall()
        self.assertEqual(3, len(result))
        check_request_order()

        cursor.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT 100;")
        result = cursor.fetchall()
        self.assertEqual(3, len(result))
        check_request_order()

        cursor.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT 3;")
        result = cursor.fetchall()
        self.assertEqual(3, len(result))
        check_request_order()

        for limit in (1, 2):
            cursor.execute("SELECT * FROM ks.cf WHERE b='1' LIMIT %d;" % (limit,))
            result = cursor.fetchall()
            self.assertEqual(limit, len(result))


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

        stmt = """
              CREATE TABLE list_index_search.users (
               user_id uuid PRIMARY KEY,
               email text,
               uuids list<uuid>
              );
            """
        cursor.execute(stmt)

        # no index present yet, make sure there's an error trying to query column
        stmt = """
              SELECT * from list_index_search.users where uuids contains {some_uuid}
            """.format(some_uuid=uuid.uuid4())
        with self.assertRaisesRegexp(ProgrammingError, 'No indexed columns present in by-columns clause'):
            cursor.execute(stmt)

        # add index and query again (even though there are no rows in the table yet)
        stmt = """
              CREATE INDEX user_uuids on list_index_search.users (uuids);
            """
        cursor.execute(stmt)

        stmt = """
              SELECT * from list_index_search.users where uuids contains {some_uuid}
            """.format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        # add a row which doesn't specify data for the indexed column, and query again
        user1_uuid = uuid.uuid4()
        stmt = """
              INSERT INTO list_index_search.users (user_id, email)
              values ({user_id}, 'test@example.com')
            """.format(user_id=user1_uuid)
        cursor.execute(stmt)

        time.sleep(5)
        stmt = """
              SELECT * from list_index_search.users where uuids contains {some_uuid}
            """.format(some_uuid=uuid.uuid4())
        cursor.execute(stmt)
        self.assertEqual(0, cursor.rowcount)

        _id = uuid.uuid4()
        # alter the row to add a single item to the indexed list
        stmt = """
              UPDATE list_index_search.users set uuids = [{id}] where user_id = {user_id}
            """.format(id=_id, user_id=user1_uuid)
        cursor.execute(stmt)

        stmt = """
              SELECT * from list_index_search.users where uuids contains {some_uuid}
            """.format(some_uuid=_id)
        cursor.execute(stmt)
        self.assertEqual(1, cursor.rowcount)

        # add a bunch of user records and query them back
        shared_uuid = uuid.uuid4() # this uuid will be on all records

        log = []

        for i in range(50001):
            user_uuid = uuid.uuid4()
            unshared_uuid = uuid.uuid4()

            # give each record a unique email address using the int index
            stmt = """
                  INSERT INTO list_index_search.users (user_id, email, uuids)
                  values ({user_uuid}, '{prefix}@example.com', [{s_uuid}, {u_uuid}])
               """.format(user_uuid=user_uuid, prefix=i, s_uuid=shared_uuid, u_uuid=unshared_uuid)
            cursor.execute(stmt)

            log.append(
                {'user_id': user_uuid,
                 'email':str(i)+'@example.com',
                 'unshared_uuid':unshared_uuid}
            )

        # confirm there is now 50k rows with the 'shared' uuid above in the secondary index
        stmt = """
              SELECT * from list_index_search.users where uuids contains {shared_uuid}
            """.format(shared_uuid=shared_uuid)
        cursor.execute(stmt)
        self.assertEqual(50000, cursor.rowcount)

        # shuffle the log in-place, and double-check a slice of records by querying the secondary index
        random.shuffle(log)

        for log_entry in log[:1000]:
            stmt = """
                  SELECT user_id, email, uuids FROM list_index_search.users where uuids contains {unshared_uuid}
               """.format(unshared_uuid=log_entry['unshared_uuid'])
            cursor.execute(stmt)

            self.assertEqual(1, cursor.rowcount)

            db_user_id, db_email, db_uuids = cursor.fetchone()

            self.assertEqual(db_user_id, log_entry['user_id'])
            self.assertEqual(db_email, log_entry['email'])
            self.assertEqual(str(db_uuids[0]), str(shared_uuid))
            self.assertEqual(str(db_uuids[1]), str(log_entry['unshared_uuid']))