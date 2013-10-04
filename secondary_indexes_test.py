import time

from dtest import Tester, TracingCursor
from tools import since

class TestSecondaryIndexes(Tester):

    def bug3367_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        cursor = self.cql_connection(node1).cursor()
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
        time.sleep(1)
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        time.sleep(.5)
        conn = self.cql_connection(node1, version='3.0.0')
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
