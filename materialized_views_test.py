import time
import collections
import sys
import traceback

from functools import partial
# TODO add in requirements.txt
from enum import Enum  # Remove when switching to py3
from multiprocessing import Process, Queue
from unittest import skipIf

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from dtest import Tester, debug
from tools import since, new_node
from assertions import assert_all, assert_one, assert_invalid, assert_unavailable, assert_none, assert_crc_check_chance_equal


@since('3.0')
class TestMaterializedViews(Tester):
    """
    Test materialized views implementation.
    @jira_ticket CASSANDRA-6477
    @since 3.0
    """

    def prepare(self, user_table=False, rf=1, options={}, nodes=3):
        cluster = self.cluster
        cluster.populate([nodes, 0])
        if options:
            cluster.set_configuration_options(values=options)
        cluster.start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', rf)

        if user_table:
            session.execute(
                ("CREATE TABLE users (username varchar, password varchar, gender varchar, "
                 "session_token varchar, state varchar, birth_year bigint, "
                 "PRIMARY KEY (username));")
            )

            # create a materialized view
            session.execute(("CREATE MATERIALIZED VIEW users_by_state AS "
                             "SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL "
                             "PRIMARY KEY (state, username)"))

        return session

    def _insert_data(self, session):
        # insert data
        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

    def _replay_batchlogs(self):
        debug("Replaying batchlog on all nodes")
        for node in self.cluster.nodelist():
            if node.is_running():
                node.nodetool("replaybatchlog")

    def create_test(self):
        """Test the materialized view creation"""

        session = self.prepare(user_table=True)

        result = session.execute(("SELECT * FROM system_schema.views "
                                  "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING"))
        self.assertEqual(len(result), 1, "Expecting 1 materialized view, got" + str(result))

    def test_gcgs_validation(self):
        """Verify that it's not possible to create or set a too low gc_grace_seconds on MVs"""
        session = self.prepare(user_table=True)

        # Shouldn't be able to alter the gc_grace_seconds of the base table to 0
        assert_invalid(session,
                       "ALTER TABLE users WITH gc_grace_seconds = 0",
                       "Cannot alter gc_grace_seconds of the base table of a materialized view "
                       "to 0, since this value is used to TTL undelivered updates. Setting "
                       "gc_grace_seconds too low might cause undelivered updates to expire "
                       "before being replayed.")

        # But can alter the gc_grace_seconds of the bease table to a value != 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 10")

        # Shouldn't be able to alter the gc_grace_seconds of the MV to 0
        assert_invalid(session,
                       "ALTER MATERIALIZED VIEW users_by_state WITH gc_grace_seconds = 0",
                       "Cannot alter gc_grace_seconds of a materialized view to 0, since "
                       "this value is used to TTL undelivered updates. Setting gc_grace_seconds "
                       "too low might cause undelivered updates to expire before being replayed.")

        # Now let's drop MV
        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        # Now we should be able to set the gc_grace_seconds of the base table to 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")

        # Now we shouldn't be able to create a new MV on this table
        assert_invalid(session,
                       "CREATE MATERIALIZED VIEW users_by_state AS "
                       "SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL "
                       "PRIMARY KEY (state, username)",
                       "Cannot create materialized view 'users_by_state' for base table 'users' "
                       "with gc_grace_seconds of 0, since this value is used to TTL undelivered "
                       "updates. Setting gc_grace_seconds too low might cause undelivered updates"
                       " to expire before being replayed.")

    def insert_test(self):
        """Test basic insertions"""

        session = self.prepare(user_table=True)

        self._insert_data(session)

        result = session.execute("SELECT * FROM users;")
        self.assertEqual(len(result), 4, "Expecting {} users, got {}".format(4, len(result)))

        result = session.execute("SELECT * FROM users_by_state WHERE state='TX';")
        self.assertEqual(len(result), 2, "Expecting {} users, got {}".format(2, len(result)))

        result = session.execute("SELECT * FROM users_by_state WHERE state='CA';")
        self.assertEqual(len(result), 1, "Expecting {} users, got {}".format(1, len(result)))

        result = session.execute("SELECT * FROM users_by_state WHERE state='MA';")
        self.assertEqual(len(result), 0, "Expecting {} users, got {}".format(0, len(result)))

    def populate_mv_after_insert_test(self):
        """Test that a view is OK when created with existing data"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))

        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("wait that all batchlogs are replayed")
        self._replay_batchlogs()

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])

    def crc_check_chance_test(self):
        """Test that crc_check_chance parameter is properly populated after mv creation and update"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id) WITH crc_check_chance = 0.5"))

        assert_crc_check_chance_equal(session, "t_by_v", 0.5, view=True)

        session.execute("ALTER MATERIALIZED VIEW t_by_v WITH crc_check_chance = 0.3")

        assert_crc_check_chance_equal(session, "t_by_v", 0.3, view=True)

    def prepared_statement_test(self):
        """Test basic insertions with prepared statement"""

        session = self.prepare(user_table=True)

        insertPrepared = session.prepare(
            "INSERT INTO users (username, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);"
        )
        selectPrepared = session.prepare(
            "SELECT state, password, session_token FROM users_by_state WHERE state=?;"
        )

        # insert data
        session.execute(insertPrepared.bind(('user1', 'ch@ngem3a', 'f', 'TX', 1968)))
        session.execute(insertPrepared.bind(('user2', 'ch@ngem3b', 'm', 'CA', 1971)))
        session.execute(insertPrepared.bind(('user3', 'ch@ngem3c', 'f', 'FL', 1978)))
        session.execute(insertPrepared.bind(('user4', 'ch@ngem3d', 'm', 'TX', 1974)))

        result = session.execute("SELECT * FROM users;")
        self.assertEqual(len(result), 4, "Expecting {} users, got {}".format(4, len(result)))

        result = session.execute(selectPrepared.bind(['TX']))
        self.assertEqual(len(result), 2, "Expecting {} users, got {}".format(2, len(result)))

        result = session.execute(selectPrepared.bind(['CA']))
        self.assertEqual(len(result), 1, "Expecting {} users, got {}".format(1, len(result)))

        result = session.execute(selectPrepared.bind(['MA']))
        self.assertEqual(len(result), 0, "Expecting {} users, got {}".format(0, len(result)))

    def immutable_test(self):
        """Test that a materialized view is immutable"""

        session = self.prepare(user_table=True)

        # cannot insert
        assert_invalid(session, "INSERT INTO users_by_state (state, username) VALUES ('TX', 'user1');",
                       "Cannot directly modify a materialized view")

        # cannot update
        assert_invalid(session, "UPDATE users_by_state SET session_token='XYZ' WHERE username='user1' AND state = 'TX';",
                       "Cannot directly modify a materialized view")

        # cannot delete a row
        assert_invalid(session, "DELETE from users_by_state where state='TX';",
                       "Cannot directly modify a materialized view")

        # cannot delete a cell
        assert_invalid(session, "DELETE session_token from users_by_state where state='TX';",
                       "Cannot directly modify a materialized view")

        # cannot alter a table
        assert_invalid(session, "ALTER TABLE users_by_state ADD first_name varchar",
                       "Cannot use ALTER TABLE on Materialized View")

    def drop_mv_test(self):
        """Test that we can drop a view properly"""

        session = self.prepare(user_table=True)

        # create another materialized view
        session.execute(("CREATE MATERIALIZED VIEW users_by_birth_year AS "
                         "SELECT * FROM users WHERE birth_year IS NOT NULL AND "
                         "username IS NOT NULL PRIMARY KEY (birth_year, username)"))

        result = session.execute(("SELECT * FROM system_schema.views "
                                  "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING"))
        self.assertEqual(len(result), 2, "Expecting {} materialized view, got {}".format(2, len(result)))

        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        result = session.execute(("SELECT * FROM system_schema.views "
                                  "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING"))
        self.assertEqual(len(result), 1, "Expecting {} materialized view, got {}".format(1, len(result)))

    def drop_column_test(self):
        """Test that we cannot drop a column if it is used by a MV"""

        session = self.prepare(user_table=True)

        result = session.execute(("SELECT * FROM system_schema.views "
                                  "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING"))
        self.assertEqual(len(result), 1, "Expecting {} materialized view, got {}".format(1, len(result)))

        assert_invalid(
            session,
            "ALTER TABLE ks.users DROP state;",
            "Cannot drop column state, depended on by materialized views"
        )

    def drop_table_test(self):
        """Test that we cannot drop a table without deleting its MVs first"""

        session = self.prepare(user_table=True)

        result = session.execute(("SELECT * FROM system_schema.views "
                                  "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING"))
        self.assertEqual(
            len(result), 1,
            "Expecting {} materialized view, got {}".format(1, len(result))
        )

        assert_invalid(
            session,
            "DROP TABLE ks.users;",
            "Cannot drop table when materialized views still depend on it"
        )

        result = session.execute(("SELECT * FROM system_schema.views "
                                  "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING"))
        self.assertEqual(
            len(result), 1,
            "Expecting {} materialized view, got {}".format(1, len(result))
        )

        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")
        session.execute("DROP TABLE ks.users;")

        result = session.execute(("SELECT * FROM system_schema.views "
                                  "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING"))
        self.assertEqual(
            len(result), 0,
            "Expecting {} materialized view, got {}".format(1, len(result))
        )

    def clustering_column_test(self):
        """Test that we can use clustering columns as primary key for a materialized view"""

        session = self.prepare()

        session.execute(("CREATE TABLE users (username varchar, password varchar, gender varchar, "
                         "session_token varchar, state varchar, birth_year bigint, "
                         "PRIMARY KEY (username, state, birth_year));"))

        # create a materialized view that use a compound key
        session.execute(("CREATE MATERIALIZED VIEW users_by_state_birth_year "
                         "AS SELECT * FROM users WHERE state IS NOT NULL AND birth_year IS NOT NULL "
                         "AND username IS NOT NULL PRIMARY KEY (state, birth_year, username)"))

        self._insert_data(session)

        result = session.execute("SELECT * FROM ks.users_by_state_birth_year WHERE state='TX'")
        self.assertEqual(len(result), 2, "Expecting {} users, got {}".format(2, len(result)))

        result = session.execute("SELECT * FROM ks.users_by_state_birth_year WHERE state='TX' AND birth_year=1968")
        self.assertEqual(len(result), 1, "Expecting {} users, got {}".format(1, len(result)))

    def add_dc_after_mv_test(self):
        """Test that materialized views work as expected when adding a datacenter."""

        session = self.prepare()

        debug("Creating schema")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Writing 1k to base")
        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))

        debug("Reading 1k from view")
        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])

        debug("Reading 1k from base")
        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE id = {}".format(i), [i, i])

        debug("Bootstrapping new node in another dc")
        node4 = new_node(self.cluster, data_center='dc2')
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug("Bootstrapping new node in another dc")
        node5 = new_node(self.cluster, remote_debug_port='1414', data_center='dc2')
        node5.start()

        session2 = self.patient_exclusive_cql_connection(node4)

        debug("Verifying data from new node in view")
        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t_by_v WHERE v = {}".format(i), [i, i])

        debug("Inserting 100 into base")
        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))

        debug("Verify 100 in view")
        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])

    def add_node_after_mv_test(self):
        """Test that materialized views work as expected when adding a node."""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])

        node4 = new_node(self.cluster)
        node4.start(wait_for_binary_proto=True)

        session2 = self.patient_exclusive_cql_connection(node4)

        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t_by_v WHERE v = {}".format(i), [i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])

    def allow_filtering_test(self):
        """Test that allow filtering works as usual for a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {v}".format(v=i), [i, i, 'a', 3.0])

        rows = session.execute("SELECT * FROM t_by_v2 WHERE v2 = 'a'")
        self.assertEqual(len(rows), 1000, "Expected 1000 rows but got {}".format(len(rows)))

        assert_invalid(session, "SELECT * FROM t_by_v WHERE v = 1 AND v2 = 'a'")
        assert_invalid(session, "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = 1")

        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {} AND v3 = 3.0 ALLOW FILTERING".format(i),
                [i, i, 'a', 3.0]
            )
            assert_one(
                session,
                "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = {} ALLOW FILTERING".format(i),
                ['a', i, i, 3.0]
            )

    def secondary_index_test(self):
        """Test that secondary indexes cannot be created on a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        assert_invalid(session, "CREATE INDEX ON t_by_v (v2)",
                       "Secondary indexes are not supported on materialized views")

    def ttl_test(self):
        """
        Test that TTL works as expected for a materialized view
        @expected_result The TTL is propagated properly between tables.
        """

        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int, v3 int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        for i in xrange(100):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, {v}, {v}) USING TTL 10".format(v=i))

        for i in xrange(100):
            assert_one(session, "SELECT * FROM t_by_v2 WHERE v2 = {}".format(i), [i, i, i, i])

        time.sleep(20)

        rows = session.execute("SELECT * FROM t_by_v2")
        self.assertEqual(len(rows), 0, "Expected 0 rows but got {}".format(len(rows)))

    def query_all_new_column_test(self):
        """
        Test that a materialized view created with a 'SELECT *' works as expected when adding a new column
        @expected_result The new column is present in the view.
        """

        session = self.prepare(user_table=True)

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

        session.execute("ALTER TABLE users ADD first_name varchar;")

        results = session.execute("SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'")
        self.assertEqual(len(results), 1)
        self.assertTrue(hasattr(results[0], 'first_name'), 'Column "first_name" not found')
        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, None, 'f', 'ch@ngem3a', None]
        )

    def query_new_column_test(self):
        """
        Test that a materialized view created with 'SELECT <col1, ...>' works as expected when adding a new column
        @expected_result The new column is not present in the view.
        """

        session = self.prepare(user_table=True)

        session.execute(("CREATE MATERIALIZED VIEW users_by_state2 AS SELECT username FROM users "
                         "WHERE STATE IS NOT NULL AND USERNAME IS NOT NULL PRIMARY KEY (state, username)"))

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1']
        )

        session.execute("ALTER TABLE users ADD first_name varchar;")

        results = session.execute("SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'")
        self.assertEqual(len(results), 1)
        self.assertFalse(hasattr(results[0], 'first_name'), 'Column "first_name" found in view')
        assert_one(
            session,
            "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1']
        )

    def lwt_test(self):
        """Test that lightweight transaction behave properly with a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Inserting initial data using IF NOT EXISTS")
        for i in xrange(1000):
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i)
            )
        self._replay_batchlogs()

        debug("All rows should have been inserted")
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug("Tyring to UpInsert data with a different value using IF NOT EXISTS")
        for i in xrange(1000):
            v = i * 2
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({id}, {v}, 'a', 3.0) IF NOT EXISTS".format(id=i, v=v)
            )
        self._replay_batchlogs()

        debug("No rows should have changed")
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug("Update the 10 first rows with a different value")
        for i in xrange(1000):
            v = i + 2000
            session.execute(
                "UPDATE t SET v={v} WHERE id = {id} IF v < 10".format(id=i, v=v)
            )
        self._replay_batchlogs()

        debug("Verify that only the 10 first rows changed.")
        results = session.execute("SELECT * FROM t_by_v;")
        self.assertEqual(len(results), 1000)
        for i in xrange(1000):
            v = i + 2000 if i < 10 else i
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(v),
                [v, i, 'a', 3.0]
            )

        debug("Deleting the first 10 rows")
        for i in xrange(1000):
            v = i + 2000
            session.execute(
                "DELETE FROM t WHERE id = {id} IF v = {v} ".format(id=i, v=v)
            )
        self._replay_batchlogs()

        debug("Verify that only the 10 first rows have been deleted.")
        results = session.execute("SELECT * FROM t_by_v;")
        self.assertEqual(len(results), 990)
        for i in xrange(10, 1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

    def interrupt_build_process_test(self):
        """Test that an interupted MV build process is resumed as it should"""

        session = self.prepare(options={'hinted_handoff_enabled': False})
        node1, node2, node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")

        debug("Inserting initial data")
        for i in xrange(10000):
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i)
            )

        debug("Create a MV")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Stop the cluster. Interrupt the MV build process.")
        self.cluster.stop()

        debug("Restart the cluster")
        self.cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        session.execute("USE ks")

        debug("MV shouldn't be built yet.")
        assert_none(session, "SELECT * FROM t_by_v WHERE v=10000;")

        debug("Wait and ensure the MV build resumed. Waiting up to 2 minutes.")
        start = time.time()
        while True:
            try:
                result = session.execute("SELECT count(*) FROM t_by_v;")
                self.assertNotEqual(result[0].count, 10000)
            except AssertionError:
                debug("MV build process is finished")
                break

            elapsed = (time.time() - start) / 60
            if elapsed > 2:
                break

            time.sleep(5)

        debug("Verify all data")
        result = session.execute("SELECT count(*) FROM t_by_v;")
        self.assertEqual(result[0].count, 10000)
        for i in xrange(10000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.ALL
            )

    def simple_repair_test(self):
        """
        Test that a materialized view are consistent after a simple repair.
        """

        session = self.prepare(rf=3, options={'hinted_handoff_enabled': False})
        node1, node2, node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Shutdown node2')
        node2.stop(wait_other_notice=True)

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        self._replay_batchlogs()

        debug('Verify the data in the MV with CL=ONE')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug('Verify the data in the MV with CL=ALL. All should be unavailable.')
        for i in xrange(1000):
            statement = SimpleStatement(
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                consistency_level=ConsistencyLevel.ALL
            )

            assert_unavailable(
                session.execute,
                statement
            )

        debug('Start node2, and repair')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node1.repair()

        debug('Verify the data in the MV with CL=ONE. All should be available now.')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.ONE
            )

    def base_replica_repair_test(self):
        """
        Test that a materialized view are consistent after the repair of the base replica.
        """

        self.prepare(rf=3)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Write initial data')
        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        self._replay_batchlogs()

        debug('Verify the data in the MV with CL=ALL')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.ALL
            )

        debug('Shutdown node1')
        node1.stop(wait_other_notice=True)
        debug('Delete node1 data')
        node1.clear(clear_all=True)
        debug('Restarting node1')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        debug('Shutdown node2 and node3')
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        debug('Verify that there is no data on node1')
        for i in xrange(1000):
            assert_none(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i)
            )

        debug('Restarting node2 and node3')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Just repair the base replica
        node1.nodetool("repair ks t")

        debug('Verify data with cl=ALL')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

    def complex_repair_test(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """

        session = self.prepare(rf=5, options={'hinted_handoff_enabled': False}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)"
                        "WITH gc_grace_seconds = 5")
        session.execute(("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Shutdown node2 and node3')
        node2.stop()
        node3.stop(wait_other_notice=True)

        debug('Write initial data to node1 (will be replicated to node4 and node5)')
        for i in xrange(1000):
            session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        debug('Verify the data in the MV on node1 with CL=ONE')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug('Shutdown node1, node4 and node5')
        node1.stop()
        node4.stop()
        node5.stop()

        debug('Start nodes 2 and 3')
        node2.start()
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        session2 = self.patient_cql_connection(node2)

        debug('Verify the data in the MV on node2 with CL=ONE. No rows should be found.')
        for i in xrange(1000):
            assert_none(
                session2,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(i)
            )

        debug('Write new data in node2 and node3 that overlap those in node1, node4 and node5')
        for i in xrange(1000):
            # we write i*2 as value, instead of i
            session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i * 2))

        debug('Verify the new data in the MV on node2 with CL=ONE')
        for i in xrange(1000):
            v = i * 2
            assert_one(
                session2,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(v),
                [v, v, 'a', 3.0]
            )

        debug('Wait for batchlogs to expire from node2 and node3')
        time.sleep(5)

        debug('Start remaining nodes')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node5.start(wait_other_notice=True, wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)

        debug('Read data from MV at QUORUM (old data should be returned)')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.QUORUM
            )

        debug('Run global repair on node1')
        node1.repair()

        debug('Read data from MV at quorum (new data should be returned after repair)')
        for i in xrange(1000):
            v = i * 2
            assert_one(
                session,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(v),
                [v, v, 'a', 3.0],
                cl=ConsistencyLevel.QUORUM
            )

    def really_complex_repair_test(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """

        session = self.prepare(rf=5, options={'hinted_handoff_enabled': False}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int, v int, v2 text, v3 decimal, PRIMARY KEY(id, v, v2))"
                        "WITH gc_grace_seconds = 1")
        session.execute(("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL AND v IS NOT NULL AND "
                         "v2 IS NOT NULL PRIMARY KEY (v2, v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Shutdown node2 and node3')
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0)")
        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'a', 3.0)")
        self._replay_batchlogs()
        debug('Verify the data in the MV on node1 with CL=ONE')
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [['a', 1, 1, 3.0], ['a', 2, 2, 3.0]])

        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'b', 3.0)")
        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'b', 3.0)")
        self._replay_batchlogs()
        debug('Verify the data in the MV on node1 with CL=ONE')
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'b'", [['b', 1, 1, 3.0], ['b', 2, 2, 3.0]])

        session.shutdown()

        debug('Shutdown node1, node4 and node5')
        node1.stop()
        node4.stop()
        node5.stop()

        debug('Start nodes 2 and 3')
        node2.start()
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        session2 = self.patient_cql_connection(node2)
        session2.execute('USE ks')

        debug('Verify the data in the MV on node2 with CL=ONE. No rows should be found.')
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'")

        debug('Write new data in node2 that overlap those in node1')
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'c', 3.0)")
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'c', 3.0)")
        self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'", [['c', 1, 1, 3.0], ['c', 2, 2, 3.0]])

        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'd', 3.0)")
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'd', 3.0)")
        self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'", [['d', 1, 1, 3.0], ['d', 2, 2, 3.0]])

        debug("Composite delete of everything")
        session2.execute("DELETE FROM ks.t WHERE id = 1 and v = 1")
        session2.execute("DELETE FROM ks.t WHERE id = 2 and v = 2")
        self._replay_batchlogs()
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'")
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'")

        debug('Wait for batchlogs to expire from node2 and node3')
        time.sleep(5)

        debug('Start remaining nodes')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        node5.start(wait_other_notice=True, wait_for_binary_proto=True)

        # at this point the data isn't repaired so we have an inconsistency.
        # this value should return None
        assert_all(
            session2,
            "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [['a', 1, 1, 3.0], ['a', 2, 2, 3.0]],
            cl=ConsistencyLevel.QUORUM
        )

        debug('Run global repair on node1')
        node1.repair()

        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", cl=ConsistencyLevel.QUORUM)


# For read verification
class MutationPresence(Enum):
    match = 1
    extra = 2
    missing = 3
    excluded = 4
    unknown = 5


class MM(object):
    mp = None

    def out(self):
        pass


class Match(MM):

    def __init__(self):
        self.mp = MutationPresence.match

    def out(self):
        return None


class Extra(MM):
    expecting = None
    value = None
    row = None

    def __init__(self, expecting, value, row):
        self.mp = MutationPresence.extra
        self.expecting = expecting
        self.value = value
        self.row = row

    def out(self):
        return "Extra. Expected {} instead of {}; row: {}".format(self.expecting, self.value, self.row)


class Missing(MM):
    value = None
    row = None

    def __init__(self, value, row):
        self.mp = MutationPresence.missing
        self.value = value
        self.row = row

    def out(self):
        return "Missing. At {}".format(self.row)


class Excluded(MM):

    def __init__(self):
        self.mp = MutationPresence.excluded

    def out(self):
        return None


class Unknown(MM):

    def __init__(self):
        self.mp = MutationPresence.unknown

    def out(self):
        return None


readConsistency = ConsistencyLevel.QUORUM
writeConsistency = ConsistencyLevel.QUORUM
SimpleRow = collections.namedtuple('SimpleRow', 'a b c d')


def row_generate(i):
    return SimpleRow(a=i % 20, b=(i % 400) / 20, c=i, d=i)


# Create a threaded session and execute queries from a Queue
def thread_session(ip, queue, start, end, rows):

    def execute_query(session, select_gi, i):
        row = row_generate(i)
        if (row.a, row.b) in rows:
            base = rows[(row.a, row.b)]
        else:
            base = -1
        gi = session.execute(select_gi, [row.c, row.a])
        if base == i and len(gi) == 1:
            return Match()
        elif base != i and len(gi) == 1:
            return Extra(base, i, (gi[0][0], gi[0][1], gi[0][2], gi[0][3]))
        elif base == i and len(gi) == 0:
            return Missing(base, i)
        elif base != i and len(gi) == 0:
            return Excluded()
        else:
            return Unknown()

    try:
        cluster = Cluster([ip])
        session = cluster.connect()
        select_gi = session.prepare("SELECT * FROM mvtest.mv1 WHERE c = ? AND a = ?")
        select_gi.consistency_level = readConsistency

        for i in range(start, end):
            ret = execute_query(session, select_gi, i)
            queue.put_nowait(ret)
    except Exception as e:
        print str(e)
        queue.close()


@since('3.0')
@skipIf(sys.platform == 'win32', 'Bug in python on Windows: https://bugs.python.org/issue10128')
class TestMaterializedViewsConsistency(Tester):

    def prepare(self, user_table=False):
        cluster = self.cluster
        cluster.populate(3).start()
        node2 = cluster.nodelist()[1]

        # Keep the status of async requests
        self.exception_type = collections.Counter()
        self.num_request_done = 0
        self.counts = {}
        for mp in MutationPresence:
            self.counts[mp] = 0
        self.rows = {}
        self.update_stats_every = 100

        debug("Set to talk to node 2")
        self.session = self.patient_cql_connection(node2)

        return self.session

    def _print_write_status(self, row):
        output = "\r{}".format(row)
        for key in self.exception_type.keys():
            output = "{} ({}: {})".format(output, key, self.exception_type[key])
        sys.stdout.write(output)
        sys.stdout.flush()

    def _print_read_status(self, row):
        if self.counts[MutationPresence.unknown] == 0:
            sys.stdout.write(
                "\rOn {}; match: {}; extra: {}; missing: {}".format(
                    row,
                    self.counts[MutationPresence.match],
                    self.counts[MutationPresence.extra],
                    self.counts[MutationPresence.missing])
            )
        else:
            sys.stdout.write(
                "\rOn {}; match: {}; extra: {}; missing: {}; WTF: {}".format(
                    row,
                    self.counts[MutationPresence.match],
                    self.counts[MutationPresence.extra],
                    self.counts[MutationPresence.missing],
                    self.counts[MutationPresence.unkown])
            )
        sys.stdout.flush()

    def _do_row(self, insert_stmt, i):

        # Error callback for async requests
        def handle_errors(row, exc):
            self.num_request_done += 1
            try:
                name = type(exc).__name__
                self.exception_type[name] += 1
            except Exception as e:
                print traceback.format_exception_only(type(e), e)

        # Success callback for async requests
        def success_callback(row):
            self.num_request_done += 1

        if i % self.update_stats_every == 0:
            self._print_write_status(i)

        row = row_generate(i)

        async = self.session.execute_async(insert_stmt, row)
        errors = partial(handle_errors, row)
        async.add_callbacks(success_callback, errors)

    def _populate_rows(self):
        statement = SimpleStatement(
            "SELECT a, b, c FROM mvtest.test1",
            consistency_level=readConsistency
        )
        data = self.session.execute(statement)
        for row in data:
            self.rows[(row.a, row.b)] = row.c

    def consistent_reads_after_write_test(self):

        session = self.prepare()
        [node1, node2, node3] = self.cluster.nodelist()

        # Test config
        lower = 0
        upper = 100000
        processes = 4
        queues = [None] * processes
        eachProcess = (upper - lower) / processes

        debug("Creating schema")
        session.execute(
            ("CREATE KEYSPACE IF NOT EXISTS mvtest WITH replication = "
             "{'class': 'SimpleStrategy', 'replication_factor': '3'}")
        )
        session.execute(
            "CREATE TABLE mvtest.test1 (a int, b int, c int, d int, PRIMARY KEY (a,b))"
        )
        session.cluster.control_connection.wait_for_schema_agreement()

        insert1 = session.prepare("INSERT INTO mvtest.test1 (a,b,c,d) VALUES (?,?,?,?)")
        insert1.consistency_level = writeConsistency

        debug("Writing data to base table")
        for i in range(upper / 10):
            self._do_row(insert1, i)

        debug("Creating materialized view")
        session.execute(
            ('CREATE MATERIALIZED VIEW mvtest.mv1 AS '
             'SELECT a,b,c,d FROM mvtest.test1 WHERE a IS NOT NULL AND b IS NOT NULL AND '
             'c IS NOT NULL PRIMARY KEY (c,a,b)')
        )
        session.cluster.control_connection.wait_for_schema_agreement()

        debug("Writing more data to base table")
        for i in range(upper / 10, upper):
            self._do_row(insert1, i)

        # Wait that all requests are done
        while self.num_request_done < upper:
            time.sleep(1)

        debug("Making sure all batchlogs are replayed on node1")
        node1.nodetool("replaybatchlog")
        debug("Making sure all batchlogs are replayed on node2")
        node2.nodetool("replaybatchlog")
        debug("Making sure all batchlogs are replayed on node3")
        node3.nodetool("replaybatchlog")

        debug("Finished writes, now verifying reads")
        self._populate_rows()

        for i in range(processes):
            start = lower + (eachProcess * i)
            if i == processes - 1:
                end = upper
            else:
                end = lower + (eachProcess * (i + 1))
            q = Queue()
            node_ip = self.get_ip_from_node(node2)
            p = Process(target=thread_session, args=(node_ip, q, start, end, self.rows))
            p.start()
            queues[i] = q

        for i in range(lower, upper):
            if i % 100 == 0:
                self._print_read_status(i)
            mm = queues[i % processes].get()
            if not mm.out() is None:
                sys.stdout.write("\r{}\n" .format(mm.out()))
            self.counts[mm.mp] += 1

        self._print_read_status(upper)
        sys.stdout.write("\n")
        sys.stdout.flush()
