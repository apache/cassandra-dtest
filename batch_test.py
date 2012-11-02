import cql
import time

from assertions import assert_invalid, assert_unavailable
from cql.cassandra.ttypes import Compression, ConsistencyLevel, TimedOutException
from dtest import Tester
from tools import since

cql_version="3.0.0"

class TestBatch(Tester):

    @since('1.2')
    def counter_batch_accepts_counter_mutations_test(self):
        """ Test that counter batch accepts counter mutations """
        cursor = self.prepare()
        cursor.execute("""
            BEGIN COUNTER BATCH
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://bar.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 2 and url = 'http://baz.com'
            APPLY BATCH
        """)
        cursor.execute("SELECT total FROM clicks")
        res = cursor.fetchall()
        assert res == [[1], [1], [1]], res

    @since('1.2')
    def counter_batch_rejects_regular_mutations_test(self):
        """ Test that counter batch rejects non-counter mutations """
        cursor = self.prepare()
        assert_invalid(cursor, """
            BEGIN COUNTER BATCH
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://bar.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 2 and url = 'http://baz.com'
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            APPLY BATCH
        """, matching="Only counter mutations are allowed in COUNTER batches")

    @since('1.2')
    def logged_batch_accepts_regular_mutations_test(self):
        """ Test that logged batch accepts regular mutations """
        cursor = self.prepare()
        cursor.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        cursor.execute("SELECT * FROM users")
        res = sorted(cursor.fetchall())
        assert res == [[0, u'Jack', u'Sparrow'], [1, u'Will', u'Turner']], res

    @since('1.2')
    def logged_batch_rejects_counter_mutations_test(self):
        """ Test that logged batch rejects counter mutations """
        cursor = self.prepare()
        assert_invalid(cursor, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            APPLY BATCH
        """, matching="Counter mutations are only allowed in COUNTER batches")

    @since('1.2')
    def unlogged_batch_accepts_regular_mutations_test(self):
        """ Test that unlogged batch accepts regular mutations """
        cursor = self.prepare()
        cursor.execute("""
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (2, 'Elizabeth', 'Swann')
            APPLY BATCH
        """)
        cursor.execute("SELECT * FROM users")
        res = sorted(cursor.fetchall())
        assert res == [[0, u'Jack', u'Sparrow'], [2, u'Elizabeth', u'Swann']], res

    @since('1.2')
    def unlogged_batch_rejects_counter_mutations_test(self):
        """ Test that unlogged batch rejects counter mutations """
        cursor = self.prepare()
        assert_invalid(cursor, """
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (2, 'Elizabeth', 'Swann')
            UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'
            APPLY BATCH
        """, matching="Counter mutations are only allowed in COUNTER batches")

    @since('1.2')
    def logged_batch_throws_uae_test(self):
        """ Test that logged batch throws UAE if there aren't enough live nodes """
        cursor = self.prepare(nodes=3)
        [ node.stop(wait_other_notice=True) for node in self.cluster.nodelist()[1:] ]
        cursor.consistency_level = 'ANY'
        assert_unavailable(cursor.execute, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)

    @since('1.2')
    def logged_batch_doesnt_throw_uae_test(self):
        """ Test that logged batch DOES NOT throw UAE if there are at least 2 live nodes """
        cursor = self.prepare(nodes=3)
        self.cluster.nodelist()[-1].stop(wait_other_notice=True)
        cursor.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, consistency_level="ANY")
        assert True

    @since('1.2')
    def aknowledged_by_batchlog_not_set_when_batchlog_write_fails_test(self):
        """ Test that acknowledged_by_batchlog is False if batchlog can't be written """
        cursor = self.prepare(nodes=3)
        # kill 2 of the 3 nodes (all the batchlog write candidates).
        [ node.stop() for node in self.cluster.nodelist()[1:] ]
        self.assert_timedout(cursor, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, ConsistencyLevel.ONE, acknowledged_by_batchlog=False)

    @since('1.2')
    def aknowledged_by_batchlog_set_when_batchlog_write_succeeds_test(self):
        """ Test that acknowledged_by_batchlog is True if batchlog can be written """
        cursor = self.prepare(nodes=3)
        # kill one of the nodes so that batchlog will be written, but the write will fail.
        self.cluster.nodelist()[-1].stop()
        self.assert_timedout(cursor, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, ConsistencyLevel.THREE, acknowledged_by_batchlog=True)

    def assert_timedout(self, cursor, query, cl, acknowledged_by=None,
                        acknowledged_by_batchlog=None):
        client = cursor._connection.client
        try:
            client.execute_cql3_query(query, Compression.NONE, cl)
        except TimedOutException as e:
            if not acknowledged_by_batchlog is None:
                msg = "Expecting acknowledged_by_batchlog to be %s, got: %s" % (
                        acknowledged_by_batchlog, e.acknowledged_by_batchlog,)
                assert e.acknowledged_by_batchlog == acknowledged_by_batchlog, msg
        except Exception as e:
            assert False, "Expecting TimedOutException, got:" + str(e)
        else:
            assert False, "Expecting TimedOutException but no exception was raised"

    def prepare(self, nodes=1):
        self.cluster.populate(nodes).start()
        time.sleep(.5)
        node1 = self.cluster.nodelist()[0]
        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', nodes)
        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
             );
         """)
        cursor.execute("""
            CREATE TABLE users (
                id int,
                firstname text,
                lastname text,
                PRIMARY KEY (id)
             );
         """)
        time.sleep(.5)
        return cursor
