import time

from pyassertions import assert_invalid, assert_unavailable
from dtest import Tester
from cassandra import ConsistencyLevel, Timeout
from cassandra.query import SimpleStatement
from cassandra.policies import RetryPolicy

cql_version="3.0.0"

class TestBatch(Tester):

    def counter_batch_accepts_counter_mutations_test(self):
        """ Test that counter batch accepts counter mutations """
        session = self.prepare()
        session.execute("""
            BEGIN COUNTER BATCH
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://bar.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 2 and url = 'http://baz.com'
            APPLY BATCH
        """)
        rows = session.execute("SELECT total FROM clicks")
        assert [list(rows[0]), list(rows[1]), list(rows[2])] == [[1], [1], [1]], rows

    def counter_batch_rejects_regular_mutations_test(self):
        """ Test that counter batch rejects non-counter mutations """
        session = self.prepare()
        if self.cluster.version() < '2.1':
            err = "Only counter mutations are allowed in COUNTER batches"
        else:
            err = "Cannot include non-counter statement in a counter batch"

        assert_invalid(session, """
            BEGIN COUNTER BATCH
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://bar.com'
            UPDATE clicks SET total = total + 1 WHERE userid = 2 and url = 'http://baz.com'
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            APPLY BATCH
            """, matching=err)

    def logged_batch_accepts_regular_mutations_test(self):
        """ Test that logged batch accepts regular mutations """
        session = self.prepare()
        session.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        rows = session.execute("SELECT * FROM users")
        res = sorted(rows)
        assert [list(res[0]), list(res[1])] == [[0, u'Jack', u'Sparrow'], [1, u'Will', u'Turner']], res

    def logged_batch_rejects_counter_mutations_test(self):
        """ Test that logged batch rejects counter mutations """
        session = self.prepare()
        if self.cluster.version() < '2.1':
            err = "Counter mutations are only allowed in COUNTER batches"
        else:
            err = "Cannot include a counter statement in a logged batch"

        assert_invalid(session, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            UPDATE clicks SET total = total + 1 WHERE userid = 1 and url = 'http://foo.com'
            APPLY BATCH
            """, matching=err)

    def unlogged_batch_accepts_regular_mutations_test(self):
        """ Test that unlogged batch accepts regular mutations """
        session = self.prepare()
        session.execute("""
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (2, 'Elizabeth', 'Swann')
            APPLY BATCH
        """)
        rows = session.execute("SELECT * FROM users")
        res = sorted(rows)
        assert [list(res[0]), list(res[1])] == [[0, u'Jack', u'Sparrow'], [2, u'Elizabeth', u'Swann']], res

    def unlogged_batch_rejects_counter_mutations_test(self):
        """ Test that unlogged batch rejects counter mutations """
        session = self.prepare()
        if self.cluster.version() < '2.1':
            err = "Counter mutations are only allowed in COUNTER batches"
        else:
            err = "Counter and non-counter mutations cannot exist in the same batch"

        assert_invalid(session, """
            BEGIN UNLOGGED BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (2, 'Elizabeth', 'Swann')
            UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'
            APPLY BATCH
            """, matching=err)

    def logged_batch_throws_uae_test(self):
        """ Test that logged batch throws UAE if there aren't enough live nodes """
        session = self.prepare(nodes=3)
        [ node.stop(wait_other_notice=True) for node in self.cluster.nodelist()[1:] ]
        session.consistency_level = 'ONE'
        assert_unavailable(session.execute, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)

    def logged_batch_doesnt_throw_uae_test(self):
        """ Test that logged batch DOES NOT throw UAE if there are at least 2 live nodes """
        session = self.prepare(nodes=3)
        self.cluster.nodelist()[-1].stop(wait_other_notice=True)
        query = SimpleStatement("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, consistency_level=ConsistencyLevel.ANY)
        session.execute(query)
        assert True

    def acknowledged_by_batchlog_not_set_when_batchlog_write_fails_test(self):
        """ Test that acknowledged_by_batchlog is False if batchlog can't be written """
        session = self.prepare(nodes=3, compression=False)
        # kill 2 of the 3 nodes (all the batchlog write candidates).
        [ node.stop(gently=False) for node in self.cluster.nodelist()[1:] ]
        self.assert_timedout(session, """
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, ConsistencyLevel.ONE, received_responses=0)

    def acknowledged_by_batchlog_set_when_batchlog_write_succeeds_test(self):
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

    def batch_uses_proper_timestamp_test(self):
        """ Test that each statement will be executed with provided BATCH timestamp """
        session = self.prepare()
        session.execute("""
            BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow')
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """)
        rows = session.execute("SELECT id, writetime(firstname), writetime(lastname) FROM users")
        res = sorted(rows)
        assert [list(res[0]), list(res[1])] == [[0, 1111111111111111, 1111111111111111], [1, 1111111111111111, 1111111111111111]], res

    def only_one_timestamp_is_valid_test(self):
        """ Test that TIMESTAMP must not be used in the statements within the batch. """
        session = self.prepare()
        assert_invalid(session, """
            BEGIN BATCH USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') USING TIMESTAMP 2
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner')
            APPLY BATCH
        """, matching="Timestamp must be set either on BATCH or individual statements")

    def each_statement_in_batch_uses_proper_timestamp_test(self):
        """ Test that each statement will be executed with its own timestamp """
        session = self.prepare()
        session.execute("""
            BEGIN BATCH
            INSERT INTO users (id, firstname, lastname) VALUES (0, 'Jack', 'Sparrow') USING TIMESTAMP 1111111111111111
            INSERT INTO users (id, firstname, lastname) VALUES (1, 'Will', 'Turner') USING TIMESTAMP 1111111111111112
            APPLY BATCH
        """)
        rows = session.execute("SELECT id, writetime(firstname), writetime(lastname) FROM users")
        res = sorted(rows)
        assert [list(res[0]), list(res[1])] == [[0, 1111111111111111, 1111111111111111], [1, 1111111111111112, 1111111111111112]], res

    def assert_timedout(self, session, query, cl, acknowledged_by=None,
                        received_responses=None):
        try:
            statement = SimpleStatement(query, consistency_level=cl)
            session.execute(statement, timeout=None)
        except Timeout as e:
            if not received_responses is None:
                msg = "Expecting received_responses to be %s, got: %s" % (
                        received_responses, e.received_responses,)
                assert e.received_responses == received_responses, msg
        except Exception as e:
            assert False, "Expecting TimedOutException, got:" + str(e)
        else:
            assert False, "Expecting TimedOutException but no exception was raised"

    def prepare(self, nodes=1, compression=True):
        if not self.cluster.nodelist():
            self.cluster.populate(nodes).start(wait_other_notice=True)

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, version=cql_version)
        session.execute("DROP KEYSPACE IF EXISTS ks")
        self.create_ks(session, 'ks', nodes)
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
        return session
