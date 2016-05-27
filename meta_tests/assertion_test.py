from cassandra import AlreadyExists, ConsistencyLevel
from dtest import Tester
from assertions import (assert_one, assert_none, assert_exception, assert_unavailable,
                        assert_invalid, assert_unauthorized, assert_all, assert_almost_equal,
                        assert_length_equal, assert_row_count)
from cassandra.query import SimpleStatement


class TestAssertionMethods(Tester):

    def prepare(self):
        cluster = self.cluster
        cluster.set_install_dir(version="git:cassandra-3.0.6")
        cluster.populate(3).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)

        self.create_ks(session, 'ks', 1)

        return session, cluster

    def clean_cluster(self, session):
        session.execute("DROP TABLE ks.test")
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

    def assertions_test(self):
        session, cluster = self.prepare()
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        def assert_exception_test(session):
            assert_exception(session, "CREATE TABLE test (k int PRIMARY KEY, v int)", matching="Table 'ks.test' already exists", expected=AlreadyExists)

        def assert_unavailable_test(session):
            node = cluster.nodelist()[0]
            node.stop()
            query = SimpleStatement("SELECT * FROM test", consistency_level=ConsistencyLevel.ALL)
            assert_unavailable(session.execute, query)
            node.start()

        def assert_invalid_test(session):
            assert_invalid(session, "USE nonexistantks")

        def assert_unauthorized_test(session):
            assert_unauthorized(session, "DROP USER dne", "You have to be logged in and not anonymous to perform this request")

        def assert_one_test(session):
            session.execute("INSERT INTO test (k, v) VALUES (1, 1)")
            assert_one(session, "SELECT * FROM test", [1, 1])

        def assert_none_test(session):
            assert_none(session, "SELECT * FROM test")

        def assert_all_test(session):
            for i in range(0, 10):
                session.execute("INSERT INTO test (k, v) VALUES ({i}, {i})".format(i=i))
            assert_all(session, "SELECT k, v FROM test", [[i, i] for i in range(0, 10)], ignore_order=True)

        def assert_almost_equal_test(session):
            assert_almost_equal(1, 1.1, 1.2, 1.9, error=1.0)

        def assert_row_count_test(session):
            session.execute("INSERT INTO test (k, v) VALUES (1, 1)")
            assert_row_count(session, 'test', 1)
            assert_row_count(session, 'test', 1, where="k = 1")

        def assert_length_equal_test(session):
            check = [1, 2, 3, 4]
            assert_length_equal(check, 4)

        assert_exception_test(session)
        self.clean_cluster(session)

        assert_unavailable_test(session)
        self.clean_cluster(session)

        assert_invalid_test(session)
        self.clean_cluster(session)

        assert_unauthorized_test(session)
        self.clean_cluster(session)

        assert_one_test(session)
        self.clean_cluster(session)

        assert_none_test(session)
        self.clean_cluster(session)

        assert_all_test(session)
        self.clean_cluster(session)

        assert_almost_equal_test(session)
        self.clean_cluster(session)

        assert_row_count_test(session)
        self.clean_cluster(session)

        assert_length_equal_test(session)
