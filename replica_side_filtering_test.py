from abc import abstractmethod

import pytest
from cassandra import ConsistencyLevel as CL
from cassandra.query import SimpleStatement

from dtest import Tester, create_ks, mk_bman_path
from tools.assertions import (assert_all, assert_none, assert_one)

since = pytest.mark.since
keyspace = 'ks'

class ReplicaSideFiltering(Tester):
    """
    @jira_ticket CASSANDRA-8272, CASSANDRA-8273
    Base consistency test for queries involving replica-side filtering when some of the replicas have stale data.
    """
    __test__ = False

    def _prepare_cluster(self, create_table, create_index=None, both_nodes=None, only_node1=None, only_node2=None):
        """
        :param create_table a table creation CQL query
        :param create_index an index creation CQL query, that will be executed depending on ``_create_index`` method
        :param both_nodes queries to be executed in both nodes with CL=ALL
        :param only_node1 queries to be executed in the first node only, with CL=ONE, while the second node is stopped
        :param only_node2 queries to be executed in the second node only, with CL=ONE, while the first node is stopped
        :return: a session connected exclusively to the first node with CL=ALL
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't interfere with the test
        if only_node1 or only_node2:
            cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
            cluster.set_batch_commitlog(enabled=True)

        cluster.populate(2)
        node1, node2 = cluster.nodelist()
        cluster.start()

        session = self.patient_exclusive_cql_connection(node1, consistency_level=CL.ALL)
        create_ks(session, keyspace, 2)
        session.execute("USE " + keyspace)

        # create the table
        session.execute(create_table)

        # create the index if it's required
        if self.create_index():
            session.execute(create_index)

        # execute the queries for both nodes with CL=ALL
        if both_nodes:
            for q in both_nodes:
                session.execute(q)

        # execute the queries for the first node only with the second node stopped
        if only_node1:
            self._execute_isolated(node_to_update=node1, node_to_stop=node2, queries=only_node1)

        # execute the queries for the second node only with the first node stopped
        if only_node2:
            self._execute_isolated(node_to_update=node2, node_to_stop=node1, queries=only_node2)

        # set the session with CL=ALL for testing queries with the created scenario
        self.session = self.patient_exclusive_cql_connection(node1, keyspace=keyspace, consistency_level=CL.ALL)

    def _execute_isolated(self, node_to_update, node_to_stop, queries):
        node_to_stop.flush()
        node_to_stop.stop()
        session = self.patient_cql_connection(node_to_update, keyspace, consistency_level=CL.ONE)
        for q in queries:
            session.execute(q)
        node_to_stop.start()

    def _assert_none(self, query):
        """
        Assert query returns nothing.
        @param query Query to run
        """
        decorated_query = self._decorate_query(query)
        assert_none(self.session, decorated_query)

    def _assert_one(self, query, row):
        """
        Assert query returns one row.
        @param query Query to run
        @param row Expected result row from query
        """
        decorated_query = self._decorate_query(query)
        assert_one(self.session, decorated_query, row)

    def _assert_all(self, query, rows):
        """
        Assert query returns all expected rows in the correct order.
        @param query Query to run
        @param rows Expected result rows from query
        """
        decorated_query = self._decorate_query(query)
        assert_all(self.session, decorated_query, rows)

    def _decorate_query(self, query):
        return query if self.create_index() else query + " ALLOW FILTERING"

    def _skip_if_index_on_static_is_not_supported(self):
        if self.create_index() and self.cluster.version() < '3.4':
            pytest.skip('Secondary indexes on static column are not supported before 3.4 (CASSANDRA-8103)')

    def _skip_if_filtering_partition_columns_is_not_supported(self):
        if not self.create_index() and self.cluster.version() < '3.11':
            pytest.skip('Filtering of partition key parts is not supported before 3.11 (CASSANDRA-13275)')

    @abstractmethod
    def create_index(self):
        """
        :return:``True`` if the tests should create an index, ``False`` if they should use ``ALLOW FILTERING`` instead
        """
        pass

    def test_update_on_skinny_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int PRIMARY KEY, v text)",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t(k, v) VALUES (0, 'old')"],
            only_node1=["UPDATE t SET v = 'new' WHERE k = 0"])

        self._assert_none("SELECT * FROM t WHERE v = 'old'")
        self._assert_one("SELECT * FROM t WHERE v = 'new'", row=[0, 'new'])

    def test_update_on_wide_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, s int STATIC, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t(k, s) VALUES (0, 9)",
                        "INSERT INTO t(k, c, v) VALUES (0, -1, 'old')",
                        "INSERT INTO t(k, c, v) VALUES (0, 0, 'old')",
                        "INSERT INTO t(k, c, v) VALUES (0, 1, 'old')"],
            only_node1=["UPDATE t SET v = 'new' WHERE k = 0 AND c = 0"])

        self._assert_all("SELECT * FROM t WHERE v = 'old'", rows=[[0, -1, 9, 'old'], [0, 1, 9, 'old']])
        self._assert_all("SELECT * FROM t WHERE v = 'new'", rows=[[0, 0, 9, 'new']])

    def test_update_on_static_column_with_empty_partition(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v int, s text STATIC, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t(k, s) VALUES (0, 'old')",
                        "INSERT INTO t(k, s) VALUES (1, 'old')"],
            only_node1=["UPDATE t SET s = 'new' WHERE k = 0"])

        self._assert_one("SELECT * FROM t WHERE s = 'old'", row=[1, None, 'old', None])
        self._assert_one("SELECT * FROM t WHERE s = 'new'", row=[0, None, 'new', None])

    def test_update_on_static_column_with_not_empty_partition(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v int, s text STATIC, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t(k, s) VALUES (0, 'old')",
                        "INSERT INTO t(k, s) VALUES (1, 'old')",
                        "INSERT INTO t(k, c, v) VALUES (0, 10, 100)",
                        "INSERT INTO t(k, c, v) VALUES (0, 20, 200)",
                        "INSERT INTO t(k, c, v) VALUES (1, 30, 300)",
                        "INSERT INTO t(k, c, v) VALUES (1, 40, 400)"],
            only_node1=["UPDATE t SET s = 'new' WHERE k = 0"])

        self._assert_all("SELECT * FROM t WHERE s = 'old'", rows=[[1, 30, 'old', 300], [1, 40, 'old', 400]])
        self._assert_all("SELECT * FROM t WHERE s = 'new'", rows=[[0, 10, 'new', 100], [0, 20, 'new', 200]])

    def test_update_on_collection(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int PRIMARY KEY, v set<int>)",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t(k, v) VALUES (0, {-1, 0, 1})"],
            only_node1=["UPDATE t SET v = v - {0} WHERE k = 0"])

        self._assert_none("SELECT * FROM t WHERE v CONTAINS 0")
        self._assert_one("SELECT * FROM t WHERE v CONTAINS 1", row=[0, [-1, 1]])

    def test_complementary_deletion_with_limit_on_partition_key_column_with_empty_partitions(self):
        self._skip_if_filtering_partition_columns_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k1 int, k2 int, c int, s int STATIC, PRIMARY KEY ((k1, k2), c))",
            create_index="CREATE INDEX ON t(k1)",
            both_nodes=["INSERT INTO t (k1, k2, s) VALUES (0, 1, 10)",
                        "INSERT INTO t (k1, k2, s) VALUES (0, 2, 20)"],
            only_node1=["DELETE FROM t WHERE k1 = 0 AND k2 = 1"],
            only_node2=["DELETE FROM t WHERE k1 = 0 AND k2 = 2"])

        self._assert_none("SELECT * FROM t WHERE k1 = 0 LIMIT 1")

    def test_complementary_deletion_with_limit_on_partition_key_column_with_not_empty_partitions(self):
        self._skip_if_filtering_partition_columns_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k1 int, k2 int, c int, s int STATIC, PRIMARY KEY ((k1, k2), c))",
            create_index="CREATE INDEX ON t(k1)",
            both_nodes=["INSERT INTO t (k1, k2, c, s) VALUES (0, 1, 10, 100)",
                        "INSERT INTO t (k1, k2, c, s) VALUES (0, 2, 20, 200)"],
            only_node1=["DELETE FROM t WHERE k1 = 0 AND k2 = 1"],
            only_node2=["DELETE FROM t WHERE k1 = 0 AND k2 = 2"])

        self._assert_none("SELECT * FROM t WHERE k1 = 0 LIMIT 1")

    def test_complementary_deletion_with_limit_on_clustering_key_column(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(c)",
            both_nodes=["INSERT INTO t (k, c) VALUES (1, 0)",
                        "INSERT INTO t (k, c) VALUES (2, 0)"],
            only_node1=["DELETE FROM t WHERE k = 1"],
            only_node2=["DELETE FROM t WHERE k = 2"])

        self._assert_none("SELECT * FROM t WHERE c = 0 LIMIT 1")

    def test_complementary_deletion_with_limit_on_static_column_with_empty_partitions(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, s int STATIC, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t (k, s) VALUES (1, 0)",
                        "INSERT INTO t (k, s) VALUES (2, 0)"],
            only_node1=["DELETE FROM t WHERE k = 1"],
            only_node2=["DELETE FROM t WHERE k = 2"])

        self._assert_none("SELECT * FROM t WHERE s = 0 LIMIT 1")

    def test_complementary_deletion_with_limit_on_static_column_with_empty_partitions_and_rows_after(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, s int STATIC, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t (k, s) VALUES (1, 0)",
                        "INSERT INTO t (k, s) VALUES (2, 0)",
                        "INSERT INTO t (k, s) VALUES (3, 0)",
                        "INSERT INTO t (k, c) VALUES (3, 1)",
                        "INSERT INTO t (k, c) VALUES (3, 2)"],
            only_node1=["DELETE FROM t WHERE k = 1"],
            only_node2=["DELETE FROM t WHERE k = 2"])

        self._assert_one("SELECT * FROM t WHERE s = 0 LIMIT 1", row=[3, 1, 0])
        self._assert_all("SELECT * FROM t WHERE s = 0 LIMIT 10", rows=[[3, 1, 0], [3, 2, 0]])
        self._assert_all("SELECT * FROM t WHERE s = 0", rows=[[3, 1, 0], [3, 2, 0]])

    def test_complementary_deletion_with_limit_on_static_column_with_not_empty_partitions(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, s int STATIC, v int, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t (k, c, v, s) VALUES (1, 10, 100, 0)",
                        "INSERT INTO t (k, c, v, s) VALUES (2, 20, 200, 0)"],
            only_node1=["DELETE FROM t WHERE k = 1"],
            only_node2=["DELETE FROM t WHERE k = 2"])

        self._assert_none("SELECT * FROM t WHERE s = 0 LIMIT 1")

    def test_complementary_deletion_with_limit_on_static_column_with_not_empty_partitions_and_rows_after(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, s int STATIC, v int, PRIMARY KEY(k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t (k, c, v, s) VALUES (1, 10, 100, 0)",
                        "INSERT INTO t (k, c, v, s) VALUES (2, 20, 200, 0)",
                        "INSERT INTO t (k, s) VALUES (3, 0)",
                        "INSERT INTO t (k, c) VALUES (3, 1)",
                        "INSERT INTO t (k, c) VALUES (3, 2)"],
            only_node1=["DELETE FROM t WHERE k = 1"],
            only_node2=["DELETE FROM t WHERE k = 2"])

        self._assert_one("SELECT * FROM t WHERE s = 0 LIMIT 1", row=[3, 1, 0, None])
        self._assert_all("SELECT * FROM t WHERE s = 0 LIMIT 10", rows=[[3, 1, 0, None], [3, 2, 0, None]])
        self._assert_all("SELECT * FROM t WHERE s = 0", rows=[[3, 1, 0, None], [3, 2, 0, None]])

    def test_complementary_deletion_with_limit_on_regular_column(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v int, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t (k, c, v) VALUES (0, 1, 0)",
                        "INSERT INTO t (k, c, v) VALUES (0, 2, 0)"],
            only_node1=["DELETE FROM t WHERE k = 0 AND c = 1"],
            only_node2=["DELETE FROM t WHERE k = 0 AND c = 2"])

        self._assert_none("SELECT * FROM t WHERE v = 0 LIMIT 1")

    def test_complementary_deletion_with_limit_and_rows_after(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v int, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t (k, c, v) VALUES (0, 1, 0)",
                        "INSERT INTO t (k, c, v) VALUES (0, 2, 0)",
                        "INSERT INTO t (k, c, v) VALUES (0, 3, 0)"],
            only_node1=["DELETE FROM t WHERE k = 0 AND c = 1",
                        "INSERT INTO t (k, c, v) VALUES (0, 4, 0)"],
            only_node2=["INSERT INTO t (k, c, v) VALUES (0, 5, 0)",
                        "DELETE FROM t WHERE k = 0 AND c = 2"])

        self._assert_one("SELECT * FROM t WHERE v = 0 LIMIT 1", row=[0, 3, 0])
        self._assert_all("SELECT * FROM t WHERE v = 0 LIMIT 2", rows=[[0, 3, 0], [0, 4, 0]])
        self._assert_all("SELECT * FROM t WHERE v = 0 LIMIT 3", rows=[[0, 3, 0], [0, 4, 0], [0, 5, 0]])
        self._assert_all("SELECT * FROM t WHERE v = 0 LIMIT 4", rows=[[0, 3, 0], [0, 4, 0], [0, 5, 0]])

    def test_complementary_deletion_with_limit_and_rows_between(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v int, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t (k, c, v) VALUES (0, 1, 0)",
                        "INSERT INTO t (k, c, v) VALUES (0, 4, 0)"],
            only_node1=["DELETE FROM t WHERE k = 0 AND c = 1"],
            only_node2=["INSERT INTO t (k, c, v) VALUES (0, 2, 0)",
                        "INSERT INTO t (k, c, v) VALUES (0, 3, 0)",
                        "DELETE FROM t WHERE k = 0 AND c = 4"])

        self._assert_one("SELECT * FROM t WHERE v = 0 LIMIT 1", row=[0, 2, 0])
        self._assert_all("SELECT * FROM t WHERE v = 0 LIMIT 2", rows=[[0, 2, 0], [0, 3, 0]])
        self._assert_all("SELECT * FROM t WHERE v = 0 LIMIT 3", rows=[[0, 2, 0], [0, 3, 0]])

    def test_complementary_update_with_limit_on_static_column_with_empty_partitions(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, s text STATIC, v int, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t (k, s) VALUES (1, 'old')",
                        "INSERT INTO t (k, s) VALUES (2, 'old')"],
            only_node1=["UPDATE t SET s = 'new' WHERE k = 1"],
            only_node2=["UPDATE t SET s = 'new' WHERE k = 2"])

        self._assert_none("SELECT * FROM t WHERE s = 'old' LIMIT 1")
        self._assert_one("SELECT k, c, v, s FROM t WHERE s = 'new' LIMIT 1", row=[1, None, None, 'new'])
        self._assert_all("SELECT k, c, v, s FROM t WHERE s = 'new'",
                         rows=[[1, None, None, 'new'], [2, None, None, 'new']])

    def test_complementary_update_with_limit_on_static_column_with_not_empty_partitions(self):
        self._skip_if_index_on_static_is_not_supported()
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, s text STATIC, v int, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(s)",
            both_nodes=["INSERT INTO t (k, c, v, s) VALUES (1, 10, 100, 'old')",
                        "INSERT INTO t (k, c, v, s) VALUES (2, 20, 200, 'old')"],
            only_node1=["UPDATE t SET s = 'new' WHERE k = 1"],
            only_node2=["UPDATE t SET s = 'new' WHERE k = 2"])

        self._assert_none("SELECT * FROM t WHERE s = 'old' LIMIT 1")
        self._assert_one("SELECT k, c, v, s FROM t WHERE s = 'new' LIMIT 1", row=[1, 10, 100, 'new'])
        self._assert_all("SELECT k, c, v, s FROM t WHERE s = 'new'", rows=[[1, 10, 100, 'new'], [2, 20, 200, 'new']])

    def test_complementary_update_with_limit_on_regular_column(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t (k, c, v) VALUES (0, 1, 'old')",
                        "INSERT INTO t (k, c, v) VALUES (0, 2, 'old')"],
            only_node1=["UPDATE t SET v = 'new' WHERE k = 0 AND c = 1"],
            only_node2=["UPDATE t SET v = 'new' WHERE k = 0 AND c = 2"])

        self._assert_none("SELECT * FROM t WHERE v = 'old' LIMIT 1")
        self._assert_one("SELECT * FROM t WHERE v = 'new' LIMIT 1", row=[0, 1, 'new'])
        self._assert_all("SELECT * FROM t WHERE v = 'new'", rows=[[0, 1, 'new'], [0, 2, 'new']])

    def test_complementary_update_with_limit_and_rows_between(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t (k, c, v) VALUES (0, 1, 'old')",
                        "INSERT INTO t (k, c, v) VALUES (0, 4, 'old')"],
            only_node1=["UPDATE t SET v = 'new' WHERE k = 0 AND c = 1"],
            only_node2=["INSERT INTO t (k, c, v) VALUES (0, 2, 'old')",
                        "INSERT INTO t (k, c, v) VALUES (0, 3, 'old')",
                        "UPDATE t SET v = 'new' WHERE k = 0 AND c = 4"])

        self._assert_one("SELECT * FROM t WHERE v = 'old' LIMIT 1", row=[0, 2, 'old'])
        self._assert_all("SELECT * FROM t WHERE v = 'old' LIMIT 2", rows=[[0, 2, 'old'], [0, 3, 'old']])
        self._assert_all("SELECT * FROM t WHERE v = 'old' LIMIT 3", rows=[[0, 2, 'old'], [0, 3, 'old']])
        self._assert_one("SELECT * FROM t WHERE v = 'new' LIMIT 1", row=[0, 1, 'new'])
        self._assert_all("SELECT * FROM t WHERE v = 'new'", rows=[[0, 1, 'new'], [0, 4, 'new']])

    def test_partition_deletion_on_skinny_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int PRIMARY KEY, v text)",
            create_index="CREATE INDEX ON t(v)",
            only_node1=["INSERT INTO t (k, v) VALUES (0, 'old') USING TIMESTAMP 1"],
            only_node2=["DELETE FROM t WHERE k = 0"])

        self._assert_none("SELECT * FROM t WHERE v = 'old' LIMIT 1")
        self._assert_none("SELECT * FROM t WHERE v = 'old'")

    def test_partition_deletion_on_wide_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            only_node1=["INSERT INTO t (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1"],
            only_node2=["DELETE FROM t WHERE k = 0"])

        self._assert_none("SELECT * FROM t WHERE v = 'old' LIMIT 1")
        self._assert_none("SELECT * FROM t WHERE v = 'old'")

    def test_row_deletion_on_wide_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            only_node1=["INSERT INTO t (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1"],
            only_node2=["DELETE FROM t WHERE k = 0 AND c = 1"])

        self._assert_none("SELECT * FROM t WHERE v = 'old' LIMIT 1")
        self._assert_none("SELECT * FROM t WHERE v = 'old'")

    def test_range_deletion_on_wide_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            only_node1=["INSERT INTO t (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1",
                        "INSERT INTO t (k, c, v) VALUES (0, 2, 'old') USING TIMESTAMP 1",
                        "INSERT INTO t (k, c, v) VALUES (0, 3, 'old') USING TIMESTAMP 1",
                        "INSERT INTO t (k, c, v) VALUES (0, 4, 'old') USING TIMESTAMP 1"],
            only_node2=["DELETE FROM t WHERE k = 0 AND c > 1 AND c < 4"])

        self._assert_one("SELECT * FROM t WHERE v = 'old' LIMIT 1", row=[0, 1, 'old'])
        self._assert_all("SELECT * FROM t WHERE v = 'old'", rows=[[0, 1, 'old'], [0, 4, 'old']])

    def test_mismatching_insertions_on_skinny_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int PRIMARY KEY, v text)",
            create_index="CREATE INDEX ON t(v)",
            only_node1=["INSERT INTO t (k, v) VALUES (0, 'old') USING TIMESTAMP 1"],
            only_node2=["INSERT INTO t (k, v) VALUES (0, 'new') USING TIMESTAMP 2"])

        self._assert_none("SELECT * FROM t WHERE v = 'old' LIMIT 1")
        self._assert_none("SELECT * FROM t WHERE v = 'old'")
        self._assert_one("SELECT * FROM t WHERE v = 'new'", row=[0, 'new'])

    def test_mismatching_insertions_on_wide_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            only_node1=["INSERT INTO t (k, c, v) VALUES (0, 1, 'old') USING TIMESTAMP 1"],
            only_node2=["INSERT INTO t (k, c, v) VALUES (0, 1, 'new') USING TIMESTAMP 2"])

        self._assert_none("SELECT * FROM t WHERE v = 'old' LIMIT 1")
        self._assert_none("SELECT * FROM t WHERE v = 'old'")
        self._assert_one("SELECT * FROM t WHERE v = 'new'", row=[0, 1, 'new'])

    def test_consistent_skinny_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int PRIMARY KEY, v text)",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t(k, v) VALUES (1, 'old')",  # updated to 'new'
                        "INSERT INTO t(k, v) VALUES (2, 'old')",
                        "INSERT INTO t(k, v) VALUES (3, 'old')",  # updated to 'new'
                        "INSERT INTO t(k, v) VALUES (4, 'old')",
                        "INSERT INTO t(k, v) VALUES (5, 'old')",  # deleted partition
                        "UPDATE t SET v = 'new' WHERE k = 1",
                        "UPDATE t SET v = 'new' WHERE k = 3",
                        "DELETE FROM t WHERE k = 5"])

        self._assert_one("SELECT * FROM t WHERE v = 'old' LIMIT 1", row=[2, 'old'])
        self._assert_one("SELECT * FROM t WHERE v = 'new' LIMIT 1", row=[1, 'new'])
        self._assert_all("SELECT * FROM t WHERE v = 'old'", rows=[[2, 'old'], [4, 'old']])
        self._assert_all("SELECT * FROM t WHERE v = 'new'", rows=[[1, 'new'], [3, 'new']])

    def test_consistent_wide_table(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int, c int, v text, PRIMARY KEY (k, c))",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t(k, c, v) VALUES (0, 1, 'old')",  # updated to 'new'
                        "INSERT INTO t(k, c, v) VALUES (0, 2, 'old')",
                        "INSERT INTO t(k, c, v) VALUES (0, 3, 'old')",  # updated to 'new'
                        "INSERT INTO t(k, c, v) VALUES (0, 4, 'old')",
                        "INSERT INTO t(k, c, v) VALUES (0, 5, 'old')",  # deleted row
                        "INSERT INTO t(k, c, v) VALUES (1, 1, 'old')",  # deleted partition
                        "INSERT INTO t(k, c, v) VALUES (1, 2, 'old')",  # deleted partition
                        "UPDATE t SET v = 'new' WHERE k = 0 AND c = 1",
                        "UPDATE t SET v = 'new' WHERE k = 0 AND c = 3",
                        "DELETE FROM t WHERE k = 0 AND c = 5",
                        "DELETE FROM t WHERE k = 1"])

        self._assert_one("SELECT * FROM t WHERE v = 'old' LIMIT 1", row=[0, 2, 'old'])
        self._assert_one("SELECT * FROM t WHERE v = 'new' LIMIT 1", row=[0, 1, 'new'])
        self._assert_all("SELECT * FROM t WHERE v = 'old'", rows=[[0, 2, 'old'], [0, 4, 'old']])
        self._assert_all("SELECT * FROM t WHERE v = 'new'", rows=[[0, 1, 'new'], [0, 3, 'new']])

    def test_count(self):
        self._prepare_cluster(
            create_table="CREATE TABLE t (k int PRIMARY KEY, v text)",
            create_index="CREATE INDEX ON t(v)",
            both_nodes=["INSERT INTO t(k, v) VALUES (1, 'old')",
                        "INSERT INTO t(k, v) VALUES (2, 'old')",
                        "INSERT INTO t(k, v) VALUES (3, 'old')",
                        "INSERT INTO t(k, v) VALUES (4, 'old')",
                        "INSERT INTO t(k, v) VALUES (5, 'old')"],
            only_node1=["UPDATE t SET v = 'new' WHERE k = 2",
                        "UPDATE t SET v = 'new' WHERE k = 4"])

        self._assert_one("SELECT COUNT(*) FROM t WHERE v = 'old' LIMIT 1", row=[3])
        self._assert_one("SELECT COUNT(*) FROM t WHERE v = 'old'", row=[3])
        self._assert_one("SELECT COUNT(*) FROM t WHERE v = 'new'", row=[2])


@since('3.0.21')
class TestSecondaryIndexes(ReplicaSideFiltering):
    """
    @jira_ticket CASSANDRA-8272
    Tests the consistency of secondary indexes queries when some of the replicas have stale data.
    """
    __test__ = True

    def create_index(self):
        return True


@since('3.0.21')
class TestAllowFiltering(ReplicaSideFiltering):
    """
    @jira_ticket CASSANDRA-8273
    Test the consistency of queries using ``ALLOW FILTERING`` when some of the replicas have stale data.
    """
    __test__ = True

    def create_index(self):
        return False

    def _test_missed_update_with_transient_replicas(self, missed_by_transient):
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False,
                                                  'num_tokens': 1,
                                                  'commitlog_sync_period_in_ms': 500,
                                                  'enable_transient_replication': True,
                                                  'partitioner': 'org.apache.cassandra.dht.OrderPreservingPartitioner'})
        cluster.set_batch_commitlog(enabled=True)
        cluster.populate(2, tokens=[0, 1], debug=True, install_byteman=True)
        node1, node2 = cluster.nodelist()
        cluster.start()

        self.session = self.patient_exclusive_cql_connection(node1, consistency_level=CL.ALL)
        self.session.execute("CREATE KEYSPACE %s WITH replication = "
                             "{'class': 'SimpleStrategy', 'replication_factor': '2/1'}" % (keyspace))
        self.session.execute("USE " + keyspace)
        self.session.execute("CREATE TABLE t (k int PRIMARY KEY, v text)"
                             " WITH speculative_retry = 'NEVER'"
                             " AND additional_write_policy = 'NEVER'"
                             " AND read_repair = 'NONE'")

        # insert in both nodes with CL=ALL
        self.session.execute("INSERT INTO t(k, v) VALUES (0, 'old')")

        # update the previous value with CL=ONE only in one replica
        node = cluster.nodelist()[1 if missed_by_transient else 0]
        node.byteman_submit([mk_bman_path('stop_writes.btm')])
        self.session.execute(SimpleStatement("UPDATE t SET v = 'new' WHERE k = 0", consistency_level=CL.ONE))

        # query with CL=ALL to verify that no old values are resurrected
        self._assert_none("SELECT * FROM t WHERE v = 'old'")
        self._assert_one("SELECT * FROM t WHERE v = 'new'", row=[0, 'new'])

    @since('4.0')
    def test_update_missed_by_transient_replica(self):
        self._test_missed_update_with_transient_replicas(missed_by_transient=True)

    @since('4.0')
    def test_update_only_on_transient_replica(self):
        self._test_missed_update_with_transient_replicas(missed_by_transient=False)
