import time
import uuid
import pytest
import logging

from flaky import flaky

from distutils.version import LooseVersion

from cassandra import ConsistencyLevel as CL
from cassandra import InvalidRequest, ReadFailure, ReadTimeout
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import (SimpleStatement, dict_factory,
                             named_tuple_factory, tuple_factory)

from dtest import Tester, run_scenarios, create_ks
from tools.assertions import (assert_all, assert_invalid, assert_length_equal,
                              assert_one, assert_lists_equal_ignoring_order)
from tools.data import rows_to_list
from tools.datahelp import create_rows, flatten_into_set, parse_data_into_dicts
from tools.paging import PageAssertionMixin, PageFetcher

since = pytest.mark.since
logger = logging.getLogger(__name__)


class BasePagingTester(Tester):

    def prepare(self, row_factory=dict_factory):
        supports_v5 = self.supports_v5_protocol(self.cluster.version())
        protocol_version = 5 if supports_v5 else None
        cluster = self.cluster
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1,
                                              protocol_version=protocol_version,
                                              consistency_level=CL.QUORUM,
                                              row_factory=row_factory)
        return session


@since('2.0')
class TestPagingSize(BasePagingTester, PageAssertionMixin):
    """
    Basic tests relating to page size (relative to results set)
    and validation of page size setting.
    """

    def test_with_no_results(self):
        """
        No errors when a page is requested and query has no results.
        """
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        # run a query that has no results and make sure it's exhausted
        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        pf.request_all()
        assert [] == pf.all_data()
        assert not pf.has_more_pages

    def test_with_less_results_than_page_size(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            +--+----------------+
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future)
        pf.request_all()

        assert not pf.has_more_pages
        assert len(expected_data) == len(pf.all_data())

    def test_with_more_results_than_page_size(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            +--+----------------+
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |6 |testing         |
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [5, 4]

        # make sure expected and actual have same data elements (ignoring order)
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="id")

    def test_with_equal_results_to_page_size(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            +--+----------------+
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.num_results_all() == [5]
        assert pf.pagecount() == 1

        # make sure expected and actual have same data elements (ignoring order)
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="id")

    def test_undefined_page_size_default(self):
        """
        If the page size isn't sent then the default fetch size is used.
        """
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        def random_txt(text):
            return uuid.uuid4()

        data = """
               | id     |value   |
               +--------+--------+
          *5001| [uuid] |testing |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': random_txt, 'value': str})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.num_results_all(), [5000, 1]

        # make sure expected and actual have same data elements (ignoring order)
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="id")


@since('2.0')
class TestPagingWithModifiers(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when CQL modifiers (such as order, limit, allow filtering) are used.
    """

    def test_with_order_by(self):
        """"
        Paging over a single partition with ordering should work.
        (Spanning multiple partitions won't though, by design. See CASSANDRA-6722).
        """
        session = self.prepare()
        create_ks(session, 'test_paging', 2)
        session.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value ASC)
            """)

        data = """
            |id|value|
            +--+-----+
            |1 |a    |
            |1 |b    |
            |1 |c    |
            |1 |d    |
            |1 |e    |
            |1 |f    |
            |1 |g    |
            |1 |h    |
            |1 |i    |
            |1 |j    |
            """

        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id = 1 order by value asc", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [5, 5]

        # these should be equal (in the same order)
        assert pf.all_data() == expected_data

        # make sure we don't allow paging over multiple partitions with order because that's weird
        with pytest.raises(InvalidRequest, match='Cannot page queries with both ORDER BY and a IN restriction on the partition key'):
            stmt = SimpleStatement("select * from paging_test where id in (1,2) order by value asc", consistency_level=CL.ALL)
            session.execute(stmt)

    def test_with_order_by_reversed(self):
        """"
        Paging over a single partition with ordering and a reversed clustering order.
        """
        session = self.prepare()
        create_ks(session, 'test_paging', 2)
        session.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                value2 text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value DESC)
            """)

        data = """
            |id|value|value2|
            +--+-----+------+
            |1 |a    |a     |
            |1 |b    |b     |
            |1 |c    |c     |
            |1 |d    |d     |
            |1 |e    |e     |
            |1 |f    |f     |
            |1 |g    |g     |
            |1 |h    |h     |
            |1 |i    |i     |
            |1 |j    |j     |
            """

        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str, 'value2': str})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id = 1 order by value asc", fetch_size=3, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all(), [3, 3, 3, 1]

        # these should be equal (in the same order)
        assert pf.all_data() == expected_data

        # drop the ORDER BY
        future = session.execute_async(
            SimpleStatement("select * from paging_test where id = 1", fetch_size=3, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all(), [3, 3, 3, 1]

        # these should be equal (in the same order)
        assert pf.all_data() == list(reversed(expected_data))

    def test_with_limit(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
               | id | value         |
               +----+---------------+
             *5| 1  | [random text] |
             *5| 2  | [random text] |
            *10| 3  | [random text] |
            *10| 4  | [random text] |
            *20| 5  | [random text] |
            *30| 6  | [random text] |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': random_txt})

        scenarios = [
            # using equals clause w/single partition
            {'limit': 10, 'fetch': 20, 'data_size': 30, 'whereclause': 'WHERE id = 6', 'expect_pgcount': 1, 'expect_pgsizes': [10]},      # limit < fetch < data
            {'limit': 10, 'fetch': 30, 'data_size': 20, 'whereclause': 'WHERE id = 5', 'expect_pgcount': 1, 'expect_pgsizes': [10]},      # limit < data < fetch
            {'limit': 20, 'fetch': 10, 'data_size': 30, 'whereclause': 'WHERE id = 6', 'expect_pgcount': 2, 'expect_pgsizes': [10, 10]},  # fetch < limit < data
            {'limit': 30, 'fetch': 10, 'data_size': 20, 'whereclause': 'WHERE id = 5', 'expect_pgcount': 2, 'expect_pgsizes': [10, 10]},  # fetch < data < limit
            {'limit': 20, 'fetch': 30, 'data_size': 10, 'whereclause': 'WHERE id = 3', 'expect_pgcount': 1, 'expect_pgsizes': [10]},      # data < limit < fetch
            {'limit': 30, 'fetch': 20, 'data_size': 10, 'whereclause': 'WHERE id = 3', 'expect_pgcount': 1, 'expect_pgsizes': [10]},      # data < fetch < limit

            # using 'in' clause w/multi partitions
            {'limit': 9, 'fetch': 20, 'data_size': 80, 'whereclause': 'WHERE id in (1,2,3,4,5,6)', 'expect_pgcount': 1, 'expect_pgsizes': [9]},  # limit < fetch < data
            {'limit': 10, 'fetch': 30, 'data_size': 20, 'whereclause': 'WHERE id in (3,4)', 'expect_pgcount': 1, 'expect_pgsizes': [10]},      # limit < data < fetch
            {'limit': 20, 'fetch': 10, 'data_size': 30, 'whereclause': 'WHERE id in (4,5)', 'expect_pgcount': 2, 'expect_pgsizes': [10, 10]},  # fetch < limit < data
            {'limit': 30, 'fetch': 10, 'data_size': 20, 'whereclause': 'WHERE id in (3,4)', 'expect_pgcount': 2, 'expect_pgsizes': [10, 10]},  # fetch < data < limit
            {'limit': 20, 'fetch': 30, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1, 'expect_pgsizes': [10]},      # data < limit < fetch
            {'limit': 30, 'fetch': 20, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1, 'expect_pgsizes': [10]},      # data < fetch < limit

            # no limit but with a defined pagesize. Scenarios added for CASSANDRA-8408.
            {'limit': None, 'fetch': 20, 'data_size': 80, 'whereclause': 'WHERE id in (1,2,3,4,5,6)', 'expect_pgcount': 4, 'expect_pgsizes': [20, 20, 20, 20]},  # fetch < data
            {'limit': None, 'fetch': 30, 'data_size': 20, 'whereclause': 'WHERE id in (3,4)', 'expect_pgcount': 1, 'expect_pgsizes': [20]},          # data < fetch
            {'limit': None, 'fetch': 10, 'data_size': 30, 'whereclause': 'WHERE id in (4,5)', 'expect_pgcount': 3, 'expect_pgsizes': [10, 10, 10]},  # fetch < data
            {'limit': None, 'fetch': 30, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1, 'expect_pgsizes': [10]},          # data < fetch

            # not setting fetch_size (unpaged) but using limit. Scenarios added for CASSANDRA-8408.
            {'limit': 9, 'fetch': None, 'data_size': 80, 'whereclause': 'WHERE id in (1,2,3,4,5,6)', 'expect_pgcount': 1, 'expect_pgsizes': [9]},  # limit < data
            {'limit': 30, 'fetch': None, 'data_size': 10, 'whereclause': 'WHERE id in (1,2)', 'expect_pgcount': 1, 'expect_pgsizes': [10]},        # data < limit
        ]

        def handle_scenario(scenario):
            # using a limit and a fetch
            if scenario['limit'] and scenario['fetch']:
                future = session.execute_async(
                    SimpleStatement(
                        "select * from paging_test {} limit {}".format(scenario['whereclause'], scenario['limit']),
                        fetch_size=scenario['fetch'], consistency_level=CL.ALL)
                )
            # using a limit but not specifying a fetch_size
            elif scenario['limit'] and scenario['fetch'] is None:
                future = session.execute_async(
                    SimpleStatement(
                        "select * from paging_test {} limit {}".format(scenario['whereclause'], scenario['limit']),
                        consistency_level=CL.ALL)
                )
            # no limit but a fetch_size specified
            elif scenario['limit'] is None and scenario['fetch']:
                future = session.execute_async(
                    SimpleStatement(
                        "select * from paging_test {}".format(scenario['whereclause']),
                        fetch_size=scenario['fetch'], consistency_level=CL.ALL)
                )
            else:
                # this should not happen
                pytest.fail("Invalid scenario configuration. Scenario is: {}".format(scenario))

            pf = PageFetcher(future).request_all()
            assert pf.num_results_all() == scenario['expect_pgsizes']
            assert pf.pagecount() == scenario['expect_pgcount']

            # make sure all the data retrieved is a subset of input data
            self.assertIsSubsetOf(pf.all_data(), expected_data)

        run_scenarios(scenarios, handle_scenario, deferred_exceptions=(AssertionError,))

    def test_with_allow_filtering(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        data = """
            |id|value           |
            +--+----------------+
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |6 |testing         |
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """
        create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': str})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where value = 'and more testing' ALLOW FILTERING", fetch_size=4, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [4, 3]

        # make sure the allow filtering query matches the expected results (ignoring order)
        expected_data = parse_data_into_dicts(
            """
            |id|value           |
            +--+----------------+
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            |7 |and more testing|
            |8 |and more testing|
            |9 |and more testing|
            """, format_funcs={'id': int, 'value': str}
        )
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="value")


@since('2.0')
class TestPagingData(BasePagingTester, PageAssertionMixin):

    def test_paging_a_single_wide_row(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
              | id | value                  |
              +----+------------------------+
        *10000| 1  | [replaced with random] |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': random_txt})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id = 1", fetch_size=3000, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all(), [3000, 3000, 3000, 1000]
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="value")

    def test_paging_across_multi_wide_rows(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
              | id | value                  |
              +----+------------------------+
         *5000| 1  | [replaced with random] |
         *5000| 2  | [replaced with random] |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': random_txt})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=3000, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        assert pf.pagecount() == 4
        assert pf.num_results_all(), [3000, 3000, 3000, 1000]
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="value")

    def test_paging_using_secondary_indexes(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mybool boolean, sometext text, PRIMARY KEY (id, sometext) )")
        session.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(text):
            return str(uuid.uuid4())

        def bool_from_str_int(text):
            return bool(int(text))

        data = """
             | id | mybool| sometext |
             +----+-------+----------+
         *100| 1  | 1     | [random] |
         *300| 2  | 0     | [random] |
         *500| 3  | 1     | [random] |
         *400| 4  | 0     | [random] |
            """
        all_data = create_rows(
            data, session, 'paging_test', cl=CL.ALL,
            format_funcs={'id': int, 'mybool': bool_from_str_int, 'sometext': random_txt}
        )

        future = session.execute_async(
            SimpleStatement("select * from paging_test where mybool = true", fetch_size=400, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        # the query only searched for True rows, so let's pare down the expectations for comparison
        expected_data = [x for x in all_data if x.get('mybool') is True]

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [400, 200]
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="sometext")

    def test_paging_with_in_orderby_and_two_partition_keys(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test (col_1 int, col_2 int, col_3 int, PRIMARY KEY ((col_1, col_2), col_3))")

        assert_invalid(session, "select * from paging_test where col_1=1 and col_2 IN (1, 2) order by col_3 desc;", expected=InvalidRequest)
        assert_invalid(session, "select * from paging_test where col_2 IN (1, 2) and col_1=1 order by col_3 desc;", expected=InvalidRequest)

    @since('3.10')
    def test_group_by_paging(self):
        """
        @jira_ticket CASSANDRA-10707
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_group_by', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, d int, e int, primary key (a, b, c, d))")

        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 2, 1, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 2, 2, 6, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 3, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 4, 2, 12, 24)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (1, 4, 2, 6, 12)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (2, 2, 3, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (2, 4, 3, 6, 12)")
        session.execute("INSERT INTO test (a, b, c, d, e) VALUES (4, 8, 2, 12, 24)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (5, 8, 2, 12)")

        # Makes sure that we have some tombstones
        session.execute("DELETE FROM test WHERE a = 1 AND b = 3 AND c = 2")
        session.execute("DELETE FROM test WHERE a = 5")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a"))
            assert res == [[1, 2, 6, 4, 24], [2, 2, 6, 2, 12], [4, 8, 24, 1, 24]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b"))
            assert res == [[1, 2, 6, 2, 12],
                           [1, 4, 12, 2, 24],
                           [2, 2, 6, 1, 6],
                           [2, 4, 12, 1, 12],
                           [4, 8, 24, 1, 24]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test"))
            assert res == [[1, 2, 6, 7, 24]]

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE b = 2 GROUP BY a, b ALLOW FILTERING"))
            assert res == [[1, 2, 6, 2, 12],
                           [2, 2, 6, 1, 6]]

            assert_invalid(session, "SELECT a, b, e, count(b), max(e) FROM test WHERE b = 2 GROUP BY a, b;", expected=InvalidRequest)

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE b = 2 ALLOW FILTERING"))
            assert res == [[1, 2, 6, 3, 12]]

            assert_invalid(session, "SELECT a, b, e, count(b), max(e) FROM test WHERE b = 2", expected=InvalidRequest)

            # Range queries without aggregates
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test GROUP BY a, b, c"))
            assert res == [[1, 2, 1, 3],
                           [1, 2, 2, 6],
                           [1, 4, 2, 6],
                           [2, 2, 3, 3],
                           [2, 4, 3, 6],
                           [4, 8, 2, 12]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test GROUP BY a, b"))
            assert res == [[1, 2, 1, 3],
                           [1, 4, 2, 6],
                           [2, 2, 3, 3],
                           [2, 4, 3, 6],
                           [4, 8, 2, 12]]

            # Range query with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b LIMIT 2"))
            assert res == [[1, 2, 6, 2, 12],
                           [1, 4, 12, 2, 24]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test LIMIT 2"))
            assert res == [[1, 2, 6, 7, 24]]

            # Range queries without aggregates and with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test GROUP BY a, b, c LIMIT 3"))
            assert res == [[1, 2, 1, 3],
                           [1, 2, 2, 6],
                           [1, 4, 2, 6]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test GROUP BY a, b LIMIT 3"))
            assert res == [[1, 2, 1, 3],
                           [1, 4, 2, 6],
                           [2, 2, 3, 3]]

            # Range query with PER PARTITION LIMIT
            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2"))
            assert res == [[1, 2, 6, 2, 12],
                           [1, 4, 12, 2, 24],
                           [2, 2, 6, 1, 6],
                           [2, 4, 12, 1, 12],
                           [4, 8, 24, 1, 24]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 1"))
            assert res == [[1, 2, 6, 2, 12],
                           [2, 2, 6, 1, 6],
                           [4, 8, 24, 1, 24]]

            # Range queries with PER PARTITION LIMIT and LIMIT
            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 3"))
            assert res == [[1, 2, 6, 2, 12],
                           [1, 4, 12, 2, 24],
                           [2, 2, 6, 1, 6]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5"))
            assert res == [[1, 2, 6, 2, 12],
                           [1, 4, 12, 2, 24],
                           [2, 2, 6, 1, 6],
                           [2, 4, 12, 1, 12],
                           [4, 8, 24, 1, 24]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 10"))
            assert res == [[1, 2, 6, 2, 12],
                           [1, 4, 12, 2, 24],
                           [2, 2, 6, 1, 6],
                           [2, 4, 12, 1, 12],
                           [4, 8, 24, 1, 24]]

            # Range queries without aggregates and with PER PARTITION LIMIT
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test GROUP BY a, b, c PER PARTITION LIMIT 2"))
            assert res == [[1, 2, 1, 3],
                           [1, 2, 2, 6],
                           [2, 2, 3, 3],
                           [2, 4, 3, 6],
                           [4, 8, 2, 12]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test GROUP BY a, b PER PARTITION LIMIT 1"))
            assert res == [[1, 2, 1, 3],
                           [2, 2, 3, 3],
                           [4, 8, 2, 12]]

            # Range query with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test GROUP BY a"))
            assert res == [[1, 1],
                           [2, 1],
                           [4, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test"))
            assert res == [[1, 3]]

            # Range query with DISTINCT and LIMIT
            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test GROUP BY a LIMIT 2"))
            assert res == [[1, 1],
                           [2, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test LIMIT 2"))
            assert res == [[1, 3]]

            # Single partition queries
            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12],
                           [1, 4, 12, 2, 24]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1"))
            assert res == [[1, 2, 6, 4, 24]]

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 AND b = 2 GROUP BY a, b, c"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 AND b = 2"))
            assert res == [[1, 2, 6, 2, 12]]

            # Single partition queries without aggregates
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b"))
            assert res == [[1, 2, 1, 3],
                           [1, 4, 2, 6]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b, c"))
            assert res == [[1, 2, 1, 3],
                           [1, 2, 2, 6],
                           [1, 4, 2, 6]]

            # Single partition query with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a = 1 GROUP BY a"))
            assert res == [[1, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a = 1 GROUP BY a"))
            assert res == [[1, 1]]

            # Single partition queries with LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 10"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12],
                           [1, 4, 12, 2, 24]]

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 2"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 LIMIT 2"))
            assert res == [[1, 2, 6, 4, 24]]

            res = rows_to_list(
                session.execute("SELECT count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 1"))
            assert res == [[1, 6]]

            # Single partition queries with PER PARTITION LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 2"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12]]

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 3"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12],
                           [1, 4, 12, 2, 24]]

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c PER PARTITION LIMIT 3"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12],
                           [1, 4, 12, 2, 24]]

            # Single partition queries without aggregates and with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b LIMIT 2"))
            assert res == [[1, 2, 1, 3],
                           [1, 4, 2, 6]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b LIMIT 1"))
            assert res == [[1, 2, 1, 3]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a = 1 GROUP BY a, b, c LIMIT 2"))
            assert res == [[1, 2, 1, 3],
                           [1, 2, 2, 6]]

            # Single partition queries with ORDER BY
            res = rows_to_list(session.execute(
                "SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC"))
            assert res == [[1, 4, 24, 2, 24],
                           [1, 2, 12, 1, 12],
                           [1, 2, 6, 1, 6]]

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 ORDER BY b DESC, c DESC"))
            assert res == [[1, 4, 24, 4, 24]]

            # Single partition queries with ORDER BY and LIMIT
            res = rows_to_list(session.execute(
                "SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 GROUP BY a, b, c ORDER BY b DESC, c DESC LIMIT 2"))
            assert res == [[1, 4, 24, 2, 24],
                                   [1, 2, 12, 1, 12]]

            res = rows_to_list(session.execute(
                "SELECT a, b, e, count(b), max(e) FROM test WHERE a = 1 ORDER BY b DESC, c DESC LIMIT 2"))
            assert res == [[1, 4, 24, 4, 24]]

            # Multi-partitions queries
            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4) GROUP BY a, b, c"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12],
                           [1, 4, 12, 2, 24],
                           [2, 2, 6, 1, 6],
                           [2, 4, 12, 1, 12],
                           [4, 8, 24, 1, 24]]

            res = rows_to_list(session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4)"))
            assert res == [[1, 2, 6, 7, 24]]

            res = rows_to_list(session.execute(
                "SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4) AND b = 2 GROUP BY a, b, c"))
            assert res == [[1, 2, 6, 1, 6],
                           [1, 2, 12, 1, 12],
                           [2, 2, 6, 1, 6]]

            res = rows_to_list(
                session.execute("SELECT a, b, e, count(b), max(e) FROM test WHERE a IN (1, 2, 4) AND b = 2"))
            assert res == [[1, 2, 6, 3, 12]]

            # Multi-partitions queries without aggregates
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b"))
            assert res == [[1, 2, 1, 3],
                           [1, 4, 2, 6],
                           [2, 2, 3, 3],
                           [2, 4, 3, 6],
                           [4, 8, 2, 12]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b, c"))
            assert res == [[1, 2, 1, 3],
                           [1, 2, 2, 6],
                           [1, 4, 2, 6],
                           [2, 2, 3, 3],
                           [2, 4, 3, 6],
                           [4, 8, 2, 12]]

            # Multi-partitions queries with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4) GROUP BY a"))
            assert res == [[1, 1],
                           [2, 1],
                           [4, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4)"))
            assert res == [[1, 3]]

            # Multi-partitions query with DISTINCT and LIMIT
            res = rows_to_list(
                session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4) GROUP BY a LIMIT 2"))
            assert res == [[1, 1],
                                   [2, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, count(a)FROM test WHERE a IN (1, 2, 4) LIMIT 2"))
            assert res == [[1, 3]]

            # Multi-partitions queries without aggregates and with PER PARTITION LIMIT
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1"))
            assert res == [[1, 2, 1, 3],
                           [2, 2, 3, 3],
                           [4, 8, 2, 12]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 2"))
            assert res == [[1, 2, 1, 3],
                           [1, 4, 2, 6],
                           [2, 2, 3, 3],
                           [2, 4, 3, 6],
                           [4, 8, 2, 12]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 3"))
            assert res == [[1, 2, 1, 3],
                           [1, 4, 2, 6],
                           [2, 2, 3, 3],
                           [2, 4, 3, 6],
                           [4, 8, 2, 12]]

            # Multi-partitions queries without aggregates, with PER PARTITION LIMIT and with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"))
            assert res == [[1, 2, 1, 3],
                                   [2, 2, 3, 3]]

            res = rows_to_list(session.execute("SELECT a, b, c, d FROM test WHERE a IN (1, 2, 4) GROUP BY a, b PER PARTITION LIMIT 3 LIMIT 2"))
            assert res == [[1, 2, 1, 3],
                                   [1, 4, 2, 6]]

    @since('3.10')
    def test_group_by_with_range_name_query_paging(self):
        """
        @jira_ticket CASSANDRA-10707
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'group_by_with_range_name_query_paging_test', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, d int, primary key (a, b, c))")

        for i in range(1, 5):
            for j in range(1, 5):
                for k in range(1, 5):
                    session.execute("INSERT INTO test (a, b, c, d) VALUES ({}, {}, {}, {})".format(i, j, k, i + j))

        # Makes sure that we have some tombstones
        session.execute("DELETE FROM test WHERE a = 3")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3],
                                   [4, 1, 5, 2, 5]]

            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3],
                                   [4, 1, 5, 2, 5]]

            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [1, 2, 3, 2, 3],
                                   [2, 1, 3, 2, 3],
                                   [2, 2, 4, 2, 4],
                                   [4, 1, 5, 2, 5],
                                   [4, 2, 6, 2, 6]]

            # Range queries with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a LIMIT 5 ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3],
                                   [4, 1, 5, 2, 5]]

            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3],
                                   [4, 1, 5, 2, 5]]

            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b LIMIT 3 ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [1, 2, 3, 2, 3],
                                   [2, 1, 3, 2, 3]]

            # Range queries with PER PARTITION LIMIT
            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3],
                                   [4, 1, 5, 2, 5]]

            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3],
                                   [4, 1, 5, 2, 5]]

            # Range queries with PER PARTITION LIMIT and LIMIT
            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b = 1 and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 5 ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3],
                                   [4, 1, 5, 2, 5]]

            res = rows_to_list(session.execute("SELECT a, b, d, count(b), max(d) FROM test WHERE b IN (1, 2) and c IN (1, 2) GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING"))
            assert res == [[1, 1, 2, 2, 2],
                                   [2, 1, 3, 2, 3]]

    @since('3.10')
    def test_group_by_with_static_columns_paging(self):
        """
        @jira_ticket CASSANDRA-10707
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_group_by_and_static_columns', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, s int static, d int, primary key (a, b, c))")

        # ------------------------------------
        # Test with non static columns empty
        # ------------------------------------

        session.execute("UPDATE test SET s = 1 WHERE a = 1")
        session.execute("UPDATE test SET s = 2 WHERE a = 2")
        session.execute("UPDATE test SET s = 3 WHERE a = 4")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a"))
            assert res == [[1, None, 1, 0, 1],
                                   [2, None, 2, 0, 1],
                                   [4, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b"))
            assert res == [[1, None, 1, 0, 1],
                                   [2, None, 2, 0, 1],
                                   [4, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test"))
            assert res == [[1, None, 1, 0, 3]]

            # Range query without aggregates
            res = rows_to_list(session.execute("SELECT a, b, s FROM test GROUP BY a, b"))
            assert res == [[1, None, 1],
                                   [2, None, 2],
                                   [4, None, 3]]

            # Range queries with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b LIMIT 2"))
            assert res == [[1, None, 1, 0, 1],
                                   [2, None, 2, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test LIMIT 2"))
            assert res == [[1, None, 1, 0, 3]]

            # Range query with PER PARTITION LIMIT
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 2"))
            assert res == [[1, None, 1, 0, 1],
                           [2, None, 2, 0, 1],
                           [4, None, 3, 0, 1]]

            # Range queries with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(s) FROM test GROUP BY a"))
            assert res == [[1, 1, 1],
                           [2, 2, 1],
                           [4, 3, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(s) FROM test "))
            assert res == [[1, 1, 3]]

            # Range queries with DISTINCT and LIMIT
            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(s) FROM test GROUP BY a LIMIT 2"))
            assert res == [[1, 1, 1],
                           [2, 2, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(s) FROM test LIMIT 2"))
            assert res == [[1, 1, 3]]

            # Single partition queries
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a"))
            assert res == [[1, None, 1, 0, 1]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a, b"))
            assert res == [[1, None, 1, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1"))
            assert res == [[1, None, 1, 0, 1]]

            # Single partition query without aggregates
            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a = 1 GROUP BY a, b"))
            assert res == [[1, None, 1]]

            # Single partition queries with LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a, b LIMIT 2"))
            assert res == [[1, None, 1, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 LIMIT 2"))
            assert res == [[1, None, 1, 0, 1]]

            # Single partition queries with PER PARTITION LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a, b PER PARTITION LIMIT 2"))
            assert res == [[1, None, 1, 0, 1]]

            # Single partition queries with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a = 1 GROUP BY a"))
            assert res == [[1, 1, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a = 1"))
            assert res == [[1, 1, 1]]

            # Multi-partitions queries
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a"))
            assert res == [[1, None, 1, 0, 1],
                           [2, None, 2, 0, 1],
                           [4, None, 3, 0, 1]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b"))
            assert res == [[1, None, 1, 0, 1],
                           [2, None, 2, 0, 1],
                           [4, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4)"))
            assert res == [[1, None, 1, 0, 3]]

            # Multi-partitions query without aggregates
            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b"))
            assert res == [[1, None, 1],
                           [2, None, 2],
                           [4, None, 3]]

            # Multi-partitions query with LIMIT
            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 2"))
            assert res == [[1, None, 1, 0, 1],
                           [2, None, 2, 0, 1]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2"))
            assert res == [[1, None, 1, 0, 3]]

            # Multi-partitions query with PER PARTITION LIMIT
            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1"))
            assert res == [[1, None, 1, 0, 1],
                           [2, None, 2, 0, 1],
                           [4, None, 3, 0, 1]]

            # Multi-partitions queries with DISTINCT
            res = rows_to_list(
                session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a"))
            assert res == [[1, 1, 1],
                           [2, 2, 1],
                           [4, 3, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4)"))
            assert res == [[1, 1, 3]]

            # Multi-partitions queries with DISTINCT and LIMIT
            res = rows_to_list(
                session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"))
            assert res == [[1, 1, 1],
                           [2, 2, 1]]

            res = rows_to_list(
                session.execute("SELECT DISTINCT a, s, count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2"))
            assert res == [[1, 1, 3]]

        # ------------------------------------
        # Test with non static columns not empty
        # ------------------------------------
        session.execute("UPDATE test SET s = 3 WHERE a = 3")
        session.execute("DELETE s FROM test WHERE a = 4")

        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 2, 1, 3)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 2, 2, 6)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 3, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 4, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 4, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (2, 2, 3, 3)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (2, 4, 3, 6)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (4, 8, 2, 12)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (5, 8, 2, 12)")

        # Makes sure that we have some tombstones
        session.execute("DELETE FROM test WHERE a = 1 AND b = 3 AND c = 2")
        session.execute("DELETE FROM test WHERE a = 5")

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a"))
            assert res == [[1, 2, 1, 4, 4],
                           [2, 2, 2, 2, 2],
                           [4, 8, None, 1, 0],
                           [3, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b"))
            assert res == [[1, 2, 1, 2, 2],
                           [1, 4, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [2, 4, 2, 1, 1],
                           [4, 8, None, 1, 0],
                           [3, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test"))
            assert res == [[1, 2, 1, 7, 7]]

            res = rows_to_list(
                session.execute(
                    "SELECT a, b, s, count(b), count(s) FROM test WHERE b = 2 GROUP BY a, b ALLOW FILTERING"))
            assert res == [[1, 2, 1, 2, 2],
                           [2, 2, 2, 1, 1]]

            assert_invalid(session, "SELECT a, b, s, count(b), count(s) FROM test WHERE b = 2 GROUP BY a, b", expected=InvalidRequest)

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE b = 2 ALLOW FILTERING"))
            assert res == [[1, 2, 1, 3, 3]]

            assert_invalid(session, "SELECT a, b, s, count(b), count(s) FROM test WHERE b = 2", expected=InvalidRequest)

            # Range queries without aggregates
            res = rows_to_list(session.execute("SELECT a, b, s FROM test GROUP BY a"))
            assert res == [[1, 2, 1],
                           [2, 2, 2],
                           [4, 8, None],
                           [3, None, 3]]

            res = rows_to_list(session.execute("SELECT a, b, s FROM test GROUP BY a, b"))
            assert res == [[1, 2, 1],
                           [1, 4, 1],
                           [2, 2, 2],
                           [2, 4, 2],
                           [4, 8, None],
                           [3, None, 3]]

            # Range query with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a LIMIT 2"))
            assert res == [[1, 2, 1, 4, 4],
                           [2, 2, 2, 2, 2]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test LIMIT 2"))
            assert res == [[1, 2, 1, 7, 7]]

            # Range queries without aggregates and with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, s FROM test GROUP BY a LIMIT 2"))
            assert res == [[1, 2, 1],
                           [2, 2, 2]]

            res = rows_to_list(session.execute("SELECT a, b, s FROM test GROUP BY a, b LIMIT 10"))
            assert res == [[1, 2, 1],
                           [1, 4, 1],
                           [2, 2, 2],
                           [2, 4, 2],
                           [4, 8, None],
                           [3, None, 3]]

            # Range queries with PER PARTITION LIMITS
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 2"))
            assert res == [[1, 2, 1, 2, 2],
                           [1, 4, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [2, 4, 2, 1, 1],
                           [4, 8, None, 1, 0],
                           [3, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1"))
            assert res == [[1, 2, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [4, 8, None, 1, 0],
                           [3, None, 3, 0, 1]]

            # Range queries with PER PARTITION LIMITS and LIMIT
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 5"))
            assert res == [[1, 2, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [4, 8, None, 1, 0],
                           [3, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 4"))
            assert res == [[1, 2, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [4, 8, None, 1, 0],
                           [3, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test GROUP BY a, b PER PARTITION LIMIT 1 LIMIT 2"))
            assert res == [[1, 2, 1, 2, 2],
                           [2, 2, 2, 1, 1]]

            # Range queries with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test GROUP BY a"))
            assert res == [[1, 1, 1, 1],
                           [2, 2, 1, 1],
                           [4, None, 1, 0],
                           [3, 3, 1, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test"))
            assert res == [[1, 1, 4, 3]]

            # Range queries with DISTINCT and LIMIT
            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test GROUP BY a LIMIT 2"))
            assert res == [[1, 1, 1, 1],
                           [2, 2, 1, 1]]

            res = rows_to_list(session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test LIMIT 2"))
            assert res == [[1, 1, 4, 3]]

            # Single partition queries
            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 1 GROUP BY a"))
            assert res == [[1, 2, 1, 4, 4]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 3 GROUP BY a, b"))
            assert res == [[3, None, 3, 0, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 3"))
            assert res == [[3, None, 3, 0, 1]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 AND b = 2 GROUP BY a, b"))
            assert res == [[2, 2, 2, 1, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 AND b = 2"))
            assert res == [[2, 2, 2, 1, 1]]

            # Single partition queries without aggregates
            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a = 1 GROUP BY a"))
            assert res == [[1, 2, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a = 4 GROUP BY a, b"))
            assert res == [[4, 8, None]]

            # Single partition queries with LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b LIMIT 1"))
            assert res == [[2, 2, 2, 1, 1]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 LIMIT 1"))
            assert res == [[2, 2, 2, 2, 2]]

            # Single partition queries without aggregates and with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a = 2 GROUP BY a, b LIMIT 1"))
            assert res == [[2, 2, 2]]

            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a = 2 GROUP BY a, b LIMIT 2"))
            assert res == [[2, 2, 2],
                           [2, 4, 2]]

            # Single partition queries with PER PARTITION LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b PER PARTITION LIMIT 1"))
            assert res == [[2, 2, 2, 1, 1]]

            # Single partition queries with DISTINCT
            res = rows_to_list(
                session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a = 2 GROUP BY a"))
            assert res == [[2, 2, 1, 1]]

            # Single partition queries with ORDER BY
            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC"))
            assert res == [[2, 4, 2, 1, 1],
                           [2, 2, 2, 1, 1]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 ORDER BY b DESC, c DESC"))
            assert res == [[2, 4, 2, 2, 2]]

            # Single partition queries with ORDER BY and LIMIT
            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC LIMIT 1"))
            assert res == [[2, 4, 2, 1, 1]]

            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 ORDER BY b DESC, c DESC LIMIT 2"))
            assert res == [[2, 4, 2, 2, 2]]

            # Single partition queries with ORDER BY and PER PARTITION LIMIT
            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a = 2 GROUP BY a, b ORDER BY b DESC, c DESC PER PARTITION LIMIT 1"))
            assert res == [[2, 4, 2, 1, 1]]

            # Multi-partitions queries
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a"))
            assert res == [[1, 2, 1, 4, 4],
                           [2, 2, 2, 2, 2],
                           [3, None, 3, 0, 1],
                           [4, 8, None, 1, 0]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b"))
            assert res == [[1, 2, 1, 2, 2],
                           [1, 4, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [2, 4, 2, 1, 1],
                           [3, None, 3, 0, 1],
                           [4, 8, None, 1, 0]]

            res = rows_to_list(session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4)"))
            assert res == [[1, 2, 1, 7, 7]]

            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) AND b = 2 GROUP BY a, b"))
            assert res == [[1, 2, 1, 2, 2],
                           [2, 2, 2, 1, 1]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) AND b = 2"))
            assert res == [[1, 2, 1, 3, 3]]

            # Multi-partitions queries without aggregates
            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a"))
            assert res == [[1, 2, 1],
                           [2, 2, 2],
                           [3, None, 3],
                           [4, 8, None]]

            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b"))
            assert res == [[1, 2, 1],
                           [1, 4, 1],
                           [2, 2, 2],
                           [2, 4, 2],
                           [3, None, 3],
                           [4, 8, None]]

            # Multi-partitions queries with LIMIT
            res = rows_to_list(session.execute(
                "SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"))
            assert res == [[1, 2, 1, 4, 4],
                           [2, 2, 2, 2, 2]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2"))
            assert res == [[1, 2, 1, 7, 7]]

            # Multi-partitions queries without aggregates and with LIMIT
            res = rows_to_list(session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"))
            assert res == [[1, 2, 1],
                           [2, 2, 2]]

            res = rows_to_list(
                session.execute("SELECT a, b, s FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b LIMIT 10"))
            assert res == [[1, 2, 1],
                           [1, 4, 1],
                           [2, 2, 2],
                           [2, 4, 2],
                           [3, None, 3],
                           [4, 8, None]]

            # Multi-partitions queries with PER PARTITION LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 1"))
            assert res == [[1, 2, 1, 4, 4],
                           [2, 2, 2, 2, 2],
                           [3, None, 3, 0, 1],
                           [4, 8, None, 1, 0]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2"))
            assert res == [[1, 2, 1, 2, 2],
                           [1, 4, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [2, 4, 2, 1, 1],
                           [3, None, 3, 0, 1],
                           [4, 8, None, 1, 0]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 1"))
            assert res == [[1, 2, 1, 2, 2],
                           [2, 2, 2, 1, 1],
                           [3, None, 3, 0, 1],
                           [4, 8, None, 1, 0]]

            # Multi-partitions queries with DISTINCT
            res = rows_to_list(session.execute(
                "SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a"))
            assert res == [[1, 1, 1, 1],
                           [2, 2, 1, 1],
                           [3, 3, 1, 1],
                           [4, None, 1, 0]]

            # Multi-partitions queries with PER PARTITION LIMIT and LIMIT
            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a PER PARTITION LIMIT 1 LIMIT 3"))
            assert res == [[1, 2, 1, 4, 4],
                           [2, 2, 2, 2, 2],
                           [3, None, 3, 0, 1]]

            res = rows_to_list(
                session.execute("SELECT a, b, s, count(b), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a, b PER PARTITION LIMIT 2 LIMIT 3"))
            assert res == [[1, 2, 1, 2, 2],
                           [1, 4, 1, 2, 2],
                           [2, 2, 2, 1, 1]]

            res = rows_to_list(
                session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4)"))
            assert res == [[1, 1, 4, 3]]

            # Multi-partitions query with DISTINCT and LIMIT
            res = rows_to_list(session.execute(
                "SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4) GROUP BY a LIMIT 2"))
            assert res == [[1, 1, 1, 1],
                           [2, 2, 1, 1]]

            res = rows_to_list(
                session.execute("SELECT DISTINCT a, s, count(a), count(s) FROM test WHERE a IN (1, 2, 3, 4) LIMIT 2"))
            assert res == [[1, 1, 4, 3]]

    @since('2.0.6')
    def test_static_columns_paging(self):
        """
        Exercises paging with static columns to detect bugs
        @jira_ticket CASSANDRA-8502.
        """
        session = self.prepare(row_factory=named_tuple_factory)
        create_ks(session, 'test_paging_static_cols', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, s1 int static, s2 int static, PRIMARY KEY (a, b))")

        for i in range(4):
            for j in range(4):
                session.execute("INSERT INTO test (a, b, c, s1, s2) VALUES (%d, %d, %d, %d, %d)" % (i, j, j, 17, 42))

        selectors = (
            "*",
            "a, b, c, s1, s2",
            "a, b, c, s1",
            "a, b, c, s2",
            "a, b, c")

        PAGE_SIZES = (2, 3, 4, 5, 15, 16, 17, 100)

        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test" % selector))
                assert_length_equal(results, 16)
                assert [0] * 4 + [1] * 4 + [2] * 4 + [3] * 4 == sorted([r.a for r in results])
                assert [0, 1, 2, 3] * 4 == [r.b for r in results]
                assert [0, 1, 2, 3] * 4 == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results]

        # IN over the partitions
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a IN (0, 1, 2, 3)" % selector))
                assert_length_equal(results, 16)
                assert [0] * 4 + [1] * 4 + [2] * 4 + [3] * 4 == sorted([r.a for r in results])
                assert [0, 1, 2, 3] * 4 == [r.b for r in results]
                assert [0, 1, 2, 3] * 4 == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results]

        # single partition
        for i in range(16):
            session.execute("INSERT INTO test (a, b, c, s1, s2) VALUES (%d, %d, %d, %d, %d)" % (99, i, i, 17, 42))

        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99" % selector))
                assert_length_equal(results, 16)
                assert [99] * 16 == [r.a for r in results]
                assert list(range(16)) == [r.b for r in results]
                assert list(range(16)) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results]

        # reversed
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 ORDER BY b DESC" % selector))
                assert_length_equal(results, 16)
                assert [99] * 16 == [r.a for r in results]
                assert list(reversed(list(range(16)))) == [r.b for r in results]
                assert list(reversed(list(range(16)))) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 16 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 16 == [r.s2 for r in results]

        # IN on clustering column
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15)" % selector))
                assert_length_equal(results, 5)
                assert [99] * 5 == [r.a for r in results]
                assert [3, 4, 8, 14, 15] == [r.b for r in results]
                assert [3, 4, 8, 14, 15] == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 5 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 5 == [r.s2 for r in results]

        # reversed IN on clustering column
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15) ORDER BY b DESC" % selector))
                assert_length_equal(results, 5)
                assert [99] * 5 == [r.a for r in results]
                assert list(reversed([3, 4, 8, 14, 15])) == [r.b for r in results]
                assert list(reversed([3, 4, 8, 14, 15])) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 5 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 5 == [r.s2 for r in results]

        # slice on clustering column with set start
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3" % selector))
                assert_length_equal(results, 12)
                assert [99] * 12 == [r.a for r in results]
                assert list(range(4, 16)) == [r.b for r in results]
                assert list(range(4, 16)) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 12 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 12 == [r.s2 for r in results]

        # reversed slice on clustering column with set finish
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 ORDER BY b DESC" % selector))
                assert_length_equal(results, 12)
                assert [99] * 12 == [r.a for r in results]
                assert list(reversed(list(range(4, 16)))) == [r.b for r in results]
                assert list(reversed(list(range(4, 16)))) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 12 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 12 == [r.s2 for r in results]

        # slice on clustering column with set finish
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b < 14" % selector))
                assert_length_equal(results, 14)
                assert [99] * 14 == [r.a for r in results]
                assert list(range(14)) == [r.b for r in results]
                assert list(range(14)) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 14 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 14 == [r.s2 for r in results]

        # reversed slice on clustering column with set start
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b < 14 ORDER BY b DESC" % selector))
                assert_length_equal(results, 14)
                assert [99] * 14 == [r.a for r in results]
                assert list(reversed(list(range(14)))) == [r.b for r in results]
                assert list(reversed(list(range(14)))) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 14 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 14 == [r.s2 for r in results]

        # slice on clustering column with start and finish
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14" % selector))
                assert_length_equal(results, 10)
                assert [99] * 10 == [r.a for r in results]
                assert list(range(4, 14)) == [r.b for r in results]
                assert list(range(4, 14)) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 10 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 10 == [r.s2 for r in results]

        # reversed slice on clustering column with start and finish
        for page_size in PAGE_SIZES:
            logger.debug("Current page size is {}".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14 ORDER BY b DESC" % selector))
                assert_length_equal(results, 10)
                assert [99] * 10 == [r.a for r in results]
                assert list(reversed(list(range(4, 14)))) == [r.b for r in results]
                assert list(reversed(list(range(4, 14)))) == [r.c for r in results]
                if "s1" in selector:
                    assert [17] * 10 == [r.s1 for r in results]
                if "s2" in selector:
                    assert [42] * 10 == [r.s2 for r in results]

    @since('2.0.6')
    def test_paging_using_secondary_indexes_with_static_cols(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, s1 int static, s2 int static, mybool boolean, sometext text, PRIMARY KEY (id, sometext) )")
        session.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(text):
            return str(uuid.uuid4())

        def bool_from_str_int(text):
            return bool(int(text))

        data = """
             | id | s1 | s2 | mybool| sometext |
             +----+----+----+-------+----------+
         *100| 1  | 1  | 4  | 1     | [random] |
         *300| 2  | 2  | 3  | 0     | [random] |
         *500| 3  | 3  | 2  | 1     | [random] |
         *400| 4  | 4  | 1  | 0     | [random] |
            """
        all_data = create_rows(
            data, session, 'paging_test', cl=CL.ALL,
            format_funcs={'id': int, 'mybool': bool_from_str_int, 'sometext': random_txt, 's1': int, 's2': int}
        )

        future = session.execute_async(
            SimpleStatement("select * from paging_test where mybool = true", fetch_size=400, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        # the query only searched for True rows, so let's pare down the expectations for comparison
        expected_data = [x for x in all_data if x.get('mybool') is True]

        assert pf.pagecount() == 2
        assert pf.num_results_all() == [400, 200]
        assert_lists_equal_ignoring_order(expected_data, pf.all_data(), sort_key="sometext")

    def test_static_columns_with_empty_non_static_columns_paging(self):
        """
        @jira_ticket CASSANDRA-10381.
        """
        session = self.prepare(row_factory=named_tuple_factory)
        create_ks(session, 'test_paging_static_cols', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, s int static, PRIMARY KEY (a, b))")

        for i in range(10):
            session.execute("UPDATE test SET s = {} WHERE a = {}".format(i, i))

        session.default_fetch_size = 2
        results = list(session.execute("SELECT * FROM test"))
        assert [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] == sorted([r.s for r in results])

        results = list(session.execute("SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4)"))
        assert [0, 1, 2, 3, 4] == sorted([r.s for r in results])

    def test_select_in_clause_with_duplicate_keys(self):
        """
        @jira_ticket CASSANDRA-12420
        avoid duplicated result when key is duplicated in IN clause
        """
        session = self.prepare(row_factory=named_tuple_factory)
        create_ks(session, 'test_paging_static_cols', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, v int, PRIMARY KEY ((a, b),c))")

        for i in range(3):
            for j in range(3):
                for k in range(3):
                    session.execute("INSERT INTO test (a, b, c, v) VALUES ({}, {}, {}, {})".format(i, j, k, k))

        # based on partition key's token order instead of provided order and no duplication
        for i in range(6):
            session.default_fetch_size = i
            results = rows_to_list(session.execute("SELECT * FROM test WHERE a = 1 AND b in (2, 2, 1, 1, 1)"))
            assert results == [[1, 1, 0, 0],
                                       [1, 1, 1, 1],
                                       [1, 1, 2, 2],
                                       [1, 2, 0, 0],
                                       [1, 2, 1, 1],
                                       [1, 2, 2, 2]]

    @since('3.0.0')
    def test_paging_with_filtering(self):
        """
        @jira_ticket CASSANDRA-6377
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_filtering', 2)
        session.execute("CREATE TABLE test (a int, b int, s int static, c int, d int, primary key (a, b))")

        for i in range(5):
            session.execute("INSERT INTO test (a, s) VALUES ({}, {})".format(i, i))
            # Lets a row with only static values
            if i != 2:
                for j in range(4):
                    session.execute("INSERT INTO test (a, b, c, d) VALUES ({}, {}, {}, {})".format(i, j, j, i + j))

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            assert_all(session, "SELECT * FROM test WHERE c = 2 ALLOW FILTERING", [[1, 2, 1, 2, 3],
                                                                                   [0, 2, 0, 2, 2],
                                                                                   [4, 2, 4, 2, 6],
                                                                                   [3, 2, 3, 2, 5]])

            assert_all(session, "SELECT * FROM test WHERE c > 1 AND c <= 2 ALLOW FILTERING", [[1, 2, 1, 2, 3],
                                                                                              [0, 2, 0, 2, 2],
                                                                                              [4, 2, 4, 2, 6],
                                                                                              [3, 2, 3, 2, 5]])

            assert_all(session, "SELECT * FROM test WHERE c = 2 AND d > 4 ALLOW FILTERING", [[4, 2, 4, 2, 6],
                                                                                             [3, 2, 3, 2, 5]])

            assert_all(session, "SELECT * FROM test WHERE c = 2 AND s > 1 ALLOW FILTERING", [[4, 2, 4, 2, 6],
                                                                                             [3, 2, 3, 2, 5]])

            # Range queries with LIMIT
            assert_all(session, "SELECT * FROM test WHERE c = 2 LIMIT 2 ALLOW FILTERING", [[1, 2, 1, 2, 3],
                                                                                           [0, 2, 0, 2, 2]])

            assert_all(session, "SELECT * FROM test WHERE c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING", [[1, 2, 1, 2, 3],
                                                                                                      [4, 2, 4, 2, 6]])

            # Range query with DISTINCT
            assert_all(session, "SELECT DISTINCT a, s FROM test WHERE s >= 1 ALLOW FILTERING", [[1, 1],
                                                                                                [2, 2],
                                                                                                [4, 4],
                                                                                                [3, 3]])

            # Range query with DISTINCT and LIMIT
            assert_all(session, "SELECT DISTINCT a, s FROM test WHERE s >= 1 LIMIT 2 ALLOW FILTERING", [[1, 1],
                                                                                                        [2, 2]])

            # Single partition queries
            assert_all(session, "SELECT * FROM test WHERE a = 0 AND c >= 1 ALLOW FILTERING", [[0, 1, 0, 1, 1],
                                                                                              [0, 2, 0, 2, 2],
                                                                                              [0, 3, 0, 3, 3]])

            assert_all(session, "SELECT * FROM test WHERE a= 0 AND c >= 1 AND c <=2 ALLOW FILTERING", [[0, 1, 0, 1, 1],
                                                                                                       [0, 2, 0, 2, 2]])

            assert_one(session, "SELECT * FROM test WHERE a = 0 AND c >= 1 AND d = 1 ALLOW FILTERING", [0, 1, 0, 1, 1])

            assert_all(session, "SELECT * FROM test WHERE a = 3 AND c >= 1 AND s > 1 ALLOW FILTERING", [[3, 1, 3, 1, 4],
                                                                                                        [3, 2, 3, 2, 5],
                                                                                                        [3, 3, 3, 3, 6]])

            # Single partition queries with LIMIT
            assert_all(session, "SELECT * FROM test WHERE a = 0 AND c >= 1 LIMIT 2 ALLOW FILTERING", [[0, 1, 0, 1, 1],
                                                                                                      [0, 2, 0, 2, 2]])

            assert_all(session, "SELECT * FROM test WHERE a = 3 AND c >= 1 AND s > 1 LIMIT 2 ALLOW FILTERING", [[3, 1, 3, 1, 4],
                                                                                                                [3, 2, 3, 2, 5]])

            #  Single partition query with DISTINCT
            assert_one(session, "SELECT DISTINCT a, s FROM test WHERE a = 2 AND s >= 1 ALLOW FILTERING", [2, 2])

            # Single partition query with ORDER BY
            assert_all(session, "SELECT * FROM test WHERE a = 0 AND c >= 1 ORDER BY b DESC ALLOW FILTERING", [[0, 3, 0, 3, 3],
                                                                                                              [0, 2, 0, 2, 2],
                                                                                                              [0, 1, 0, 1, 1]])

            # Single partition query with ORDER BY and LIMIT
            assert_all(session, "SELECT * FROM test WHERE a = 0 AND c >= 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING", [[0, 3, 0, 3, 3],
                                                                                                                      [0, 2, 0, 2, 2]])

            # Multi-partitions queries
            assert_all(session, "SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4) AND  c = 2 ALLOW FILTERING", [[0, 2, 0, 2, 2],
                                                                                                             [1, 2, 1, 2, 3],
                                                                                                             [3, 2, 3, 2, 5],
                                                                                                             [4, 2, 4, 2, 6]])

            assert_all(session, "SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4) AND c > 1 AND c <=2 ALLOW FILTERING", [[0, 2, 0, 2, 2],
                                                                                                                      [1, 2, 1, 2, 3],
                                                                                                                      [3, 2, 3, 2, 5],
                                                                                                                      [4, 2, 4, 2, 6]])

            assert_all(session, "SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4) AND c = 2 AND d > 4 ALLOW FILTERING", [[3, 2, 3, 2, 5],
                                                                                                                      [4, 2, 4, 2, 6]])

            assert_all(session, "SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4) AND c = 2 AND s > 1 ALLOW FILTERING", [[3, 2, 3, 2, 5],
                                                                                                                      [4, 2, 4, 2, 6]])

            # Multi-partitions queries with LIMIT
            assert_all(session, "SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4) AND c = 2 LIMIT 2 ALLOW FILTERING", [[0, 2, 0, 2, 2],
                                                                                                                    [1, 2, 1, 2, 3]])

            assert_all(session, "SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4) AND c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING", [[1, 2, 1, 2, 3],
                                                                                                                               [3, 2, 3, 2, 5]])

            # Multi-partitions query with DISTINCT
            assert_all(session, "SELECT DISTINCT a, s FROM test WHERE a IN (0, 1, 2, 3, 4) AND s >= 1 ALLOW FILTERING", [[1, 1],
                                                                                                                         [2, 2],
                                                                                                                         [3, 3],
                                                                                                                         [4, 4]])

            # Multi-partitions query with DISTINCT and LIMIT
            assert_all(session, "SELECT DISTINCT a, s FROM test WHERE a IN (0, 1, 2, 3, 4) AND s >= 1 LIMIT 2 ALLOW FILTERING", [[1, 1],
                                                                                                                                 [2, 2]])

    def _test_paging_with_filtering_on_counter_columns(self, session, with_compact_storage):
        if with_compact_storage:
            create_ks(session, 'test_flt_counter_columns_compact_storage', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE")
        else:
            create_ks(session, 'test_flt_counter_columns', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c))")

        for i in range(5):
            for j in range(10):
                session.execute("UPDATE test SET cnt = cnt + {} WHERE a={} AND b={} AND c={}".format(j + 2, i, j, j + 1))

        self.longMessage = True
        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size
            page_size_error_msg = "Query failed with page size {}".format(page_size)

            # single partition
            res = rows_to_list(session.execute("SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt > 8 ALLOW FILTERING"))
            assert res == [[4, 7, 8, 9],
                           [4, 8, 9, 10],
                           [4, 9, 10, 11]], \
                page_size_error_msg

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt >= 8 ALLOW FILTERING"))
            assert res == [[4, 6, 7, 8],
                           [4, 7, 8, 9],
                           [4, 8, 9, 10],
                           [4, 9, 10, 11]], \
                             page_size_error_msg

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt >= 8 AND cnt < 10 ALLOW FILTERING"))
            assert res == [[4, 6, 7, 8],
                           [4, 7, 8, 9]], \
                             page_size_error_msg

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt >= 8 AND cnt <= 10 ALLOW FILTERING"))
            assert res == [[4, 6, 7, 8],
                           [4, 7, 8, 9],
                           [4, 8, 9, 10]], \
                             page_size_error_msg

            res = rows_to_list(session.execute("SELECT * FROM test WHERE cnt = 5 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 3, 4, 5],
                                              [1, 3, 4, 5],
                                              [2, 3, 4, 5],
                                              [3, 3, 4, 5],
                                              [4, 3, 4, 5]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a IN (1,2,3) AND cnt = 5 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[1, 3, 4, 5],
                                              [2, 3, 4, 5],
                                              [3, 3, 4, 5]])

    @since('3.6')
    def test_paging_with_filtering_on_counter_columns(self):
        """
        test paging, when filtering on counter columns
        @jira_ticket CASSANDRA-11629
        """
        session = self.prepare(row_factory=tuple_factory)
        self._test_paging_with_filtering_on_counter_columns(session, False)

    @since("3.6", max_version="3.X")  # Compact Storage
    def test_paging_with_filtering_on_counter_columns_compact(self):
        """
        test paging, when filtering on counter columns with compact storage
        @jira_ticket CASSANDRA-11629
        """
        session = self.prepare(row_factory=tuple_factory)

        self._test_paging_with_filtering_on_counter_columns(session, True)

    def _test_paging_with_filtering_on_clustering_columns(self, session, with_compact_storage):
        if with_compact_storage:
            create_ks(session, 'test_flt_clustering_columns_compact_storage', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE")
        else:
            create_ks(session, 'test_flt_clustering_columns', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c))")

        for i in range(5):
            for j in range(10):
                session.execute("INSERT INTO test (a,b,c,d) VALUES ({},{},{},{})".format(i, j, j + 1, j + 2))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            # single partition
            assert_all(session, "SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 ALLOW FILTERING", [[4, 4, 5, 6],
                                                                                                       [4, 5, 6, 7],
                                                                                                       [4, 6, 7, 8],
                                                                                                       [4, 7, 8, 9],
                                                                                                       [4, 8, 9, 10],
                                                                                                       [4, 9, 10, 11]])

            assert_all(session, "SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 LIMIT 4 ALLOW FILTERING", [[4, 4, 5, 6],
                                                                                                               [4, 5, 6, 7],
                                                                                                               [4, 6, 7, 8],
                                                                                                               [4, 7, 8, 9]])

            assert_all(session, "SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 ORDER BY b DESC ALLOW FILTERING", [[4, 9, 10, 11],
                                                                                                                       [4, 8, 9, 10],
                                                                                                                       [4, 7, 8, 9],
                                                                                                                       [4, 6, 7, 8],
                                                                                                                       [4, 5, 6, 7],
                                                                                                                       [4, 4, 5, 6]])

            assert_all(session, "SELECT * FROM test WHERE b > 7 AND c > 9 ALLOW FILTERING", [[0, 9, 10, 11],
                                                                                             [1, 9, 10, 11],
                                                                                             [2, 9, 10, 11],
                                                                                             [3, 9, 10, 11],
                                                                                             [4, 9, 10, 11]], ignore_order=True)

            assert_all(session, "SELECT * FROM test WHERE b > 4 AND b < 6 AND c > 3 ALLOW FILTERING", [[0, 5, 6, 7],
                                                                                                       [1, 5, 6, 7],
                                                                                                       [2, 5, 6, 7],
                                                                                                       [3, 5, 6, 7],
                                                                                                       [4, 5, 6, 7]], ignore_order=True)

            assert_all(session, "SELECT * FROM test WHERE d = 5 ALLOW FILTERING", [[0, 3, 4, 5],
                                                                                   [1, 3, 4, 5],
                                                                                   [2, 3, 4, 5],
                                                                                   [3, 3, 4, 5],
                                                                                   [4, 3, 4, 5]], ignore_order=True)

            assert_all(session, "SELECT * FROM test WHERE (b, c) > (4, 3) AND (b, c) < (5, 6) ALLOW FILTERING", [[0, 4, 5, 6],
                                                                                                                 [1, 4, 5, 6],
                                                                                                                 [2, 4, 5, 6],
                                                                                                                 [3, 4, 5, 6],
                                                                                                                 [4, 4, 5, 6]], ignore_order=True)

            assert_all(session, "SELECT * FROM test WHERE (b, c) > (2, 3) AND b < 4 ALLOW FILTERING", [[0, 3, 4, 5],
                                                                                                       [1, 3, 4, 5],
                                                                                                       [2, 3, 4, 5],
                                                                                                       [3, 3, 4, 5],
                                                                                                       [4, 3, 4, 5]], ignore_order=True)

            assert_all(session, "SELECT * FROM test where (b, c) > (2, 2) AND b < 8 AND d = 5 ALLOW FILTERING", [[0, 3, 4, 5],
                                                                                                                 [1, 3, 4, 5],
                                                                                                                 [2, 3, 4, 5],
                                                                                                                 [3, 3, 4, 5],
                                                                                                                 [4, 3, 4, 5]], ignore_order=True)

    @since('3.6')
    def test_paging_with_filtering_on_clustering_columns(self):
        """
        test paging, when filtering on clustering columns
        @jira_ticket CASSANDRA-11310
        """
        session = self.prepare(row_factory=tuple_factory)
        self._test_paging_with_filtering_on_clustering_columns(session, False)

    @since('3.6', max_version="3.X")  # Compact Storage
    def test_paging_with_filtering_on_clustering_columns_compact(self):
        """
        test paging, when filtering on clustering columns with compact storage
        @jira_ticket CASSANDRA-11310
        """
        session = self.prepare(row_factory=tuple_factory)
        self._test_paging_with_filtering_on_clustering_columns(session, True)

    @since('3.6')
    def test_paging_with_filtering_on_clustering_columns_with_contains(self):
        """
        test paging, when filtering on clustering columns (frozen collections) with CONTAINS statement
        @jira_ticket CASSANDRA-11310
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_flt_clustering_clm_contains', 2)
        session.execute("CREATE TABLE test_list (a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c))")
        session.execute("CREATE TABLE test_map (a int, b int, c frozen<map<int, int>>, d int, PRIMARY KEY (a, b, c))")

        for i in range(5):
            for j in range(10):
                session.execute("INSERT INTO test_list (a,b,c,d) VALUES ({},{},[{}, {}],{})".format(i, j, j + 1, j + 2, j + 3, j + 4))
                session.execute("INSERT INTO test_map (a,b,c,d) VALUES ({},{},{{ {}: {} }},{})".format(i, j, j + 1, j + 2, j + 3, j + 4))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            assert_all(session, "SELECT * FROM test_list WHERE c CONTAINS 11 ALLOW FILTERING", [[0, 9, [10, 11], 12],
                                                                                                [1, 9, [10, 11], 12],
                                                                                                [2, 9, [10, 11], 12],
                                                                                                [3, 9, [10, 11], 12],
                                                                                                [4, 9, [10, 11], 12]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_map WHERE c CONTAINS KEY 10 ALLOW FILTERING", [[0, 9, {10: 11}, 12],
                                                                                                   [1, 9, {10: 11}, 12],
                                                                                                   [2, 9, {10: 11}, 12],
                                                                                                   [3, 9, {10: 11}, 12],
                                                                                                   [4, 9, {10: 11}, 12]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_list WHERE c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING", [[0, 1, [2, 3], 4],
                                                                                                                [1, 1, [2, 3], 4],
                                                                                                                [2, 1, [2, 3], 4],
                                                                                                                [3, 1, [2, 3], 4],
                                                                                                                [4, 1, [2, 3], 4]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_map WHERE c CONTAINS KEY 2 AND c CONTAINS 3 ALLOW FILTERING", [[0, 1, {2: 3}, 4],
                                                                                                                   [1, 1, {2: 3}, 4],
                                                                                                                   [2, 1, {2: 3}, 4],
                                                                                                                   [3, 1, {2: 3}, 4],
                                                                                                                   [4, 1, {2: 3}, 4]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_list WHERE c CONTAINS 2 AND d = 4 ALLOW FILTERING", [[0, 1, [2, 3], 4],
                                                                                                         [1, 1, [2, 3], 4],
                                                                                                         [2, 1, [2, 3], 4],
                                                                                                         [3, 1, [2, 3], 4],
                                                                                                         [4, 1, [2, 3], 4]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_map WHERE c CONTAINS KEY 2 AND d = 4 ALLOW FILTERING", [[0, 1, {2: 3}, 4],
                                                                                                            [1, 1, {2: 3}, 4],
                                                                                                            [2, 1, {2: 3}, 4],
                                                                                                            [3, 1, {2: 3}, 4],
                                                                                                            [4, 1, {2: 3}, 4]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_list WHERE c CONTAINS 2 AND d = 4 ALLOW FILTERING", [[0, 1, [2, 3], 4],
                                                                                                         [1, 1, [2, 3], 4],
                                                                                                         [2, 1, [2, 3], 4],
                                                                                                         [3, 1, [2, 3], 4],
                                                                                                         [4, 1, [2, 3], 4]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_map WHERE c CONTAINS KEY 2 AND d = 4 ALLOW FILTERING", [[0, 1, {2: 3}, 4],
                                                                                                            [1, 1, {2: 3}, 4],
                                                                                                            [2, 1, {2: 3}, 4],
                                                                                                            [3, 1, {2: 3}, 4],
                                                                                                            [4, 1, {2: 3}, 4]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_list WHERE c CONTAINS 2 AND d < 4 ALLOW FILTERING", [[0, 0, [1, 2], 3],
                                                                                                         [1, 0, [1, 2], 3],
                                                                                                         [2, 0, [1, 2], 3],
                                                                                                         [3, 0, [1, 2], 3],
                                                                                                         [4, 0, [1, 2], 3]], ignore_order=True)

            assert_all(session, "SELECT * FROM test_map WHERE c CONTAINS KEY 1 AND d < 4 ALLOW FILTERING", [[0, 0, {1: 2}, 3],
                                                                                                            [1, 0, {1: 2}, 3],
                                                                                                            [2, 0, {1: 2}, 3],
                                                                                                            [3, 0, {1: 2}, 3],
                                                                                                            [4, 0, {1: 2}, 3]], ignore_order=True)

    @since('3.6')
    def test_paging_with_filtering_on_static_columns(self):
        """
        test paging, when filtering on static columns
        @jira_ticket CASSANDRA-11310
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_filtering_on_static_columns', 2)
        session.execute("CREATE TABLE test (a int, b int, s int static, d int, PRIMARY KEY (a, b))")

        for i in range(5):
            for j in range(10):
                session.execute("INSERT INTO test (a,b,s,d) VALUES ({},{},{},{})".format(i, j, i + 1, j + 1))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            assert_all(session, "SELECT * FROM test WHERE s > 1 AND b > 8 ALLOW FILTERING", [[1, 9, 2, 10],
                                                                                             [2, 9, 3, 10],
                                                                                             [3, 9, 4, 10],
                                                                                             [4, 9, 5, 10]], ignore_order=True)

            assert_all(session, "SELECT * FROM test WHERE s > 1 AND b > 5 AND b < 7 ALLOW FILTERING", [[1, 6, 2, 7],
                                                                                                       [2, 6, 3, 7],
                                                                                                       [4, 6, 5, 7],
                                                                                                       [3, 6, 4, 7]], ignore_order=True)

            assert_all(session, "SELECT * FROM test WHERE s > 1 AND a = 3 AND b > 4 ALLOW FILTERING", [[3, 5, 4, 6],
                                                                                                       [3, 6, 4, 7],
                                                                                                       [3, 7, 4, 8],
                                                                                                       [3, 8, 4, 9],
                                                                                                       [3, 9, 4, 10]])

            assert_all(session, "SELECT * FROM test WHERE s > 1 AND a = 3 AND b > 4 ORDER BY b DESC ALLOW FILTERING", [[3, 9, 4, 10],
                                                                                                                       [3, 8, 4, 9],
                                                                                                                       [3, 7, 4, 8],
                                                                                                                       [3, 6, 4, 7],
                                                                                                                       [3, 5, 4, 6]])

    @since('3.10')
    def test_paging_with_filtering_on_partition_key(self):
        """
        test allow filtering on partition key
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_filtering_on_pk', 2)
        session.execute("CREATE TABLE test (a int, b int, s int static, c int, d int, primary key (a, b))")

        for i in range(5):
            session.execute("INSERT INTO test (a, s) VALUES ({}, {})".format(i, i))
            # Lets a row with only static values
            if i != 2:
                for j in range(4):
                    session.execute("INSERT INTO test (a, b, c, d) VALUES ({}, {}, {}, {})".format(i, j, j, i + j))

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            # Range queries
            res = rows_to_list(session.execute("SELECT * FROM test WHERE a < 5 AND c = 2 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 2, 0, 2, 2],
                                              [1, 2, 1, 2, 3],
                                              [3, 2, 3, 2, 5],
                                              [4, 2, 4, 2, 6]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 0 AND c > 1 AND c <= 2 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[1, 2, 1, 2, 3],
                                              [3, 2, 3, 2, 5],
                                              [4, 2, 4, 2, 6]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >= 2 AND c = 2 AND d > 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[3, 2, 3, 2, 5],
                                              [4, 2, 4, 2, 6]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >= 2 AND c = 2 AND s > 1 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[3, 2, 3, 2, 5],
                                              [4, 2, 4, 2, 6]])

            # Range queries with LIMIT
            res = rows_to_list(session.execute("SELECT * FROM test WHERE a <= 1 AND c = 2 LIMIT 2 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 2, 0, 2, 2],
                                              [1, 2, 1, 2, 3]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a <= 1 AND c = 2 AND s >= 1 LIMIT 2 ALLOW FILTERING"))
            assert res == [[1, 2, 1, 2, 3]]

            # Range query with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, s FROM test WHERE a >= 2 AND s >= 1 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[2, 2],
                                              [4, 4],
                                              [3, 3]])

            # Single partition queries
            res = rows_to_list(session.execute("SELECT * FROM test WHERE a <= 0 AND c >= 1 ALLOW FILTERING"))
            assert res == [[0, 1, 0, 1, 1],
                           [0, 2, 0, 2, 2],
                           [0, 3, 0, 3, 3]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a < 1 AND c >= 1 AND c <=2 ALLOW FILTERING"))
            assert res == [[0, 1, 0, 1, 1],
                           [0, 2, 0, 2, 2]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >= 0 AND c >= 1 AND d = 1 ALLOW FILTERING"))
            assert res == [[0, 1, 0, 1, 1]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >= 3 AND c >= 1 AND s > 1 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[3, 1, 3, 1, 4],
                                              [3, 2, 3, 2, 5],
                                              [3, 3, 3, 3, 6],
                                              [4, 1, 4, 1, 5],
                                              [4, 2, 4, 2, 6],
                                              [4, 3, 4, 3, 7]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >= 3 AND c >= 1 AND s > 1 PER PARTITION LIMIT 2 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[3, 1, 3, 1, 4],
                                              [3, 2, 3, 2, 5],
                                              [4, 1, 4, 1, 5],
                                              [4, 2, 4, 2, 6]])

            # Single partition queries with LIMIT
            res = rows_to_list(session.execute("SELECT * FROM test WHERE a < 1 AND c >= 1 LIMIT 2 ALLOW FILTERING"))
            assert res == [[0, 1, 0, 1, 1],
                            [0, 2, 0, 2, 2]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >= 3 AND c >= 1 AND s > 1 LIMIT 2 ALLOW FILTERING"))
            assert res == [[4, 1, 4, 1, 5],
                            [4, 2, 4, 2, 6]]

            #  Single partition query with DISTINCT
            res = rows_to_list(session.execute("SELECT DISTINCT a, s FROM test WHERE a > 2 AND s >= 1 ALLOW FILTERING"))
            assert res == [[4, 4],
                           [3, 3]]

            # Single partition query with ORDER BY
            assert_invalid(session, "SELECT * FROM test WHERE a <= 0 AND c >= 1 ORDER BY b DESC ALLOW FILTERING", expected=InvalidRequest)

            # Single partition query with ORDER BY and LIMIT
            assert_invalid(session, "SELECT * FROM test WHERE a <= 0 AND c >= 1 ORDER BY b DESC LIMIT 2 ALLOW FILTERING", expected=InvalidRequest)

    @since('3.10')
    def test_paging_with_filtering_on_partition_key_with_limit(self):
        """
        test allow filtering on partition key
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_filtering_on_pk_with_limit', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, s int static, d int, primary key ((a, b), c))")

        for i in range(5):
            session.execute("INSERT INTO test (a, b, s) VALUES ({}, {}, {})".format(i, 1, i))
            session.execute("INSERT INTO test (a, b, s) VALUES ({}, {}, {})".format(i, 2, i + 1))
            for j in range(10):
                session.execute("INSERT INTO test (a, b, c, d) VALUES ({}, {}, {}, {})".format(i, 1, j, i + j))
                session.execute("INSERT INTO test (a, b, c, d) VALUES ({}, {}, {}, {})".format(i, 2, j, i + j))

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >=2 AND b = 2 LIMIT 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res,
                                        [[2, 2, 0, 3, 2],
                                         [2, 2, 1, 3, 3],
                                         [2, 2, 2, 3, 4],
                                         [2, 2, 3, 3, 5]])

    def _test_paging_with_filtering_on_partition_key_on_counter_columns(self, session, with_compact_storage):
        if with_compact_storage:
            create_ks(session, 'test_flt_counter_columns_compact_storage', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE")
        else:
            create_ks(session, 'test_flt_counter_columns', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c))")

        for i in range(5):
            for j in range(10):
                session.execute("UPDATE test SET cnt = cnt + {} WHERE a={} AND b={} AND c={}".format(j + 2, i, j, j + 1))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            # single partition
            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 3 AND b > 3 AND c > 3 AND cnt > 8 ALLOW FILTERING"))
            assert res == [[4, 7, 8, 9],
                                   [4, 8, 9, 10],
                                   [4, 9, 10, 11]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a >= 4 AND b > 3 AND c > 3 AND cnt >= 8 ALLOW FILTERING"))
            assert res == [[4, 6, 7, 8],
                                   [4, 7, 8, 9],
                                   [4, 8, 9, 10],
                                   [4, 9, 10, 11]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a < 1 AND b > 3 AND c > 3 AND cnt >= 8 AND cnt < 10 ALLOW FILTERING"))
            assert res == [[0, 6, 7, 8],
                                   [0, 7, 8, 9]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 3 AND b > 3 AND c > 3 AND cnt >= 8 AND cnt <= 10 ALLOW FILTERING"))
            assert res == [[4, 6, 7, 8],
                                   [4, 7, 8, 9],
                                   [4, 8, 9, 10]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 1 AND cnt = 5 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[2, 3, 4, 5],
                                              [3, 3, 4, 5],
                                              [4, 3, 4, 5]])

    @since('3.10')
    def test_paging_with_filtering_on_partition_key_on_counter_columns(self):
        """
        test paging, when filtering on partition key on counter columns
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)

        self._test_paging_with_filtering_on_partition_key_on_counter_columns(session, False)

    @since('3.10', max_version="3.X")  # Compact Storage
    def test_paging_with_filtering_on_partition_key_on_counter_columns_compact(self):
        """
        test paging, when filtering on partition key on counter columns with compact storage
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)

        self._test_paging_with_filtering_on_partition_key_on_counter_columns(session, True)

    def _test_paging_with_filtering_on_partition_key_on_clustering_columns(self, session, with_compact_storage):
        if with_compact_storage:
            create_ks(session, 'test_flt_pk_clustering_columns_compact_storage', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY ((a, b), c)) WITH COMPACT STORAGE")
        else:
            create_ks(session, 'test_flt_pk_clustering_columns', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))")

        for i in range(5):
            for j in range(10):
                session.execute("INSERT INTO test (a,b,c,d) VALUES ({},{},{},{})".format(i, j, j + 1, j + 2))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 ALLOW FILTERING"))
            assert res == [[4, 8, 9, 10],
                                   [4, 6, 7, 8],
                                   [4, 7, 8, 9],
                                   [4, 5, 6, 7],
                                   [4, 9, 10, 11],
                                   [4, 4, 5, 6]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 LIMIT 4 ALLOW FILTERING"))
            assert res == [[4, 8, 9, 10],
                                   [4, 6, 7, 8],
                                   [4, 7, 8, 9],
                                   [4, 5, 6, 7]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 ALLOW FILTERING"))
            assert res == [[4, 8, 9, 10],
                                   [4, 6, 7, 8],
                                   [4, 7, 8, 9],
                                   [4, 5, 6, 7],
                                   [4, 9, 10, 11],
                                   [4, 4, 5, 6]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 3 AND b > 3 AND c > 3 ALLOW FILTERING"))
            assert res == [[4, 8, 9, 10],
                                   [4, 6, 7, 8],
                                   [4, 7, 8, 9],
                                   [4, 5, 6, 7],
                                   [4, 9, 10, 11],
                                   [4, 4, 5, 6]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a < 1 AND b > 3 AND c > 3 LIMIT 4 ALLOW FILTERING"))
            assert res == [[0, 6, 7, 8],
                                   [0, 5, 6, 7],
                                   [0, 8, 9, 10],
                                   [0, 9, 10, 11]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a < 1 AND b < 3 AND c >= 3 ALLOW FILTERING"))
            assert res == [[0, 2, 3, 4]]

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 0 AND b > 7 AND c > 9 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[4, 9, 10, 11],
                                              [2, 9, 10, 11],
                                              [1, 9, 10, 11],
                                              [3, 9, 10, 11]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a < 1 AND c > 9 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 9, 10, 11]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE b > 4 AND b < 6 AND c > 3 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[2, 5, 6, 7],
                                              [0, 5, 6, 7],
                                              [1, 5, 6, 7],
                                              [3, 5, 6, 7],
                                              [4, 5, 6, 7]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE d = 5 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 3, 4, 5],
                                              [3, 3, 4, 5],
                                              [1, 3, 4, 5],
                                              [2, 3, 4, 5],
                                              [4, 3, 4, 5]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 0 AND b = 4 AND c >=5 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[3, 4, 5, 6],
                                              [2, 4, 5, 6],
                                              [1, 4, 5, 6],
                                              [4, 4, 5, 6]])

    @since('3.10')
    def test_paging_with_filtering_on_partition_key_on_clustering_columns(self):
        """
        test paging, when filtering on partition key clustering columns
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)
        self._test_paging_with_filtering_on_partition_key_on_clustering_columns(session, False)

    @since('3.10', max_version="3.X")
    def test_paging_with_filtering_on_partition_key_on_clustering_columns_compact(self):
        """
        test paging, when filtering on partition key clustering columns with compact storage
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)
        self._test_paging_with_filtering_on_partition_key_on_clustering_columns(session, True)

    @since('3.10')
    def test_paging_with_filtering_on_partition_key_on_clustering_columns_with_contains(self):
        """
        test paging, when filtering on partition key and clustering columns (frozen collections) with CONTAINS statement
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_flt_pk_clustering_clm_contains', 2)
        session.execute("CREATE TABLE test_list (a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c))")
        session.execute("CREATE TABLE test_map (a int, b int, c frozen<map<int, int>>, d int, PRIMARY KEY (a, b, c))")

        for i in range(5):
            for j in range(10):
                session.execute("INSERT INTO test_list (a,b,c,d) VALUES ({},{},[{}, {}],{})".format(i, j, j + 1, j + 2, j + 3, j + 4))
                session.execute("INSERT INTO test_map (a,b,c,d) VALUES ({},{},{{ {}: {} }},{})".format(i, j, j + 1, j + 2, j + 3, j + 4))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            res = rows_to_list(session.execute("SELECT * FROM test_list WHERE a >= 3 AND c CONTAINS 11 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[3, 9, [10, 11], 12],
                                              [4, 9, [10, 11], 12]])

            res = rows_to_list(session.execute("SELECT * FROM test_map WHERE a <= 4 AND c CONTAINS KEY 10 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 9, {10: 11}, 12],
                                              [1, 9, {10: 11}, 12],
                                              [2, 9, {10: 11}, 12],
                                              [3, 9, {10: 11}, 12],
                                              [4, 9, {10: 11}, 12]])

            res = rows_to_list(session.execute("SELECT * FROM test_list WHERE a >= 0 AND c CONTAINS 2 AND c CONTAINS 3 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 1, [2, 3], 4],
                                              [1, 1, [2, 3], 4],
                                              [2, 1, [2, 3], 4],
                                              [3, 1, [2, 3], 4],
                                              [4, 1, [2, 3], 4]])

            res = rows_to_list(session.execute("SELECT * FROM test_map WHERE a >= 3 AND c CONTAINS KEY 2 AND c CONTAINS 3 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[3, 1, {2: 3}, 4],
                                              [4, 1, {2: 3}, 4]])

            res = rows_to_list(session.execute("SELECT * FROM test_list WHERE a < 5 AND c CONTAINS 2 AND d = 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 1, [2, 3], 4],
                                              [1, 1, [2, 3], 4],
                                              [2, 1, [2, 3], 4],
                                              [3, 1, [2, 3], 4],
                                              [4, 1, [2, 3], 4]])

            res = rows_to_list(session.execute("SELECT * FROM test_map WHERE a > -1 AND c CONTAINS KEY 2 AND d = 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 1, {2: 3}, 4],
                                              [1, 1, {2: 3}, 4],
                                              [2, 1, {2: 3}, 4],
                                              [3, 1, {2: 3}, 4],
                                              [4, 1, {2: 3}, 4]])

            res = rows_to_list(session.execute("SELECT * FROM test_list WHERE a < 4 AND c CONTAINS 2 AND d = 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 1, [2, 3], 4],
                                              [1, 1, [2, 3], 4],
                                              [2, 1, [2, 3], 4],
                                              [3, 1, [2, 3], 4]])

            res = rows_to_list(session.execute("SELECT * FROM test_map WHERE a >= 0 AND c CONTAINS KEY 2 AND d = 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 1, {2: 3}, 4],
                                              [1, 1, {2: 3}, 4],
                                              [2, 1, {2: 3}, 4],
                                              [3, 1, {2: 3}, 4],
                                              [4, 1, {2: 3}, 4]])

            res = rows_to_list(session.execute("SELECT * FROM test_list WHERE a <= 2 AND c CONTAINS 2 AND d < 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 0, [1, 2], 3],
                                              [1, 0, [1, 2], 3],
                                              [2, 0, [1, 2], 3]])

            res = rows_to_list(session.execute("SELECT * FROM test_map WHERE a >= -1 AND c CONTAINS KEY 1 AND d < 4 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[0, 0, {1: 2}, 3],
                                              [1, 0, {1: 2}, 3],
                                              [2, 0, {1: 2}, 3],
                                              [3, 0, {1: 2}, 3],
                                              [4, 0, {1: 2}, 3]])

    @since('3.10')
    def test_paging_with_filtering_on_partition_key_on_static_columns(self):
        """
        test paging, when filtering on partition key, on static columns
        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_filtering_on_pk_static_columns', 2)
        session.execute("CREATE TABLE test (a int, b int, s int static, d int, PRIMARY KEY (a, b))")

        for i in range(5):
            for j in range(10):
                session.execute("INSERT INTO test (a,b,s,d) VALUES ({},{},{},{})".format(i, j, i + 1, j + 1))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a <= 2 AND s > 1 AND b > 8 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[1, 9, 2, 10],
                                              [2, 9, 3, 10]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE a > 3 AND s > 1 AND b > 5 AND b < 7 ALLOW FILTERING"))
            self.assertEqualIgnoreOrder(res, [[4, 6, 5, 7]])

            res = rows_to_list(session.execute("SELECT * FROM test WHERE s > 1 AND a > 3 AND b > 4 ALLOW FILTERING"))
            assert res == [[4, 5, 5, 6],
                                   [4, 6, 5, 7],
                                   [4, 7, 5, 8],
                                   [4, 8, 5, 9],
                                   [4, 9, 5, 10]]

            assert_invalid(session, "SELECT * FROM test WHERE s > 1 AND a < 2 AND b > 4 ORDER BY b DESC ALLOW FILTERING", expected=InvalidRequest)

    @since('2.1.14', max_version="3.X")  # Compact Storage
    def test_paging_on_compact_table_with_tombstone_on_first_column(self):
        """
        test paging, on  COMPACT tables without clustering columns, when the first column has a tombstone
        @jira_ticket CASSANDRA-11467
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_on_compact_table_with_tombstone', 2)
        session.execute("CREATE TABLE test (a int primary key, b int, c int) WITH COMPACT STORAGE")

        for i in range(5):
            session.execute("INSERT INTO test (a, b, c) VALUES ({}, {}, {})".format(i, 1, 1))
            session.execute("DELETE b FROM test WHERE a = {}".format(i))

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            assert_all(session, "SELECT * FROM test", [[1, None, 1],
                                                       [0, None, 1],
                                                       [2, None, 1],
                                                       [4, None, 1],
                                                       [3, None, 1]])

    def test_paging_with_no_clustering_columns(self):
        """
        test paging for tables without clustering columns
        @jira_ticket CASSANDRA-11208
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_no_clustering_columns', 2)
        session.execute("CREATE TABLE test (a int primary key, b int)")
        self._test_paging_with_no_clustering_columns('test', session)

    @since("2.0", max_version="3.X")
    def test_paging_with_no_clustering_columns_compact(self):
        """
        test paging for tables without clustering columns
        @jira_ticket CASSANDRA-11208
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_no_clustering_columns', 2)
        session.execute("CREATE TABLE test_compact (a int primary key, b int) WITH COMPACT STORAGE")
        self._test_paging_with_no_clustering_columns('test_compact', session)

    def _test_paging_with_no_clustering_columns(self, table, session):
        for i in range(5):
            session.execute("INSERT INTO {} (a, b) VALUES ({}, {})".format(table, i, i))

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

        # Range query
        assert_all(session, "SELECT * FROM {}".format(table), [[1, 1],
                                                               [0, 0],
                                                               [2, 2],
                                                               [4, 4],
                                                               [3, 3]])

        # Range query with LIMIT
        assert_all(session, "SELECT * FROM {} LIMIT 3".format(table), [[1, 1],
                                                                       [0, 0],
                                                                       [2, 2]])

        # Range query with DISTINCT
        assert_all(session, "SELECT DISTINCT a FROM {}".format(table), [[1],
                                                                        [0],
                                                                        [2],
                                                                        [4],
                                                                        [3]])

        # Range query with DISTINCT and LIMIT
        assert_all(session, "SELECT DISTINCT a FROM {} LIMIT 3".format(table), [[1],
                                                                                [0],
                                                                                [2]])

        # Multi-partition query
        assert_all(session, "SELECT * FROM {} WHERE a IN (1, 2, 3, 4)".format(table), [[1, 1],
                                                                                       [2, 2],
                                                                                       [3, 3],
                                                                                       [4, 4]])

        # Multi-partition query with LIMIT
        assert_all(session, "SELECT * FROM {} WHERE a IN (1, 2, 3, 4) LIMIT 3".format(table), [[1, 1],
                                                                                               [2, 2],
                                                                                               [3, 3]])

        # Multi-partition query with DISTINCT
        assert_all(session, "SELECT DISTINCT a FROM {} WHERE a IN (1, 2, 3, 4)".format(table), [[1],
                                                                                                [2],
                                                                                                [3],
                                                                                                [4]])

        # Multi-partition query with DISTINCT and LIMIT
        assert_all(session, "SELECT DISTINCT a FROM {} WHERE a IN (1, 2, 3, 4) LIMIT 3".format(table), [[1],
                                                                                                        [2],
                                                                                                        [3]])

    @since('3.6')
    def test_per_partition_limit_paging(self):
        """
        Test paging with per partition limit queries.

        @jira_ticket CASSANDRA-11535
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_per_partition_limit', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, PRIMARY KEY (a, b))")

        for i in range(5):
            for j in range(5):
                session.execute("INSERT INTO test (a, b, c) VALUES ({}, {}, {})".format(i, j, j))

        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            assert_all(session, "SELECT * FROM test PER PARTITION LIMIT 2", [[0, 0, 0],
                                                                             [0, 1, 1],
                                                                             [1, 0, 0],
                                                                             [1, 1, 1],
                                                                             [2, 0, 0],
                                                                             [2, 1, 1],
                                                                             [3, 0, 0],
                                                                             [3, 1, 1],
                                                                             [4, 0, 0],
                                                                             [4, 1, 1]], ignore_order=True)

            res = rows_to_list(session.execute("SELECT * FROM test PER PARTITION LIMIT 2 LIMIT 6"))
            assert_length_equal(res, 6)

            for row in res:
                # since partitions are coming unordered, we don't know which are trimmed by the limit
                assert row[0] in set(range(5))
                assert row[1] in set(range(2))
                assert row[1] in set(range(2))

            # even/odd number of results
            res = rows_to_list(session.execute("SELECT * FROM test PER PARTITION LIMIT 2 LIMIT 5"))
            assert_length_equal(res, 5)

            assert_all(session, "SELECT * FROM test WHERE a IN (1,2,3) PER PARTITION LIMIT 3", [[1, 0, 0],
                                                                                                [1, 1, 1],
                                                                                                [1, 2, 2],
                                                                                                [2, 0, 0],
                                                                                                [2, 1, 1],
                                                                                                [2, 2, 2],
                                                                                                [3, 0, 0],
                                                                                                [3, 1, 1],
                                                                                                [3, 2, 2]])

            assert_all(session, "SELECT * FROM test WHERE a IN (1,2,3) PER PARTITION LIMIT 3 LIMIT 7", [[1, 0, 0],
                                                                                                        [1, 1, 1],
                                                                                                        [1, 2, 2],
                                                                                                        [2, 0, 0],
                                                                                                        [2, 1, 1],
                                                                                                        [2, 2, 2],
                                                                                                        [3, 0, 0]])

            assert_all(session, "SELECT * FROM test WHERE a = 1 PER PARTITION LIMIT 4", [[1, 0, 0],
                                                                                         [1, 1, 1],
                                                                                         [1, 2, 2],
                                                                                         [1, 3, 3]])

            assert_all(session, "SELECT * FROM test WHERE a = 1 PER PARTITION LIMIT 3", [[1, 0, 0],
                                                                                         [1, 1, 1],
                                                                                         [1, 2, 2]])

            assert_all(session, "SELECT * FROM test WHERE a = 1 ORDER BY b DESC PER PARTITION LIMIT 4", [[1, 4, 4],
                                                                                                         [1, 3, 3],
                                                                                                         [1, 2, 2],
                                                                                                         [1, 1, 1]])

            assert_all(session, "SELECT * FROM test WHERE a = 1 PER PARTITION LIMIT 4 LIMIT 3", [[1, 0, 0],
                                                                                                 [1, 1, 1],
                                                                                                 [1, 2, 2]])

            assert_all(session, "SELECT * FROM test WHERE a = 1 AND b > 1 PER PARTITION LIMIT 2 ALLOW FILTERING", [[1, 2, 2],
                                                                                                                   [1, 3, 3]])

            assert_all(session, "SELECT * FROM test WHERE a = 1 AND b > 1 ORDER BY b DESC PER PARTITION LIMIT 2 ALLOW FILTERING", [[1, 4, 4],
                                                                                                                                   [1, 3, 3]])

    def test_paging_for_range_name_queries(self):
        """
        test paging for range name queries
        @jira_ticket CASSANDRA-11669
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_for_range_name_queries', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY(a, b, c))")

        self._test_paging_for_range_name_queries('test', session)

    @since("2.0", max_version="3.X")  # Compact Storage
    def test_paging_for_range_name_queries_compact(self):
        """
        test paging for range name queries with compact storage
        @jira_ticket CASSANDRA-11669
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_for_range_name_queries', 2)
        session.execute("CREATE TABLE test_compact (a int, b int, c int, d int, PRIMARY KEY(a, b, c)) WITH COMPACT STORAGE")

        self._test_paging_for_range_name_queries('test_compact', session)

    def _test_paging_for_range_name_queries(self, table, session):
        for i in range(4):
            for j in range(4):
                for k in range(4):
                    session.execute("INSERT INTO {} (a, b, c, d) VALUES ({}, {}, {}, {})".format(table, i, j, k, i + j))

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            assert_all(session, "SELECT * FROM {} WHERE b = 1 AND c = 1  ALLOW FILTERING".format(table), [[1, 1, 1, 2],
                                                                                                          [0, 1, 1, 1],
                                                                                                          [2, 1, 1, 3],
                                                                                                          [3, 1, 1, 4]])

            assert_all(session, "SELECT * FROM {} WHERE b = 1 AND c IN (1, 2) ALLOW FILTERING".format(table), [[1, 1, 1, 2],
                                                                                                               [1, 1, 2, 2],
                                                                                                               [0, 1, 1, 1],
                                                                                                               [0, 1, 2, 1],
                                                                                                               [2, 1, 1, 3],
                                                                                                               [2, 1, 2, 3],
                                                                                                               [3, 1, 1, 4],
                                                                                                               [3, 1, 2, 4]])

            if self.cluster.version() >= '2.2':
                assert_all(session, "SELECT * FROM {} WHERE b IN (1, 2) AND c IN (1, 2)  ALLOW FILTERING".format(table), [[1, 1, 1, 2],
                                                                                                                          [1, 1, 2, 2],
                                                                                                                          [1, 2, 1, 3],
                                                                                                                          [1, 2, 2, 3],
                                                                                                                          [0, 1, 1, 1],
                                                                                                                          [0, 1, 2, 1],
                                                                                                                          [0, 2, 1, 2],
                                                                                                                          [0, 2, 2, 2],
                                                                                                                          [2, 1, 1, 3],
                                                                                                                          [2, 1, 2, 3],
                                                                                                                          [2, 2, 1, 4],
                                                                                                                          [2, 2, 2, 4],
                                                                                                                          [3, 1, 1, 4],
                                                                                                                          [3, 1, 2, 4],
                                                                                                                          [3, 2, 1, 5],
                                                                                                                          [3, 2, 2, 5]])

    @flaky
    @since('2.1')
    def test_paging_with_empty_row_and_empty_static_columns(self):
        """
        test paging when the rows and the static columns are empty
        @jira_ticket CASSANDRA-13017
        """
        session = self.prepare(row_factory=tuple_factory)
        create_ks(session, 'test_paging_with_empty_rows_and_static_columns', 2)
        session.execute("CREATE TABLE test (pk int, c int, v int, s int static, primary key(pk, c))")

        for i in range(5):
            for j in range(5):
                session.execute("INSERT INTO test (pk, c) VALUES ({}, {})".format(i, j))

        for page_size in (2, 3, 4, 5, 7, 10):
            session.default_fetch_size = page_size

            res = rows_to_list(session.execute("SELECT DISTINCT pk FROM test"))
            assert res == [[1],
                           [0],
                           [2],
                           [4],
                           [3]]

            res = rows_to_list(session.execute("SELECT DISTINCT pk FROM test LIMIT 4"))
            assert res == [[1],
                           [0],
                           [2],
                           [4]]

            res = rows_to_list(session.execute("SELECT DISTINCT pk, s FROM test"))
            assert res == [[1, None],
                           [0, None],
                           [2, None],
                           [4, None],
                           [3, None]]

            res = rows_to_list(session.execute("SELECT DISTINCT pk, s FROM test LIMIT 4"))
            assert res == [[1, None],
                           [0, None],
                           [2, None],
                           [4, None]]


@since('2.0')
class TestPagingDatasetChanges(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when the queried dataset changes while pages are being retrieved.
    """

    def test_data_change_impacting_earlier_page(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
              | id | mytext   |
              +----+----------+
          *500| 1  | [random] |
          *500| 2  | [random] |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt})

        # get 501 rows so we have definitely got the 1st row of the second partition
        future = session.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=501, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # we got one page and should be done with the first partition (for id=1)
        # let's add another row for that first partition (id=1) and make sure it won't sneak into results
        session.execute(SimpleStatement("insert into paging_test (id, mytext) values (1, 'foo')", consistency_level=CL.ALL))

        pf.request_all()
        assert pf.pagecount() == 2
        assert pf.num_results_all(), [501, 499]

        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    def test_data_change_impacting_later_page(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
              | id | mytext   |
              +----+----------+
          *500| 1  | [random] |
          *499| 2  | [random] |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=500, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # we've already paged the first partition, but adding a row for the second (id=2)
        # should still result in the row being seen on the subsequent pages
        session.execute(SimpleStatement("insert into paging_test (id, mytext) values (2, 'foo')", consistency_level=CL.ALL))

        pf.request_all()
        assert pf.pagecount() == 2
        assert pf.num_results_all(), [500 == 500]

        # add the new row to the expected data and then do a compare
        expected_data.append({'id': 2, 'mytext': 'foo'})
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    def test_row_TTL_expiry_during_paging(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return str(uuid.uuid4())

        # create rows with TTL (some of which we'll try to get after expiry)
        create_rows(
            """
                | id | mytext   |
                +----+----------+
            *300| 1  | [random] |
            *400| 2  | [random] |
            """,
            session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}, postfix='USING TTL 10'
        )

        # create rows without TTL
        create_rows(
            """
                | id | mytext   |
                +----+----------+
            *500| 3  | [random] |
            """,
            session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}
        )

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2,3)", fetch_size=300, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved
        # this page will be partition id=1, it has TTL rows but they are not expired yet

        # sleep so that the remaining TTL rows from partition id=2 expire
        time.sleep(15)

        pf.request_all()
        assert pf.pagecount() == 3
        assert pf.num_results_all() == [300, 300, 200]

    def test_cell_TTL_expiry_during_paging(self):
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("""
            CREATE TABLE paging_test (
                id int,
                mytext text,
                somevalue text,
                anothervalue text,
                PRIMARY KEY (id, mytext) )
            """)

        def random_txt(text):
            return str(uuid.uuid4())

        data = create_rows(
            """
                | id | mytext   | somevalue | anothervalue |
                +----+----------+-----------+--------------+
            *500| 1  | [random] | foo       |  bar         |
            *500| 2  | [random] | foo       |  bar         |
            *500| 3  | [random] | foo       |  bar         |
            """,
            session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}
        )

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2,3)", fetch_size=500, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)

        # no need to request page here, because the first page is automatically retrieved
        page1 = pf.page_data(1)
        assert_lists_equal_ignoring_order(page1, data[:500], sort_key="mytext")

        # set some TTLs for data on page 3
        for row in data[1000:1500]:
            _id, mytext = row['id'], row['mytext']
            stmt = SimpleStatement("""
                update paging_test using TTL 10
                set somevalue='one', anothervalue='two' where id = {id} and mytext = '{mytext}'
                """.format(id=_id, mytext=mytext),
                consistency_level=CL.ALL
            )
            session.execute(stmt)

        # check page two
        pf.request_one()
        page2 = pf.page_data(2)
        assert_lists_equal_ignoring_order(page2, data[500:1000], sort_key="mytext")

        page3expected = []
        for row in data[1000:1500]:
            _id, mytext = row['id'], row['mytext']
            page3expected.append(
                {'id': _id, 'mytext': mytext, 'somevalue': None, 'anothervalue': None}
            )

        time.sleep(15)

        pf.request_one()
        page3 = pf.page_data(3)
        assert_lists_equal_ignoring_order(page3, page3expected, sort_key="mytext")

    def test_node_unavailabe_during_paging(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        create_ks(session, 'test_paging_size', 1)
        session.execute("CREATE TABLE paging_test ( id uuid, mytext text, PRIMARY KEY (id, mytext) )")

        def make_uuid(text):
            return uuid.uuid4()

        create_rows(
            """
                  | id      | mytext |
                  +---------+--------+
            *10000| [uuid]  | foo    |
            """,
            session, 'paging_test', cl=CL.ALL, format_funcs={'id': make_uuid}
        )

        future = session.execute_async(
            SimpleStatement("select * from paging_test where mytext = 'foo' allow filtering", fetch_size=2000, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # stop a node and make sure we get an error trying to page the rest
        node1.stop()
        with pytest.raises(RuntimeError, match='Requested pages were not delivered before timeout'):
            pf.request_all()

        # TODO: can we resume the node and expect to get more results from the result set or is it done?


@since('2.0')
class TestPagingQueryIsolation(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with isolation of paged queries (queries can't affect each other).
    """

    def test_query_isolation(self):
        """
        Interleave some paged queries and make sure nothing bad happens.
        """
        session = self.prepare()
        create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
               | id | mytext   |
               +----+----------+
          *5000| 1  | [random] |
          *5000| 2  | [random] |
          *5000| 3  | [random] |
          *5000| 4  | [random] |
          *5000| 5  | [random] |
          *5000| 6  | [random] |
          *5000| 7  | [random] |
          *5000| 8  | [random] |
          *5000| 9  | [random] |
          *5000| 10 | [random] |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt})

        stmts = [
            SimpleStatement("select * from paging_test where id in (1)", fetch_size=500, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (2)", fetch_size=600, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (3)", fetch_size=700, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (4)", fetch_size=800, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (5)", fetch_size=900, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (1)", fetch_size=1000, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (2)", fetch_size=1100, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (3)", fetch_size=1200, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (4)", fetch_size=1300, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (5)", fetch_size=1400, consistency_level=CL.ALL),
            SimpleStatement("select * from paging_test where id in (1,2,3,4,5,6,7,8,9,10)", fetch_size=1500, consistency_level=CL.ALL)
        ]

        page_fetchers = []

        for stmt in stmts:
            future = session.execute_async(stmt)
            page_fetchers.append(PageFetcher(future))
            # first page is auto-retrieved, so no need to request it

        for pf in page_fetchers:
            pf.request_one()

        for pf in page_fetchers:
            pf.request_one()

        for pf in page_fetchers:
            pf.request_all()

        assert page_fetchers[0].pagecount() == 10
        assert page_fetchers[1].pagecount() == 9
        assert page_fetchers[2].pagecount() == 8
        assert page_fetchers[3].pagecount() == 7
        assert page_fetchers[4].pagecount() == 6
        assert page_fetchers[5].pagecount() == 5
        assert page_fetchers[6].pagecount() == 5
        assert page_fetchers[7].pagecount() == 5
        assert page_fetchers[8].pagecount() == 4
        assert page_fetchers[9].pagecount() == 4
        assert page_fetchers[10].pagecount() == 34

        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[0].all_data()), flatten_into_set(expected_data[:5000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[1].all_data()), flatten_into_set(expected_data[5000:10000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[2].all_data()), flatten_into_set(expected_data[10000:15000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[3].all_data()), flatten_into_set(expected_data[15000:20000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[4].all_data()), flatten_into_set(expected_data[20000:25000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[5].all_data()), flatten_into_set(expected_data[:5000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[6].all_data()), flatten_into_set(expected_data[5000:10000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[7].all_data()), flatten_into_set(expected_data[10000:15000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[8].all_data()), flatten_into_set(expected_data[15000:20000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[9].all_data()), flatten_into_set(expected_data[20000:25000]))
        self.assertEqualIgnoreOrder(flatten_into_set(page_fetchers[10].all_data()), flatten_into_set(expected_data[:50000]))


@since('2.0')
class TestPagingWithDeletions(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when deletions occur.
    """

    def setup_data(self):

        create_ks(self.session, 'test_paging_size', 2)
        self.session.execute("CREATE TABLE paging_test ( "
                             "id int, mytext text, col1 int, col2 int, col3 int, "
                             "PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return str(uuid.uuid4())

        data = """
             | id | mytext   | col1 | col2 | col3 |
             +----+----------+------+------+------+
          *40| 1  | [random] | 1    | 1    | 1    |
          *40| 2  | [random] | 2    | 2    | 2    |
          *40| 3  | [random] | 4    | 3    | 3    |
          *40| 4  | [random] | 4    | 4    | 4    |
          *40| 5  | [random] | 5    | 5    | 5    |
        """

        create_rows(data, self.session, 'paging_test', cl=CL.ALL,
                    format_funcs={
                        'id': int,
                        'mytext': random_txt,
                        'col1': int,
                        'col2': int,
                        'col3': int
                    })

        pf = self.get_page_fetcher()
        pf.request_all()
        return pf.all_data()

    def get_page_fetcher(self):
        future = self.session.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2,3,4,5)", fetch_size=25,
                            consistency_level=CL.ALL)
        )

        return PageFetcher(future)

    def check_all_paging_results(self, expected_data, pagecount, num_page_results):
        """Check all paging results: pagecount, num_results per page, data."""

        page_size = 25
        expected_pages_data = [expected_data[x:x + page_size] for x in
                               range(0, len(expected_data), page_size)]

        pf = self.get_page_fetcher()
        pf.request_all(timeout=60)
        assert pf.pagecount() == pagecount
        assert pf.num_results_all() == num_page_results

        for i in range(pf.pagecount()):
            page_data = pf.page_data(i + 1)
            assert page_data == expected_pages_data[i]

    def test_single_partition_deletions(self):
        """Test single partition deletions """
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the a single partition at the beginning
        self.session.execute(
            SimpleStatement("delete from paging_test where id = 1",
                            consistency_level=CL.ALL)
        )
        expected_data = [row for row in expected_data if row['id'] != 1]
        self.check_all_paging_results(expected_data, 7,
                                      [25, 25, 25, 25, 25, 25, 10])

        # Delete the a single partition in the middle
        self.session.execute(
            SimpleStatement("delete from paging_test where id = 3",
                            consistency_level=CL.ALL)
        )
        expected_data = [row for row in expected_data if row['id'] != 3]
        self.check_all_paging_results(expected_data, 5, [25, 25, 25, 25, 20])

        # Delete the a single partition at the end
        self.session.execute(
            SimpleStatement("delete from paging_test where id = 5",
                            consistency_level=CL.ALL)
        )
        expected_data = [row for row in expected_data if row['id'] != 5]
        self.check_all_paging_results(expected_data, 4, [25, 25, 25, 5])

        # Keep only the partition '2'
        self.session.execute(
            SimpleStatement("delete from paging_test where id = 4",
                            consistency_level=CL.ALL)
        )
        expected_data = [row for row in expected_data if row['id'] != 4]
        self.check_all_paging_results(expected_data, 2, [25, 15])

    def test_multiple_partition_deletions(self):
        """Test multiple partition deletions """
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Keep only the partition '1'
        self.session.execute(
            SimpleStatement("delete from paging_test where id in (2,3,4,5)",
                            consistency_level=CL.ALL)
        )
        expected_data = [row for row in expected_data if row['id'] == 1]
        self.check_all_paging_results(expected_data, 2, [25, 15])

    def test_single_row_deletions(self):
        """Test single row deletions """
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the first row
        row = expected_data.pop(0)
        self.session.execute(SimpleStatement(
            ("delete from paging_test where "
             "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
            consistency_level=CL.ALL)
        )
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 24])

        # Delete a row in the middle
        row = expected_data.pop(100)
        self.session.execute(SimpleStatement(
            ("delete from paging_test where "
             "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
            consistency_level=CL.ALL)
        )
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 23])

        # Delete the last row
        row = expected_data.pop()
        self.session.execute(SimpleStatement(
            ("delete from paging_test where "
             "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
            consistency_level=CL.ALL)
        )
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 22])

        # Delete all the last page row by row
        rows = expected_data[-22:]
        for row in rows:
            self.session.execute(SimpleStatement(
                ("delete from paging_test where "
                 "id = {} and mytext = '{}'".format(row['id'], row['mytext'])),
                consistency_level=CL.ALL)
            )
        self.check_all_paging_results(expected_data, 7,
                                      [25, 25, 25, 25, 25, 25, 25])

    @pytest.mark.skip(reason="Feature In Development")
    def test_multiple_row_deletions(self):
        """
        Test multiple row deletions.
        This test should be finished when CASSANDRA-6237 is done.
        """
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete a bunch of rows
        rows = expected_data[100:105]
        expected_data = expected_data[0:100] + expected_data[105:]
        in_condition = ','.join("'{}'".format(r['mytext']) for r in rows)

        self.session.execute(SimpleStatement(
            ("delete from paging_test where "
             "id = {} and mytext in ({})".format(3, in_condition)),
            consistency_level=CL.ALL)
        )
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 20])

    def test_single_cell_deletions(self):
        """Test single cell deletions """
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the first cell of some rows of the last partition
        pkeys = [r['mytext'] for r in expected_data if r['id'] == 5][:20]
        for r in expected_data:
            if r['id'] == 5 and r['mytext'] in pkeys:
                r['col1'] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement(
                ("delete col1 from paging_test where id = 5 "
                 "and mytext = '{}'".format(pkey)),
                consistency_level=CL.ALL))
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 25])

        # Delete the mid cell of some rows of the first partition
        pkeys = [r['mytext'] for r in expected_data if r['id'] == 1][20:]
        for r in expected_data:
            if r['id'] == 1 and r['mytext'] in pkeys:
                r['col2'] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement(
                ("delete col2 from paging_test where id = 1 "
                 "and mytext = '{}'".format(pkey)),
                consistency_level=CL.ALL))
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 25])

        # Delete the last cell of all rows of the mid partition
        pkeys = [r['mytext'] for r in expected_data if r['id'] == 3]
        for r in expected_data:
            if r['id'] == 3 and r['mytext'] in pkeys:
                r['col3'] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement(
                ("delete col3 from paging_test where id = 3 "
                 "and mytext = '{}'".format(pkey)),
                consistency_level=CL.ALL))
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 25])

    def test_multiple_cell_deletions(self):
        """Test multiple cell deletions """
        self.session = self.prepare()
        expected_data = self.setup_data()

        # Delete the multiple cells of some rows of the second partition
        pkeys = [r['mytext'] for r in expected_data if r['id'] == 2][20:]
        for r in expected_data:
            if r['id'] == 2 and r['mytext'] in pkeys:
                r['col1'] = None
                r['col2'] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement(
                ("delete col1, col2 from paging_test where id = 2 "
                 "and mytext = '{}'".format(pkey)),
                consistency_level=CL.ALL))
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 25])

        # Delete the multiple cells of all rows of the fourth partition
        pkeys = [r['mytext'] for r in expected_data if r['id'] == 4]
        for r in expected_data:
            if r['id'] == 4 and r['mytext'] in pkeys:
                r['col2'] = None
                r['col3'] = None

        for pkey in pkeys:
            self.session.execute(SimpleStatement(
                ("delete col2, col3 from paging_test where id = 4 "
                 "and mytext = '{}'".format(pkey)),
                consistency_level=CL.ALL))
        self.check_all_paging_results(expected_data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 25])

    def test_ttl_deletions(self):
        """Test ttl deletions. Paging over a query that has only tombstones """
        self.session = self.prepare()
        data = self.setup_data()

        # Set TTL to all row
        ttl_seconds = 15
        for row in data:
            s = ("insert into paging_test (id, mytext, col1, col2, col3) "
                 "values ({}, '{}', {}, {}, {}) using ttl {};").format(
                row['id'], row['mytext'], row['col1'],
                row['col2'], row['col3'], ttl_seconds)
            self.session.execute(
                SimpleStatement(s, consistency_level=CL.ALL)
            )
        self.check_all_paging_results(data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 25])
        time.sleep(ttl_seconds)
        self.check_all_paging_results([], 0, [])

    def test_failure_threshold_deletions(self):
        """Test that paging throws a failure in case of tombstone threshold """
        supports_v5_protocol = self.supports_v5_protocol(self.cluster.version())

        self.fixture_dtest_setup.allow_log_errors = True
        self.cluster.set_configuration_options(
            values={'tombstone_failure_threshold': 500}
        )
        self.session = self.prepare()
        self.setup_data()

        # Add more data
        values = [uuid.uuid4() for i in range(3000)]
        for value in values:
            self.session.execute(SimpleStatement(
                "insert into paging_test (id, mytext, col1) values (1, '{}', null) ".format(
                    value
                ),
                consistency_level=CL.ALL
            ))

        try:
            self.session.execute(SimpleStatement("select * from paging_test", fetch_size=1000, consistency_level=CL.ALL, retry_policy=FallthroughRetryPolicy()))
        except ReadTimeout as exc:
            assert self.cluster.version() < LooseVersion('2.2')
        except ReadFailure as exc:
            if supports_v5_protocol:
                assert exc.error_code_map is not None
                assert 0x0001 == list(exc.error_code_map.values())[0]
        except Exception:
            raise
        else:
            pytest.fail('Expected ReadFailure or ReadTimeout, depending on the cluster version')

        if self.cluster.version() < "3.0":
            failure_msg = ("Scanned over.* tombstones in test_paging_size."
                           "paging_test.* query aborted")
        else:
            failure_msg = ("Scanned over.* tombstones during query.* query aborted")

        self.cluster.wait_for_any_log(failure_msg, 25)

    @since('2.2.6')
    def test_deletion_with_distinct_paging(self):
        """
        Test that deletion does not affect paging for distinct queries.

        @jira_ticket CASSANDRA-10010
        """
        self.session = self.prepare()
        create_ks(self.session, 'test_paging_size', 2)
        self.session.execute("CREATE TABLE paging_test ( "
                             "k int, s int static, c int, v int, "
                             "PRIMARY KEY (k, c) )")

        for whereClause in ('', 'WHERE k IN (0, 1, 2, 3)'):
            for i in range(4):
                for j in range(2):
                    self.session.execute("INSERT INTO paging_test (k, s, c, v) VALUES (%s, %s, %s, %s)", (i, i, j, j))

            self.session.default_fetch_size = 2
            result = self.session.execute("SELECT DISTINCT k, s FROM paging_test {}".format(whereClause))
            result = list(result)
            assert 4 == len(result)

            future = self.session.execute_async("SELECT DISTINCT k, s FROM paging_test {}".format(whereClause))

            # this will fetch the first page
            fetcher = PageFetcher(future)

            # delete the first row in the last partition that was returned in the first page
            self.session.execute("DELETE FROM paging_test WHERE k = %s AND c = %s", (result[1]['k'], 0))

            # finish paging
            fetcher.request_all()
            assert [2, 2] == fetcher.num_results_all()
