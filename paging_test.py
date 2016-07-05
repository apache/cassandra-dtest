import time
import uuid

from cassandra import ConsistencyLevel as CL
from cassandra import InvalidRequest, ReadFailure, ReadTimeout
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import (SimpleStatement, dict_factory,
                             named_tuple_factory, tuple_factory)

from assertions import assert_invalid, assert_all, assert_one, assert_length_equal
from datahelp import create_rows, flatten_into_set, parse_data_into_dicts
from dtest import debug, Tester, run_scenarios
from tools import known_failure, rows_to_list, since


class Page(object):
    data = None

    def __init__(self):
        self.data = []

    def add_row(self, row):
        self.data.append(row)


class PageFetcher(object):
    """
    Requests pages, handles their receipt,
    and provides paged data for testing.

    The first page is automatically retrieved, so an initial
    call to request_one is actually getting the *second* page!
    """
    pages = None
    error = None
    future = None
    requested_pages = None
    retrieved_pages = None
    retrieved_empty_pages = None

    def __init__(self, future):
        self.pages = []

        # the first page is automagically returned (eventually)
        # so we'll count this as a request, but the retrieved count
        # won't be incremented until it actually arrives
        self.requested_pages = 1
        self.retrieved_pages = 0
        self.retrieved_empty_pages = 0

        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error
        )

        # wait for the first page to arrive, otherwise we may call
        # future.has_more_pages too early, since it should only be
        # called after the first page is returned
        self.wait(seconds=30)

    def handle_page(self, rows):
        # occasionally get a final blank page that is useless
        if rows == []:
            self.retrieved_empty_pages += 1
            return

        page = Page()
        self.pages.append(page)

        for row in rows:
            page.add_row(row)

        self.retrieved_pages += 1

    def handle_error(self, exc):
        self.error = exc
        raise exc

    def request_one(self):
        """
        Requests the next page if there is one.

        If the future is exhausted, this is a no-op.
        """
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait()

        return self

    def request_all(self):
        """
        Requests any remaining pages.

        If the future is exhausted, this is a no-op.
        """
        while self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait()

        return self

    def wait(self, seconds=5):
        """
        Blocks until all *requested* pages have been returned.

        Requests are made by calling request_one and/or request_all.

        Raises RuntimeError if seconds is exceeded.
        """
        expiry = time.time() + seconds

        while time.time() < expiry:
            if self.requested_pages == (self.retrieved_pages + self.retrieved_empty_pages):
                return self
            # small wait so we don't need excess cpu to keep checking
            time.sleep(0.1)

        raise RuntimeError(
            "Requested pages were not delivered before timeout." +
            "Requested: %d; retrieved: %d; empty retrieved: %d" %
            (self.requested_pages, self.retrieved_pages, self.retrieved_empty_pages))

    def pagecount(self):
        """
        Returns count of *retrieved* pages which were not empty.

        Pages are retrieved by requesting them with request_one and/or request_all.
        """
        return len(self.pages)

    def num_results(self, page_num):
        """
        Returns the number of results found at page_num
        """
        return len(self.pages[page_num - 1].data)

    def num_results_all(self):
        return [len(page.data) for page in self.pages]

    def page_data(self, page_num):
        """
        Returns retreived data found at pagenum.

        The page should have already been requested with request_one and/or request_all.
        """
        return self.pages[page_num - 1].data

    def all_data(self):
        """
        Returns all retrieved data flattened into a single list (instead of separated into Page objects).

        The page(s) should have already been requested with request_one and/or request_all.
        """
        all_pages_combined = []
        for page in self.pages:
            all_pages_combined.extend(page.data[:])

        return all_pages_combined

    @property  # make property to match python driver api
    def has_more_pages(self):
        """
        Returns bool indicating if there are any pages not retrieved.
        """
        return self.future.has_more_pages


class PageAssertionMixin(object):
    """Can be added to subclasses of unittest.Tester"""

    def assertEqualIgnoreOrder(self, actual, expected):
        return self.assertItemsEqual(actual, expected)

    def assertIsSubsetOf(self, subset, superset):
        self.assertLessEqual(flatten_into_set(subset), flatten_into_set(superset))


class BasePagingTester(Tester):

    def prepare(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.row_factory = dict_factory
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
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        # run a query that has no results and make sure it's exhausted
        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        pf.request_all()
        self.assertEqual([], pf.all_data())
        self.assertFalse(pf.has_more_pages)

    def test_with_less_results_than_page_size(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
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
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future)
        pf.request_all()

        self.assertFalse(pf.has_more_pages)
        self.assertEqual(len(expected_data), len(pf.all_data()))

    def test_with_more_results_than_page_size(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
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
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [5, 4])

        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    def test_with_equal_results_to_page_size(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
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
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.num_results_all(), [5])
        self.assertEqual(pf.pagecount(), 1)

        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    def test_undefined_page_size_default(self):
        """
        If the page size isn't sent then the default fetch size is used.
        """
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        def random_txt(text):
            return uuid.uuid4()

        data = """
               | id     |value   |
               +--------+--------+
          *5001| [uuid] |testing |
            """
        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': random_txt, 'value': unicode})

        future = session.execute_async(
            SimpleStatement("select * from paging_test", consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.num_results_all(), [5000, 1])

        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)


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
        self.create_ks(session, 'test_paging', 2)
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

        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id = 1 order by value asc", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [5, 5])

        # these should be equal (in the same order)
        self.assertEqual(pf.all_data(), expected_data)

        # make sure we don't allow paging over multiple partitions with order because that's weird
        with self.assertRaisesRegexp(InvalidRequest, 'Cannot page queries with both ORDER BY and a IN restriction on the partition key'):
            stmt = SimpleStatement("select * from paging_test where id in (1,2) order by value asc", consistency_level=CL.ALL)
            session.execute(stmt)

    def test_with_order_by_reversed(self):
        """"
        Paging over a single partition with ordering and a reversed clustering order.
        """
        session = self.prepare()
        self.create_ks(session, 'test_paging', 2)
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

        expected_data = create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode, 'value2': unicode})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where id = 1 order by value asc", fetch_size=3, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all(), [3, 3, 3, 1])

        # these should be equal (in the same order)
        self.assertEqual(pf.all_data(), expected_data)

        # drop the ORDER BY
        future = session.execute_async(
            SimpleStatement("select * from paging_test where id = 1", fetch_size=3, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all(), [3, 3, 3, 1])

        # these should be equal (in the same order)
        self.assertEqual(pf.all_data(), list(reversed(expected_data)))

    def test_with_limit(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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
                self.fail("Invalid scenario configuration. Scenario is: {}".format(scenario))

            pf = PageFetcher(future).request_all()
            self.assertEqual(pf.num_results_all(), scenario['expect_pgsizes'])
            self.assertEqual(pf.pagecount(), scenario['expect_pgcount'])

            # make sure all the data retrieved is a subset of input data
            self.assertIsSubsetOf(pf.all_data(), expected_data)

        run_scenarios(scenarios, handle_scenario, deferred_exceptions=(AssertionError,))

    def test_with_allow_filtering(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
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
        create_rows(data, session, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = session.execute_async(
            SimpleStatement("select * from paging_test where value = 'and more testing' ALLOW FILTERING", fetch_size=4, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [4, 3])

        # make sure the allow filtering query matches the expected results (ignoring order)
        self.assertEqualIgnoreOrder(
            pf.all_data(),
            parse_data_into_dicts(
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
                """, format_funcs={'id': int, 'value': unicode}
            )
        )


@since('2.0')
class TestPagingData(BasePagingTester, PageAssertionMixin):

    def test_paging_a_single_wide_row(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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

        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all(), [3000, 3000, 3000, 1000])

        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11249',
                   flaky=True,
                   notes='windows')
    def test_paging_across_multi_wide_rows(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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

        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all(), [3000, 3000, 3000, 1000])

        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11253',
                   flaky=True,
                   notes='windows')
    def test_paging_using_secondary_indexes(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mybool boolean, sometext text, PRIMARY KEY (id, sometext) )")
        session.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(text):
            return unicode(uuid.uuid4())

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
        expected_data = filter(lambda x: x.get('mybool') is True, all_data)

        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [400, 200])
        self.assertEqualIgnoreOrder(expected_data, pf.all_data())

    def test_paging_with_in_orderby_and_two_partition_keys(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test (col_1 int, col_2 int, col_3 int, PRIMARY KEY ((col_1, col_2), col_3))")

        assert_invalid(session, "select * from paging_test where col_1=1 and col_2 IN (1, 2) order by col_3 desc;", expected=InvalidRequest)
        assert_invalid(session, "select * from paging_test where col_2 IN (1, 2) and col_1=1 order by col_3 desc;", expected=InvalidRequest)

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12068',
                   flaky=True)
    @since('2.0.6')
    def static_columns_paging_test(self):
        """
        Exercises paging with static columns to detect bugs
        @jira_ticket CASSANDRA-8502.
        """

        session = self.prepare()
        self.create_ks(session, 'test_paging_static_cols', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, s1 int static, s2 int static, PRIMARY KEY (a, b))")
        session.row_factory = named_tuple_factory

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
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test" % selector))
                assert_length_equal(results, 16)
                self.assertEqual([0] * 4 + [1] * 4 + [2] * 4 + [3] * 4, sorted([r.a for r in results]))
                self.assertEqual([0, 1, 2, 3] * 4, [r.b for r in results])
                self.assertEqual([0, 1, 2, 3] * 4, [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 16, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 16, [r.s2 for r in results])

        # IN over the partitions
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a IN (0, 1, 2, 3)" % selector))
                assert_length_equal(results, 16)
                self.assertEqual([0] * 4 + [1] * 4 + [2] * 4 + [3] * 4, sorted([r.a for r in results]))
                self.assertEqual([0, 1, 2, 3] * 4, [r.b for r in results])
                self.assertEqual([0, 1, 2, 3] * 4, [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 16, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 16, [r.s2 for r in results])

        # single partition
        for i in range(16):
            session.execute("INSERT INTO test (a, b, c, s1, s2) VALUES (%d, %d, %d, %d, %d)" % (99, i, i, 17, 42))

        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99" % selector))
                assert_length_equal(results, 16)
                self.assertEqual([99] * 16, [r.a for r in results])
                self.assertEqual(range(16), [r.b for r in results])
                self.assertEqual(range(16), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 16, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 16, [r.s2 for r in results])

        # reversed
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 ORDER BY b DESC" % selector))
                assert_length_equal(results, 16)
                self.assertEqual([99] * 16, [r.a for r in results])
                self.assertEqual(list(reversed(range(16))), [r.b for r in results])
                self.assertEqual(list(reversed(range(16))), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 16, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 16, [r.s2 for r in results])

        # IN on clustering column
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15)" % selector))
                assert_length_equal(results, 5)
                self.assertEqual([99] * 5, [r.a for r in results])
                self.assertEqual([3, 4, 8, 14, 15], [r.b for r in results])
                self.assertEqual([3, 4, 8, 14, 15], [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 5, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 5, [r.s2 for r in results])

        # reversed IN on clustering column
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15) ORDER BY b DESC" % selector))
                assert_length_equal(results, 5)
                self.assertEqual([99] * 5, [r.a for r in results])
                self.assertEqual(list(reversed([3, 4, 8, 14, 15])), [r.b for r in results])
                self.assertEqual(list(reversed([3, 4, 8, 14, 15])), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 5, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 5, [r.s2 for r in results])

        # slice on clustering column with set start
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3" % selector))
                assert_length_equal(results, 12)
                self.assertEqual([99] * 12, [r.a for r in results])
                self.assertEqual(range(4, 16), [r.b for r in results])
                self.assertEqual(range(4, 16), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 12, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 12, [r.s2 for r in results])

        # reversed slice on clustering column with set finish
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 ORDER BY b DESC" % selector))
                assert_length_equal(results, 12)
                self.assertEqual([99] * 12, [r.a for r in results])
                self.assertEqual(list(reversed(range(4, 16))), [r.b for r in results])
                self.assertEqual(list(reversed(range(4, 16))), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 12, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 12, [r.s2 for r in results])

        # slice on clustering column with set finish
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b < 14" % selector))
                assert_length_equal(results, 14)
                self.assertEqual([99] * 14, [r.a for r in results])
                self.assertEqual(range(14), [r.b for r in results])
                self.assertEqual(range(14), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 14, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 14, [r.s2 for r in results])

        # reversed slice on clustering column with set start
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b < 14 ORDER BY b DESC" % selector))
                assert_length_equal(results, 14)
                self.assertEqual([99] * 14, [r.a for r in results])
                self.assertEqual(list(reversed(range(14))), [r.b for r in results])
                self.assertEqual(list(reversed(range(14))), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 14, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 14, [r.s2 for r in results])

        # slice on clustering column with start and finish
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14" % selector))
                assert_length_equal(results, 10)
                self.assertEqual([99] * 10, [r.a for r in results])
                self.assertEqual(range(4, 14), [r.b for r in results])
                self.assertEqual(range(4, 14), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 10, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 10, [r.s2 for r in results])

        # reversed slice on clustering column with start and finish
        for page_size in PAGE_SIZES:
            debug("Current page size is %s".format(page_size))
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14 ORDER BY b DESC" % selector))
                assert_length_equal(results, 10)
                self.assertEqual([99] * 10, [r.a for r in results])
                self.assertEqual(list(reversed(range(4, 14))), [r.b for r in results])
                self.assertEqual(list(reversed(range(4, 14))), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 10, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 10, [r.s2 for r in results])

    @since('2.0.6')
    def test_paging_using_secondary_indexes_with_static_cols(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, s1 int static, s2 int static, mybool boolean, sometext text, PRIMARY KEY (id, sometext) )")
        session.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(text):
            return unicode(uuid.uuid4())

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
        expected_data = filter(lambda x: x.get('mybool') is True, all_data)

        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [400, 200])
        self.assertEqualIgnoreOrder(expected_data, pf.all_data())

    def static_columns_with_empty_non_static_columns_paging_test(self):
        """
        @jira_ticket CASSANDRA-10381.
        """

        session = self.prepare()
        self.create_ks(session, 'test_paging_static_cols', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, s int static, PRIMARY KEY (a, b))")
        session.row_factory = named_tuple_factory

        for i in range(10):
            session.execute("UPDATE test SET s = {} WHERE a = {}".format(i, i))

        session.default_fetch_size = 2
        results = list(session.execute("SELECT * FROM test"))
        self.assertEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], sorted([r.s for r in results]))

        results = list(session.execute("SELECT * FROM test WHERE a IN (0, 1, 2, 3, 4)"))
        self.assertEqual([0, 1, 2, 3, 4], sorted([r.s for r in results]))

    @since('3.0.0')
    def test_paging_with_filtering(self):
        """
        @jira_ticket CASSANDRA-6377
        """

        session = self.prepare()
        self.create_ks(session, 'test_paging_with_filtering', 2)
        session.execute("CREATE TABLE test (a int, b int, s int static, c int, d int, primary key (a, b))")
        session.row_factory = tuple_factory

        for i in xrange(5):
            session.execute("INSERT INTO test (a, s) VALUES ({}, {})".format(i, i))
            # Lets a row with only static values
            if i != 2:
                for j in xrange(4):
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
            self.create_ks(session, 'test_flt_counter_columns_compact_storage', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE")
        else:
            self.create_ks(session, 'test_flt_counter_columns', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, cnt counter, PRIMARY KEY (a, b, c))")

        for i in xrange(5):
            for j in xrange(10):
                session.execute("UPDATE test SET cnt = cnt + {} WHERE a={} AND b={} AND c={}".format(j + 2, i, j, j + 1))

        for page_size in (2, 3, 4, 5, 7, 10, 20):
            session.default_fetch_size = page_size

            # single partition
            assert_all(session, "SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt > 8 ALLOW FILTERING", [[4, 7, 8, 9],
                                                                                                                   [4, 8, 9, 10],
                                                                                                                   [4, 9, 10, 11]])

            assert_all(session, "SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt >= 8 ALLOW FILTERING", [[4, 6, 7, 8],
                                                                                                                    [4, 7, 8, 9],
                                                                                                                    [4, 8, 9, 10],
                                                                                                                    [4, 9, 10, 11]])

            assert_all(session, "SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt >= 8 AND cnt < 10 ALLOW FILTERING", [[4, 6, 7, 8],
                                                                                                                                 [4, 7, 8, 9]])

            assert_all(session, "SELECT * FROM test WHERE a = 4 AND b > 3 AND c > 3 AND cnt >= 8 AND cnt <= 10 ALLOW FILTERING", [[4, 6, 7, 8],
                                                                                                                                  [4, 7, 8, 9],
                                                                                                                                  [4, 8, 9, 10]])

            assert_all(session, "SELECT * FROM test WHERE cnt = 5 ALLOW FILTERING", [[1, 3, 4, 5], [0, 3, 4, 5], [2, 3, 4, 5], [4, 3, 4, 5], [3, 3, 4, 5]])

            assert_all(session, "SELECT * FROM test WHERE a IN (1,2,3) AND cnt = 5 ALLOW FILTERING", [[1, 3, 4, 5],
                                                                                                      [2, 3, 4, 5],
                                                                                                      [3, 3, 4, 5]])

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12025',
                   flaky=True)
    @since('3.6')
    def test_paging_with_filtering_on_counter_columns(self):
        """
        test paging, when filtering on counter columns
        @jira_ticket CASSANDRA-11629
        """

        session = self.prepare()
        session.row_factory = tuple_factory

        self._test_paging_with_filtering_on_counter_columns(session, False)
        self._test_paging_with_filtering_on_counter_columns(session, True)

    def _test_paging_with_filtering_on_clustering_columns(self, session, with_compact_storage):
        if with_compact_storage:
            self.create_ks(session, 'test_flt_clustering_columns_compact_storage', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c)) WITH COMPACT STORAGE")
        else:
            self.create_ks(session, 'test_flt_clustering_columns', 2)
            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c))")

        session.row_factory = tuple_factory

        for i in xrange(5):
            for j in xrange(10):
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

        session = self.prepare()
        self._test_paging_with_filtering_on_clustering_columns(session, False)
        self._test_paging_with_filtering_on_clustering_columns(session, True)

    @since('3.6')
    def test_paging_with_filtering_on_clustering_columns_with_contains(self):
        """
        test paging, when filtering on clustering columns (frozen collections) with CONTAINS statement
        @jira_ticket CASSANDRA-11310
        """

        session = self.prepare()
        self.create_ks(session, 'test_paging_flt_clustering_clm_contains', 2)
        session.execute("CREATE TABLE test_list (a int, b int, c frozen<list<int>>, d int, PRIMARY KEY (a, b, c))")
        session.execute("CREATE TABLE test_map (a int, b int, c frozen<map<int, int>>, d int, PRIMARY KEY (a, b, c))")
        session.row_factory = tuple_factory

        for i in xrange(5):
            for j in xrange(10):
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

        session = self.prepare()
        self.create_ks(session, 'test_paging_with_filtering_on_static_columns', 2)
        session.execute("CREATE TABLE test (a int, b int, s int static, d int, PRIMARY KEY (a, b))")
        session.row_factory = tuple_factory

        for i in xrange(5):
            for j in xrange(10):
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

    @since('2.1.14')
    def test_paging_on_compact_table_with_tombstone_on_first_column(self):
        """
        test paging, on  COMPACT tables without clustering columns, when the first column has a tombstone
        @jira_ticket CASSANDRA-11467
        """

        session = self.prepare()
        self.create_ks(session, 'test_paging_on_compact_table_with_tombstone', 2)
        session.execute("CREATE TABLE test (a int primary key, b int, c int) WITH COMPACT STORAGE")
        session.row_factory = tuple_factory

        for i in xrange(5):
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

        session = self.prepare()
        self.create_ks(session, 'test_paging_with_no_clustering_columns', 2)
        session.execute("CREATE TABLE test (a int primary key, b int)")
        session.execute("CREATE TABLE test_compact (a int primary key, b int) WITH COMPACT STORAGE")
        session.row_factory = tuple_factory

        for table in ('test', 'test_compact'):

            for i in xrange(5):
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
        session = self.prepare()
        self.create_ks(session, 'test_paging_with_per_partition_limit', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, PRIMARY KEY (a, b))")
        session.row_factory = tuple_factory

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
                self.assertTrue(row[0] in set(range(5)))
                self.assertTrue(row[1] in set(range(2)))
                self.assertTrue(row[1] in set(range(2)))

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

        session = self.prepare()
        self.create_ks(session, 'test_paging_for_range_name_queries', 2)
        session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY(a, b, c))")
        session.execute("CREATE TABLE test_compact (a int, b int, c int, d int, PRIMARY KEY(a, b, c)) WITH COMPACT STORAGE")
        session.row_factory = tuple_factory

        for table in ('test', 'test_compact'):

            for i in xrange(4):
                for j in xrange(4):
                    for k in xrange(4):
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


@since('2.0')
class TestPagingDatasetChanges(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when the queried dataset changes while pages are being retrieved.
    """

    def test_data_change_impacting_earlier_page(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [501, 499])

        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    def test_data_change_impacting_later_page(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [500, 500])

        # add the new row to the expected data and then do a compare
        expected_data.append({u'id': 2, u'mytext': u'foo'})
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    def test_row_TTL_expiry_during_paging(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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
        self.assertEqual(pf.pagecount(), 3)
        self.assertEqual(pf.num_results_all(), [300, 300, 200])

    def test_cell_TTL_expiry_during_paging(self):
        session = self.prepare()
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("""
            CREATE TABLE paging_test (
                id int,
                mytext text,
                somevalue text,
                anothervalue text,
                PRIMARY KEY (id, mytext) )
            """)

        def random_txt(text):
            return unicode(uuid.uuid4())

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
        self.assertEqualIgnoreOrder(page1, data[:500])

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
        self.assertEqualIgnoreOrder(page2, data[500:1000])

        page3expected = []
        for row in data[1000:1500]:
            _id, mytext = row['id'], row['mytext']
            page3expected.append(
                {u'id': _id, u'mytext': mytext, u'somevalue': None, u'anothervalue': None}
            )

        time.sleep(15)

        pf.request_one()
        page3 = pf.page_data(3)
        self.assertEqualIgnoreOrder(page3, page3expected)

    def test_node_unavailabe_during_paging(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.cql_connection(node1)
        self.create_ks(session, 'test_paging_size', 1)
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
        with self.assertRaisesRegexp(RuntimeError, 'Requested pages were not delivered before timeout'):
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
        self.create_ks(session, 'test_paging_size', 2)
        session.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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

        self.assertEqual(page_fetchers[0].pagecount(), 10)
        self.assertEqual(page_fetchers[1].pagecount(), 9)
        self.assertEqual(page_fetchers[2].pagecount(), 8)
        self.assertEqual(page_fetchers[3].pagecount(), 7)
        self.assertEqual(page_fetchers[4].pagecount(), 6)
        self.assertEqual(page_fetchers[5].pagecount(), 5)
        self.assertEqual(page_fetchers[6].pagecount(), 5)
        self.assertEqual(page_fetchers[7].pagecount(), 5)
        self.assertEqual(page_fetchers[8].pagecount(), 4)
        self.assertEqual(page_fetchers[9].pagecount(), 4)
        self.assertEqual(page_fetchers[10].pagecount(), 34)

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

        self.create_ks(self.session, 'test_paging_size', 2)
        self.session.execute("CREATE TABLE paging_test ( "
                             "id int, mytext text, col1 int, col2 int, col3 int, "
                             "PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

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
        pf.request_all()
        self.assertEqual(pf.pagecount(), pagecount)
        self.assertEqual(pf.num_results_all(), num_page_results)

        for i in range(pf.pagecount()):
            page_data = pf.page_data(i + 1)
            self.assertEquals(page_data, expected_pages_data[i])

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

    def test_multiple_row_deletions(self):
        """Test multiple row deletions.
           This test should be finished when CASSANDRA-6237 is done.
        """
        self.skipTest("Feature In Development")
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
        for row in data:
            s = ("insert into paging_test (id, mytext, col1, col2, col3) "
                 "values ({}, '{}', {}, {}, {}) using ttl 3;").format(
                row['id'], row['mytext'], row['col1'],
                row['col2'], row['col3'])
            self.session.execute(
                SimpleStatement(s, consistency_level=CL.ALL)
            )
        self.check_all_paging_results(data, 8,
                                      [25, 25, 25, 25, 25, 25, 25, 25])
        time.sleep(5)
        self.check_all_paging_results([], 0, [])

    def test_failure_threshold_deletions(self):
        """Test that paging throws a failure in case of tombstone threshold """
        self.allow_log_errors = True
        self.cluster.set_configuration_options(
            values={'tombstone_failure_threshold': 500}
        )
        self.session = self.prepare()
        self.setup_data()

        # Add more data
        values = map(lambda i: uuid.uuid4(), range(3000))
        for value in values:
            self.session.execute(SimpleStatement(
                "insert into paging_test (id, mytext, col1) values (1, '{}', null) ".format(
                    value
                ),
                consistency_level=CL.ALL
            ))

        assert_invalid(self.session,
                       SimpleStatement("select * from paging_test", fetch_size=1000, consistency_level=CL.ALL, retry_policy=FallthroughRetryPolicy()),
                       expected=ReadTimeout if self.cluster.version() < '2.2' else ReadFailure)

        if self.cluster.version() < "3.0":
            failure_msg = ("Scanned over.* tombstones in test_paging_size."
                           "paging_test.* query aborted")
        else:
            failure_msg = ("Scanned over.* tombstones during query.* query aborted")

        self.wait_for_any_log(self.cluster.nodelist(), failure_msg, 25)

    @since('2.2.6')
    def test_deletion_with_distinct_paging(self):
        """
        Test that deletion does not affect paging for distinct queries.

        @jira_ticket CASSANDRA-10010
        """
        self.session = self.prepare()
        self.create_ks(self.session, 'test_paging_size', 2)
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
            self.assertEqual(4, len(result))

            future = self.session.execute_async("SELECT DISTINCT k, s FROM paging_test {}".format(whereClause))

            # this will fetch the first page
            fetcher = PageFetcher(future)

            # delete the first row in the last partition that was returned in the first page
            self.session.execute("DELETE FROM paging_test WHERE k = %s AND c = %s", (result[1]['k'], 0))

            # finish paging
            fetcher.request_all()
            self.assertEqual([2, 2], fetcher.num_results_all())
