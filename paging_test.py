import time
import uuid

from cassandra import ConsistencyLevel as CL
from cassandra import InvalidRequest, ReadTimeout, ReadFailure
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement, dict_factory, named_tuple_factory

from assertions import assert_invalid
from datahelp import create_rows, flatten_into_set, parse_data_into_dicts
from dtest import Tester, run_scenarios
from tools import known_failure, since


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
            "Requested: %d; retrieved: %d; empty retreived: %d" %
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
        assert flatten_into_set(subset).issubset(flatten_into_set(superset))


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
                assert False

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

        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test" % selector))
                self.assertEqual(16, len(results))
                self.assertEqual([0] * 4 + [1] * 4 + [2] * 4 + [3] * 4, sorted([r.a for r in results]))
                self.assertEqual([0, 1, 2, 3] * 4, [r.b for r in results])
                self.assertEqual([0, 1, 2, 3] * 4, [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 16, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 16, [r.s2 for r in results])

        # IN over the partitions
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a IN (0, 1, 2, 3)" % selector))
                self.assertEqual(16, len(results))
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

        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99" % selector))
                self.assertEqual(16, len(results))
                self.assertEqual([99] * 16, [r.a for r in results])
                self.assertEqual(range(16), [r.b for r in results])
                self.assertEqual(range(16), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 16, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 16, [r.s2 for r in results])

        # reversed
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 ORDER BY b DESC" % selector))
                self.assertEqual(16, len(results))
                self.assertEqual([99] * 16, [r.a for r in results])
                self.assertEqual(list(reversed(range(16))), [r.b for r in results])
                self.assertEqual(list(reversed(range(16))), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 16, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 16, [r.s2 for r in results])

        # IN on clustering column
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15)" % selector))
                self.assertEqual(5, len(results))
                self.assertEqual([99] * 5, [r.a for r in results])
                self.assertEqual([3, 4, 8, 14, 15], [r.b for r in results])
                self.assertEqual([3, 4, 8, 14, 15], [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 5, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 5, [r.s2 for r in results])

        # reversed IN on clustering column
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b IN (3, 4, 8, 14, 15) ORDER BY b DESC" % selector))
                self.assertEqual(5, len(results))
                self.assertEqual([99] * 5, [r.a for r in results])
                self.assertEqual(list(reversed([3, 4, 8, 14, 15])), [r.b for r in results])
                self.assertEqual(list(reversed([3, 4, 8, 14, 15])), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 5, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 5, [r.s2 for r in results])

        # slice on clustering column with set start
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3" % selector))
                self.assertEqual(12, len(results))
                self.assertEqual([99] * 12, [r.a for r in results])
                self.assertEqual(range(4, 16), [r.b for r in results])
                self.assertEqual(range(4, 16), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 12, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 12, [r.s2 for r in results])

        # reversed slice on clustering column with set finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 ORDER BY b DESC" % selector))
                self.assertEqual(12, len(results))
                self.assertEqual([99] * 12, [r.a for r in results])
                self.assertEqual(list(reversed(range(4, 16))), [r.b for r in results])
                self.assertEqual(list(reversed(range(4, 16))), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 12, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 12, [r.s2 for r in results])

        # slice on clustering column with set finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b < 14" % selector))
                self.assertEqual(14, len(results))
                self.assertEqual([99] * 14, [r.a for r in results])
                self.assertEqual(range(14), [r.b for r in results])
                self.assertEqual(range(14), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 14, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 14, [r.s2 for r in results])

        # reversed slice on clustering column with set start
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b < 14 ORDER BY b DESC" % selector))
                self.assertEqual(14, len(results))
                self.assertEqual([99] * 14, [r.a for r in results])
                self.assertEqual(list(reversed(range(14))), [r.b for r in results])
                self.assertEqual(list(reversed(range(14))), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 14, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 14, [r.s2 for r in results])

        # slice on clustering column with start and finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14" % selector))
                self.assertEqual(10, len(results))
                self.assertEqual([99] * 10, [r.a for r in results])
                self.assertEqual(range(4, 14), [r.b for r in results])
                self.assertEqual(range(4, 14), [r.c for r in results])
                if "s1" in selector:
                    self.assertEqual([17] * 10, [r.s1 for r in results])
                if "s2" in selector:
                    self.assertEqual([42] * 10, [r.s2 for r in results])

        # reversed slice on clustering column with start and finish
        for page_size in (2, 3, 4, 5, 15, 16, 17, 100):
            session.default_fetch_size = page_size
            for selector in selectors:
                results = list(session.execute("SELECT %s FROM test WHERE a = 99 AND b > 3 AND b < 14 ORDER BY b DESC" % selector))
                self.assertEqual(10, len(results))
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

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-10869',
                   notes='Fails on 2.1')
    def test_failure_threshold_deletions(self):
        """Test that paging throws a failure in case of tombstone threshold """
        self.allow_log_errors = True
        self.cluster.set_configuration_options(
            values={'tombstone_failure_threshold': 500}
        )
        self.session = self.prepare()
        node1, node2, node3 = self.cluster.nodelist()

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
        failure = (node1.grep_log(failure_msg) or
                   node2.grep_log(failure_msg) or
                   node3.grep_log(failure_msg))

        self.assertTrue(failure, "Cannot find tombstone failure threshold error in log")

    def test_deletion_with_distinct_paging(self):
        """Test that deletion does not affect paging for distinct queries"""
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