import time
import uuid

from cassandra import ConsistencyLevel as CL
from cassandra import InvalidRequest
from cassandra.query import SimpleStatement, dict_factory
from dtest import Tester, run_scenarios
from pytools import since

from datahelp import create_rows, parse_data_into_dicts, flatten_into_set


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

        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error
        )
        # the first page is automagically returned (eventually)
        # so we'll count this as a request, but the retrieved count
        # won't be incremented until it actually arrives
        self.requested_pages = 1
        self.retrieved_pages = 0
        self.retrieved_empty_pages = 0

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

        raise RuntimeError("Requested pages were not delivered before timeout.")

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
        node1, node2, node3 = cluster.nodelist()
        cursor = self.cql_connection(node1)
        cursor.row_factory = dict_factory
        return cursor


class TestPagingSize(BasePagingTester, PageAssertionMixin):
    """
    Basic tests relating to page size (relative to results set)
    and validation of page size setting.
    """

    @since('2.0')
    def test_with_no_results(self):
        """
        No errors when a page is requested and query has no results.
        """
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        # run a query that has no results and make sure it's exhausted
        future = cursor.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        pf.request_all()
        self.assertEqual([], pf.all_data())
        self.assertFalse(pf.has_more_pages)

    @since('2.0')
    def test_with_less_results_than_page_size(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=100, consistency_level=CL.ALL)
        )
        pf = PageFetcher(future)
        pf.request_all()

        self.assertFalse(pf.has_more_pages)
        self.assertEqual(len(expected_data), len(pf.all_data()))

    @since('2.0')
    def test_with_more_results_than_page_size(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
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
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [5, 4])

        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @since('2.0')
    def test_with_equal_results_to_page_size(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int PRIMARY KEY, value text )")

        data = """
            |id| value          |
            |1 |testing         |
            |2 |and more testing|
            |3 |and more testing|
            |4 |and more testing|
            |5 |and more testing|
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test", fetch_size=5, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.num_results_all(), [5])
        self.assertEqual(pf.pagecount(), 1)

        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @since('2.0')
    def test_undefined_page_size_default(self):
        """
        If the page size isn't sent then the default fetch size is used.
        """
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id uuid PRIMARY KEY, value text )")

        def random_txt(text):
            return uuid.uuid4()

        data = """
               | id     |value   |
          *5001| [uuid] |testing |
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': random_txt, 'value': unicode})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test", consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.num_results_all(), [5000, 1])

        # make sure expected and actual have same data elements (ignoring order)
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)


class TestPagingWithModifiers(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when CQL modifiers (such as order, limit, allow filtering) are used.
    """
    @since('2.0')
    def test_with_order_by(self):
        """"
        Paging over a single partition with ordering should work.
        (Spanning multiple partitions won't though, by design. See CASSANDRA-6722).
        """
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging', 2)
        cursor.execute(
            """
            CREATE TABLE paging_test (
                id int,
                value text,
                PRIMARY KEY (id, value)
            ) WITH CLUSTERING ORDER BY (value ASC)
            """)

        data = """
            |id|value|
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

        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = cursor.execute_async(
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
            cursor.execute(stmt)

    @since('2.0')
    def test_with_limit(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        data = """
               | id | value         |
             *5| 1  | [random text] |
             *5| 2  | [random text] |
            *10| 3  | [random text] |
            *10| 4  | [random text] |
            *20| 5  | [random text] |
            *30| 6  | [random text] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': random_txt})

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
        ]

        def handle_scenario(scenario):
            future = cursor.execute_async(
                SimpleStatement(
                    "select * from paging_test {} limit {}".format(scenario['whereclause'], scenario['limit']),
                    fetch_size=scenario['fetch'], consistency_level=CL.ALL)
            )

            pf = PageFetcher(future).request_all()
            self.assertEqual(pf.num_results_all(), scenario['expect_pgsizes'])
            self.assertEqual(pf.pagecount(), scenario['expect_pgcount'])

            # make sure all the data retrieved is a subset of input data
            self.assertIsSubsetOf(pf.all_data(), expected_data)

        run_scenarios(scenarios, handle_scenario, deferred_exceptions=(AssertionError,))

    @since('2.0')
    def test_with_allow_filtering(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        data = """
            |id|value           |
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
        create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': unicode})

        future = cursor.execute_async(
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


class TestPagingData(BasePagingTester, PageAssertionMixin):
    @since('2.0')
    def test_paging_a_single_wide_row(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        data = """
              | id | value                  |
        *10000| 1  | [replaced with random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': random_txt})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where id = 1", fetch_size=3000, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all(), [3000, 3000, 3000, 1000])

        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @since('2.0')
    def test_paging_across_multi_wide_rows(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, value text, PRIMARY KEY (id, value) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        data = """
              | id | value                  |
         *5000| 1  | [replaced with random] |
         *5000| 2  | [replaced with random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'value': random_txt})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=3000, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        self.assertEqual(pf.pagecount(), 4)
        self.assertEqual(pf.num_results_all(), [3000, 3000, 3000, 1000])

        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @since('2.0')
    def test_paging_using_secondary_indexes(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mybool boolean, sometext text, PRIMARY KEY (id, sometext) )")
        cursor.execute("CREATE INDEX ON paging_test(mybool)")

        def random_txt(text):
            return unicode(uuid.uuid4())

        def bool_from_str_int(text):
            return bool(int(text))

        data = """
             | id | mybool| sometext |
         *100| 1  | 1     | [random] |
         *300| 2  | 0     | [random] |
         *500| 3  | 1     | [random] |
         *400| 4  | 0     | [random] |
            """
        all_data = create_rows(
            data, cursor, 'paging_test', cl=CL.ALL,
            format_funcs={'id': int, 'mybool': bool_from_str_int, 'sometext': random_txt}
        )

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where mybool = true", fetch_size=400, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future).request_all()

        # the query only searched for True rows, so let's pare down the expectations for comparison
        expected_data = filter(lambda x: x.get('mybool') is True, all_data)

        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [400, 200])
        self.assertEqualIgnoreOrder(expected_data, pf.all_data())


class TestPagingDatasetChanges(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with paging when the queried dataset changes while pages are being retrieved.
    """
    @since('2.0')
    def test_data_change_impacting_earlier_page(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        data = """
              | id | mytext   |
          *500| 1  | [random] |
          *500| 2  | [random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt})

        # get 501 rows so we have definitely got the 1st row of the second partition
        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=501, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # we got one page and should be done with the first partition (for id=1)
        # let's add another row for that first partition (id=1) and make sure it won't sneak into results
        cursor.execute(SimpleStatement("insert into paging_test (id, mytext) values (1, 'foo')", consistency_level=CL.ALL))

        pf.request_all()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [501, 499])

        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @since('2.0')
    def test_data_change_impacting_later_page(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        data = """
              | id | mytext   |
          *500| 1  | [random] |
          *499| 2  | [random] |
            """
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=500, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # we've already paged the first partition, but adding a row for the second (id=2)
        # should still result in the row being seen on the subsequent pages
        cursor.execute(SimpleStatement("insert into paging_test (id, mytext) values (2, 'foo')", consistency_level=CL.ALL))

        pf.request_all()
        self.assertEqual(pf.pagecount(), 2)
        self.assertEqual(pf.num_results_all(), [500, 500])

        # add the new row to the expected data and then do a compare
        expected_data.append({u'id': 2, u'mytext': u'foo'})
        self.assertEqualIgnoreOrder(pf.all_data(), expected_data)

    @since('2.0')
    def test_data_delete_removing_remainder(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        data = """
              | id | mytext   |
          *500| 1  | [random] |
          *500| 2  | [random] |
            """

        create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt})

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where id in (1,2)", fetch_size=500, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # delete the results that would have shown up on page 2
        cursor.execute(SimpleStatement("delete from paging_test where id = 2", consistency_level=CL.ALL))

        pf.request_all()
        self.assertEqual(pf.pagecount(), 1)
        self.assertEqual(pf.num_results_all(), [500])

    @since('2.0')
    def test_row_TTL_expiry_during_paging(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        # create rows with TTL (some of which we'll try to get after expiry)
        create_rows(
            """
                | id | mytext   |
            *300| 1  | [random] |
            *400| 2  | [random] |
            """,
            cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}, postfix='USING TTL 10'
        )

        # create rows without TTL
        create_rows(
            """
                | id | mytext   |
            *500| 3  | [random] |
            """,
            cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}
        )

        future = cursor.execute_async(
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

    @since('2.0')
    def test_cell_TTL_expiry_during_paging(self):
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("""
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
            *500| 1  | [random] | foo       |  bar         |
            *500| 2  | [random] | foo       |  bar         |
            *500| 3  | [random] | foo       |  bar         |
            """,
            cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt}
        )

        future = cursor.execute_async(
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
            cursor.execute(stmt)

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

    @since('2.0')
    def test_node_unavailabe_during_paging(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        cursor = self.cql_connection(node1)
        self.create_ks(cursor, 'test_paging_size', 1)
        cursor.execute("CREATE TABLE paging_test ( id uuid, mytext text, PRIMARY KEY (id, mytext) )")

        def make_uuid(text):
            return uuid.uuid4()

        create_rows(
            """
                  | id      | mytext |
            *10000| [uuid]  | foo    |
            """,
            cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': make_uuid}
        )

        future = cursor.execute_async(
            SimpleStatement("select * from paging_test where mytext = 'foo' allow filtering", fetch_size=2000, consistency_level=CL.ALL)
        )

        pf = PageFetcher(future)
        # no need to request page here, because the first page is automatically retrieved

        # stop a node and make sure we get an error trying to page the rest
        node1.stop()
        with self.assertRaisesRegexp(RuntimeError, 'Requested pages were not delivered before timeout'):
            pf.request_all()

        # TODO: can we resume the node and expect to get more results from the result set or is it done?


class TestPagingQueryIsolation(BasePagingTester, PageAssertionMixin):
    """
    Tests concerned with isolation of paged queries (queries can't affect each other).
    """
    @since('2.0')
    def test_query_isolation(self):
        """
        Interleave some paged queries and make sure nothing bad happens.
        """
        cursor = self.prepare()
        self.create_ks(cursor, 'test_paging_size', 2)
        cursor.execute("CREATE TABLE paging_test ( id int, mytext text, PRIMARY KEY (id, mytext) )")

        def random_txt(text):
            return unicode(uuid.uuid4())

        data = """
               | id | mytext   |
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
        expected_data = create_rows(data, cursor, 'paging_test', cl=CL.ALL, format_funcs={'id': int, 'mytext': random_txt})

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
            future = cursor.execute_async(stmt)
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

        self.assertEqualIgnoreOrder(page_fetchers[0].all_data(), expected_data[:5000])
        self.assertEqualIgnoreOrder(page_fetchers[1].all_data(), expected_data[5000:10000])
        self.assertEqualIgnoreOrder(page_fetchers[2].all_data(), expected_data[10000:15000])
        self.assertEqualIgnoreOrder(page_fetchers[3].all_data(), expected_data[15000:20000])
        self.assertEqualIgnoreOrder(page_fetchers[4].all_data(), expected_data[20000:25000])
        self.assertEqualIgnoreOrder(page_fetchers[5].all_data(), expected_data[:5000])
        self.assertEqualIgnoreOrder(page_fetchers[6].all_data(), expected_data[5000:10000])
        self.assertEqualIgnoreOrder(page_fetchers[7].all_data(), expected_data[10000:15000])
        self.assertEqualIgnoreOrder(page_fetchers[8].all_data(), expected_data[15000:20000])
        self.assertEqualIgnoreOrder(page_fetchers[9].all_data(), expected_data[20000:25000])
        self.assertEqualIgnoreOrder(page_fetchers[10].all_data(), expected_data[:50000])
