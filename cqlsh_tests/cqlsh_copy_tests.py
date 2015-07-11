# coding: utf-8
import csv
import datetime
import os
import sys
from contextlib import contextmanager
from decimal import Decimal
from tempfile import NamedTemporaryFile
from uuid import uuid1, uuid4

from cassandra.concurrent import execute_concurrent_with_args

from cqlsh_tools import (DummyColorMap, assert_csvs_items_equal, csv_rows,
                         monkeypatch_driver, random_list,
                         strip_timezone_if_time_string, unmonkeypatch_driver,
                         write_rows_to_csv)
from dtest import Tester, canReuseCluster, debug
from tools import rows_to_list, since

DEFAULT_FLOAT_PRECISION = 5  # magic number copied from cqlsh script
DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'  # based on cqlsh script; timezone stripped

@canReuseCluster
@since('2.1')  # version differences break formatting code on 2.0.x
class CqlshCopyTest(Tester):
    """
    Tests the COPY TO and COPY FROM features in cqlsh.
    @jira_ticket CASSANDRA-3906
    """
    @classmethod
    def setUpClass(cls):
        cls._cached_driver_methods = monkeypatch_driver()

    @classmethod
    def tearDownClass(cls):
        unmonkeypatch_driver(cls._cached_driver_methods)

    def tearDown(self):
        if self.tempfile:
            os.unlink(self.tempfile.name)
            super(CqlshCopyTest, self).tearDown()

    def prepare(self):
        if not self.cluster.nodelist():
            self.cluster.populate(1).start(wait_for_binary_proto=True)
        self.node1, = self.cluster.nodelist()
        self.session = self.patient_cql_connection(self.node1)

        self.session.execute('DROP KEYSPACE IF EXISTS ks')
        self.create_ks(self.session, 'ks', 1)

    def all_datatypes_prepare(self):
        self.prepare()
        self.session.execute('''
            CREATE TABLE testdatatype (
                a ascii PRIMARY KEY,
                b bigint,
                c blob,
                d boolean,
                e decimal,
                f double,
                g float,
                h inet,
                i int,
                j text,
                k timestamp,
                l timeuuid,
                m uuid,
                n varchar,
                o varint
            )''')

        self.data = ('ascii',  # a ascii
                     2 ** 40,  # b bigint
                     '0xbeef',  # c blob
                     True,  # d boolean
                     Decimal(3.14),  # e decimal
                     2.444,  # f double
                     1.1,  # g float
                     '127.0.0.1',  # h inet
                     25,  # i int
                     'ヽ(´ー｀)ノ',  # j text
                     datetime.datetime(2005, 7, 14, 12, 30),  # k timestamp
                     uuid1(),  # l timeuuid
                     uuid4(),  # m uuid
                     'asdf',  # n varchar
                     2 ** 65  # o varint
                     )

    @contextmanager
    def _cqlshlib(self):
        """
        Returns the cqlshlib module, as defined in self.cluster's first node.
        """
        # This method accomplishes its goal by manually adding the library to
        # sys.path, returning the module, then restoring the old path once the
        # context manager exits. This isn't great for maintainability and should
        # be replaced if cqlshlib is made easier to interact with.
        saved_path = list(sys.path)
        cassandra_dir = self.cluster.nodelist()[0].get_install_dir()

        try:
            sys.path = sys.path + [os.path.join(cassandra_dir, 'pylib')]
            import cqlshlib
            yield cqlshlib
        finally:
            sys.path = saved_path

    def assertCsvResultEqual(self, csv_filename, results):
        result_list = list(self.result_to_csv_rows(results))
        processed_results = [[strip_timezone_if_time_string(v) for v in row]
                             for row in result_list]

        csv_file = list(csv_rows(csv_filename))
        processed_csv = [[strip_timezone_if_time_string(v) for v in row]
                         for row in csv_file]

        self.maxDiff = None
        self.assertItemsEqual(processed_csv,
                              processed_results)

    def format_for_csv(self, val):
        with self._cqlshlib() as cqlshlib:
            from cqlshlib.formatting import format_value
            try:
                from cqlshlib.formatting import DateTimeFormat
                date_time_format = DateTimeFormat()
            except ImportError:
                date_time_format = None
            # try:
            #     from cqlshlib.formatting
        encoding_name = 'utf-8' #codecs.lookup(locale.getpreferredencoding()).name

        # different versions use time_format or date_time_format
        # but all versions reject spurious values, so we just use both
        # here
        return format_value(type(val), val,
                            encoding=encoding_name,
                            date_time_format=date_time_format,
                            time_format=DEFAULT_TIME_FORMAT,
                            float_precision=DEFAULT_FLOAT_PRECISION,
                            colormap=DummyColorMap(),
                            nullval=None).strval

    def result_to_csv_rows(self, result):
        """
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        """
        # This has no real dependencies on Tester except that self._cqlshlib has
        # to grab self.cluster's install directory. This should be pulled out
        # into a bare function if cqlshlib is made easier to interact with.
        return [[self.format_for_csv(v) for v in row] for row in result]

    def test_list_data(self):
        """
        Tests the COPY TO command with the list datatype by:

        - populating a table with lists of uuids,
        - exporting the table to a CSV file with COPY TO,
        - comparing the CSV file to the SELECTed contents of the table.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testlist (
                a int PRIMARY KEY,
                b list<uuid>
            )""")

        insert_statement = self.session.prepare("INSERT INTO testlist (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testlist"))

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testlist TO '{name}'".format(name=self.tempfile.name))

        self.assertCsvResultEqual(self.tempfile.name, results)

    def test_tuple_data(self):
        """
        Tests the COPY TO command with the tuple datatype by:

        - populating a table with tuples of uuids,
        - exporting the table to a CSV file with COPY TO,
        - comparing the CSV file to the SELECTed contents of the table.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testtuple (
                a int primary key,
                b tuple<uuid, uuid, uuid>
            )""")

        insert_statement = self.session.prepare("INSERT INTO testtuple (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4, n=3)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testtuple"))

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testtuple TO '{name}'".format(name=self.tempfile.name))

        self.assertCsvResultEqual(self.tempfile.name, results)

    def non_default_delimiter_template(self, delimiter):
        """
        @param delimiter the delimiter to use for the CSV file.

        Test exporting to CSV files using delimiters other than ',' by:

        - populating a table with integers,
        - exporting to a CSV file, specifying a delimiter, then
        - comparing the contents of the csv file to the SELECTed contents of the table.
        """

        self.prepare()
        self.session.execute("""
            CREATE TABLE testdelimiter (
                a int primary key
            )""")
        insert_statement = self.session.prepare("INSERT INTO testdelimiter (a) VALUES (?)")
        args = [(i,) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testdelimiter"))

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        cmds = "COPY ks.testdelimiter TO '{name}'".format(name=self.tempfile.name)
        cmds += " WITH DELIMITER = '{d}'".format(d=delimiter)
        self.node1.run_cqlsh(cmds=cmds)

        self.assertCsvResultEqual(self.tempfile.name, results)

    def test_colon_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter ':'.
        """
        self.non_default_delimiter_template(':')

    def test_letter_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter 'a'.
        """
        self.non_default_delimiter_template('a')

    def test_number_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter '1'.
        """
        self.non_default_delimiter_template('1')

    def custom_null_indicator_template(self, indicator):
        """
        @param indicator the null indicator to be used in COPY

        A parametrized test that tests COPY with a given null indicator.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testnullindicator (
                a int primary key,
                b text
            )""")
        insert_non_null = self.session.prepare("INSERT INTO testnullindicator (a, b) VALUES (?, ?)")
        execute_concurrent_with_args(self.session, insert_non_null,
                                     [(1, 'eggs'), (100, 'sausage')])
        insert_null = self.session.prepare("INSERT INTO testnullindicator (a) VALUES (?)")
        execute_concurrent_with_args(self.session, insert_null, [(2,), (200,)])

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        cmds = "COPY ks.testnullindicator TO '{name}'".format(name=self.tempfile.name)
        cmds += " WITH NULL = '{d}'".format(d=indicator)
        self.node1.run_cqlsh(cmds=cmds)

        results = list(self.session.execute("SELECT a, b FROM ks.testnullindicator"))
        results = [[indicator if value is None else value for value in row]
                   for row in results]

        self.assertCsvResultEqual(self.tempfile.name, results)

    def test_undefined_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = undefined.
        """
        self.custom_null_indicator_template('undefined')

    def test_null_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = 'null'.
        """
        self.custom_null_indicator_template('null')

    def test_writing_use_header(self):
        """
        Test that COPY can write a CSV with a header by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY WITH
        HEADER = true
        - checking that the contents of the CSV file are the written values plus
        the header.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testheader (
                a int primary key,
                b int
            )""")
        insert_statement = self.session.prepare("INSERT INTO testheader (a, b) VALUES (?, ?)")
        args = [(1, 10), (2, 20), (3, 30)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        cmds = "COPY ks.testheader TO '{name}'".format(name=self.tempfile.name)
        cmds += " WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        with open(self.tempfile.name, 'r') as csvfile:
            csv_values = list(csv.reader(csvfile))

        self.assertItemsEqual(csv_values,
                                 [['a', 'b'], ['1', '10'], ['2', '20'], ['3', '30']])

    def test_reading_use_header(self):
        """
        Test that COPY can read a CSV with a header by:

        - creating a table,
        - writing a CSV with a header,
        - importing the contents of the CSV file using COPY WITH HEADER = true,
        - checking that the contents of the table are the written values.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testheader (
                a int primary key,
                b int
            )""")

        self.tempfile = NamedTemporaryFile(delete=False)

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(self.tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            writer.writeheader()
            for a, b in data:
                writer.writerow({'a': a, 'b': b})
            csvfile.close

        cmds = "COPY ks.testheader FROM '{name}'".format(name=self.tempfile.name)
        cmds += " WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        result = self.session.execute("SELECT * FROM testheader")
        self.assertItemsEqual([tuple(d) for d in data],
                              [tuple(r) for r in rows_to_list(result)])

    def test_explicit_column_order_writing(self):
        """
        Test that COPY can write to a CSV file when the order of columns is
        explicitly specified by:

        - creating a table,
        - COPYing to a CSV file with columns in a different order than they
        appeared in the CREATE TABLE statement,
        - writing a CSV file with the columns in that order, and
        - asserting that the two CSV files contain the same values.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testorder (
                a int primary key,
                b int,
                c text
            )""")

        data = [[1, 20, 'ham'], [2, 40, 'eggs'],
                [3, 60, 'beans'], [4, 80, 'toast']]
        insert_statement = self.session.prepare("INSERT INTO testorder (a, b, c) VALUES (?, ?, ?)")
        execute_concurrent_with_args(self.session, insert_statement, data)

        self.tempfile = NamedTemporaryFile(delete=False)

        self.node1.run_cqlsh(
            "COPY ks.testorder (a, c, b) TO '{name}'".format(name=self.tempfile.name))

        reference_file = NamedTemporaryFile(delete=False)
        with open(reference_file.name, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])
            csvfile.close

        assert_csvs_items_equal(self.tempfile.name, reference_file.name)

    def test_explicit_column_order_reading(self):
        """
        Test that COPY can write to a CSV file when the order of columns is
        explicitly specified by:

        - creating a table,
        - writing a CSV file containing columns with the same types as the
        table, but in a different order,
        - COPYing the contents of that CSV into the table by specifying the
        order of the columns,
        - asserting that the values in the CSV file match those in the table.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testorder (
                a int primary key,
                b text,
                c int
            )""")

        data = [[1, 20, 'ham'], [2, 40, 'eggs'],
                [3, 60, 'beans'], [4, 80, 'toast']]

        self.tempfile = NamedTemporaryFile(delete=False)
        write_rows_to_csv(self.tempfile.name, data)

        self.node1.run_cqlsh(
            "COPY ks.testorder (a, c, b) FROM '{name}'".format(name=self.tempfile.name))

        results = list(self.session.execute("SELECT * FROM testorder"))
        reference_file = NamedTemporaryFile(delete=False)
        with open(reference_file.name, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])
        csvfile.close

        self.assertCsvResultEqual(reference_file.name, results)

    def quoted_column_names_reading_template(self, specify_column_names):
        """
        @param specify_column_names if truthy, specify column names in COPY statement
        A parameterized test. Tests that COPY can read from a CSV file into a
        table with quoted column names by:

        - creating a table with quoted column names,
        - writing test data to a CSV file,
        - COPYing that CSV file into the table, explicitly naming columns, and
        - asserting that the CSV file and the table contain the same data.

        If the specify_column_names parameter is truthy, the COPY statement
        explicitly names the columns.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )""")

        data = [[1, 'no'], [2, 'Yes'],
                [3, 'True'], [4, 'false']]

        self.tempfile = NamedTemporaryFile(delete=False)
        write_rows_to_csv(self.tempfile.name, data)

        stmt = ("""COPY ks.testquoted ("IdNumber", "select") FROM '{name}'"""
                if specify_column_names else
                """COPY ks.testquoted FROM '{name}'""").format(name=self.tempfile.name)

        self.node1.run_cqlsh(stmt)

        results = list(self.session.execute("SELECT * FROM testquoted"))
        self.assertCsvResultEqual(self.tempfile.name, results)

    def test_quoted_column_names_reading_specify_names(self):
        """
        Use quoted_column_names_reading_template to test reading from a CSV file
        into a table with quoted column names, explicitly specifying the column
        names in the COPY statement.
        """
        self.quoted_column_names_reading_template(specify_column_names=True)

    def test_quoted_column_names_reading_dont_specify_names(self):
        """
        Use quoted_column_names_reading_template to test reading from a CSV file
        into a table with quoted column names, without explicitly specifying the
        column names in the COPY statement.
        """
        self.quoted_column_names_reading_template(specify_column_names=False)

    def quoted_column_names_writing_template(self, specify_column_names):
        """
        @param specify_column_names if truthy, specify column names in COPY statement
        A parameterized test. Test that COPY can write to a table with quoted
        column names by:

        - creating a table with quoted column names,
        - inserting test data into that table,
        - COPYing that table into a CSV file into the table, explicitly naming columns,
        - writing that test data to a CSV file,
        - asserting that the two CSV files contain the same rows.

        If the specify_column_names parameter is truthy, the COPY statement
        explicitly names the columns.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )""")

        data = [[1, 'no'], [2, 'Yes'],
                [3, 'True'], [4, 'false']]
        insert_statement = self.session.prepare("""INSERT INTO testquoted ("IdNumber", "select") VALUES (?, ?)""")
        execute_concurrent_with_args(self.session, insert_statement, data)

        self.tempfile = NamedTemporaryFile(delete=False)
        stmt = ("""COPY ks.testquoted ("IdNumber", "select") TO '{name}'"""
                if specify_column_names else
                """COPY ks.testquoted TO '{name}'""").format(name=self.tempfile.name)
        self.node1.run_cqlsh(stmt)

        reference_file = NamedTemporaryFile(delete=False)
        write_rows_to_csv(reference_file.name, data)

        assert_csvs_items_equal(self.tempfile.name, reference_file.name)

    def test_quoted_column_names_writing_specify_names(self):
        self.quoted_column_names_writing_template(specify_column_names=True)

    def test_quoted_column_names_writing_dont_specify_names(self):
        self.quoted_column_names_writing_template(specify_column_names=False)

    def data_validation_on_read_template(self, load_as_int, expect_invalid):
        """
        @param load_as_int the value that will be loaded into a table as an int value
        @param expect_invalid whether or not to expect the COPY statement to fail

        Test that reading from CSV files fails when there is a type mismatch
        between the value being loaded and the type of the column by:

        - creating a table,
        - writing a CSV file containing the value passed in as load_as_int, then
        - COPYing that csv file into the table, loading load_as_int as an int.

        If expect_invalid, this test will succeed when the COPY command fails
        with a "Bad request" error message. If not expect_invalid, this test
        will succeed when the COPY command prints no errors and the table
        matches the loaded CSV file.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testvalidate (
                a int PRIMARY KEY,
                b int
            )""")

        data = [[1, load_as_int]]

        self.tempfile = NamedTemporaryFile(delete=False)
        write_rows_to_csv(self.tempfile.name, data)

        cmd = """COPY ks.testvalidate (a, b) FROM '{name}'""".format(name=self.tempfile.name)
        out, err = self.node1.run_cqlsh(cmd, return_output=True)
        results = list(self.session.execute("SELECT * FROM testvalidate"))

        if expect_invalid:
            self.assertRegexpMatches('Bad [Rr]equest', err)
            self.assertFalse(results)
        else:
            self.assertFalse(err)
            self.assertCsvResultEqual(self.tempfile.name, results)

    def test_read_valid_data(self):
        """
        Use data_validation_on_read_template to test COPYing an int value from a
        CSV into an int column. This test exists to make sure the parameterized
        test works.
        """
        # make sure the template works properly
        self.data_validation_on_read_template(2, expect_invalid=False)

    def test_read_invalid_float(self):
        """
        Use data_validation_on_read_template to test COPYing a float value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template(2.14, expect_invalid=True)

    def test_read_invalid_uuid(self):
        """
        Use data_validation_on_read_template to test COPYing a uuid value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template(uuid4(), expect_invalid=True)

    def test_read_invalid_text(self):
        """
        Use data_validation_on_read_template to test COPYing a text value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template('test', expect_invalid=True)

    def test_all_datatypes_write(self):
        """
        Test that, after COPYing a table containing all CQL datatypes to a CSV
        file, that the table contains the same values as the CSV by:

        - creating and populating a table containing all datatypes,
        - COPYing the contents of that table to a CSV file, and
        - asserting that the CSV file contains the same data as the table.
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype TO '{name}'".format(name=self.tempfile.name))

        results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.assertCsvResultEqual(self.tempfile.name, results)

    def test_all_datatypes_read(self):
        """
        Test that, after COPYing a CSV file to a table containing all CQL
        datatypes, that the table contains the same values as the CSV by:

        - creating a table containing all datatypes,
        - writing a corresponding CSV file containing each datatype,
        - COPYing the CSV file into the table, and
        - asserting that the CSV file contains the same data as the table.
        """
        self.all_datatypes_prepare()

        self.tempfile = NamedTemporaryFile(delete=False)

        with open(self.tempfile.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(self.data)
            csvfile.close

        debug('Importing from csv file: {name}'.format(name=self.tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype FROM '{name}'".format(name=self.tempfile.name))

        results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.assertCsvResultEqual(self.tempfile.name, results)

    def test_all_datatypes_round_trip(self):
        """
        Test that a table containing all CQL datatypes successfully round-trips
        to and from a CSV file via COPY by:

        - creating and populating a table containing every datatype,
        - COPYing that table to a CSV file,
        - SELECTing the contents of the table,
        - TRUNCATEing the table,
        - COPYing the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype TO '{name}'".format(name=self.tempfile.name))

        exported_results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.session.execute('TRUNCATE ks.testdatatype')

        self.node1.run_cqlsh(cmds="COPY ks.testdatatype FROM '{name}'".format(name=self.tempfile.name))

        imported_results = list(self.session.execute("SELECT * FROM testdatatype"))

        assert len(imported_results) == 1

        self.assertEqual(exported_results, imported_results)

    def test_wrong_number_of_columns(self):
        """
        Test that a COPY statement will fail when trying to import from a CSV
        file with the wrong number of columns by:

        - creating a table with a single column,
        - writing a CSV file with two columns,
        - attempting to COPY the CSV file into the table, and
        - asserting that the COPY operation failed.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcolumns (
                a int PRIMARY KEY,
                b int
            )""")

        data = [[1, 2, 3]]
        self.tempfile = NamedTemporaryFile(delete=False)
        write_rows_to_csv(self.tempfile.name, data)

        debug('Importing from csv file: {name}'.format(name=self.tempfile.name))
        out, err = self.node1.run_cqlsh("COPY ks.testcolumns FROM '{name}'".format(name=self.tempfile.name),
                                        return_output=True)

        self.assertFalse(self.session.execute("SELECT * FROM testcolumns"))
        self.assertIn('Aborting import', err)

    def test_round_trip(self):
        """
        Test a simple round trip of a small CQL table to and from a CSV file via
        COPY.

        - creating and populating a table,
        - COPYing that table to a CSV file,
        - SELECTing the contents of the table,
        - TRUNCATEing the table,
        - COPYing the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )""")

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testcopyto"))

        self.tempfile = NamedTemporaryFile(delete=False)
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testcopyto TO '{name}'".format(name=self.tempfile.name))

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        debug('Importing from csv file: {name}'.format(name=self.tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '{name}'".format(name=self.tempfile.name))
        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        self.assertEqual(results, new_results)

    def test_source_copy_round_trip(self):
        """
        Like test_round_trip, but uses the SOURCE command to execute the
        COPY command.  This checks that we don't have unicode-related
        problems when sourcing COPY commands (CASSANDRA-9083).
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )""")

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testcopyto"))

        self.tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=self.tempfile.name))

        commandfile = NamedTemporaryFile()
        with open(commandfile.name, 'w') as commands:
            commands.write('USE ks;\n')
            commands.write("COPY ks.testcopyto TO '{name}' WITH HEADER=false;".format(name=self.tempfile.name))

        self.node1.run_cqlsh(cmds="SOURCE '{name}'".format(name=commandfile.name))

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        debug('Importing from csv file: {name}'.format(name=self.tempfile.name))

        commandfile = NamedTemporaryFile()
        with open(commandfile.name, 'w') as commands:
            commands.write('USE ks;\n')
            commands.write("COPY ks.testcopyto FROM '{name}' WITH HEADER=false;".format(name=self.tempfile.name))

        self.node1.run_cqlsh(cmds="SOURCE '{name}'".format(name=commandfile.name))
        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        self.assertEqual(results, new_results)
