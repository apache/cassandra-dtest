# coding=utf-8

import csv
import datetime
import glob
import io
import json
import locale
import logging
import os
import re
import time
from collections import namedtuple
from decimal import Decimal
from distutils.version import LooseVersion
from tempfile import NamedTemporaryFile, gettempdir, template
from uuid import uuid1, uuid4

import pytest
from cassandra.cluster import ConsistencyLevel, SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.murmur3 import murmur3
from cassandra.util import SortedSet
from ccmlib.common import is_win

from dtest import (FlakyRetryPolicy, Tester, create_ks)
from tools.data import rows_to_list
from . import util
from .cqlsh_test_types import (Address, Datetime, ImmutableDict,
                               ImmutableSet, Name, UTC, drop_microseconds)
from .cqlsh_tools import (assert_csvs_items_equal,
                          csv_rows, monkeypatch_driver, random_list,
                          unmonkeypatch_driver, write_rows_to_csv)

since = pytest.mark.since
logger = logging.getLogger(__name__)

PARTITIONERS = {
    "murmur3": "org.apache.cassandra.dht.Murmur3Partitioner",
    "random": "org.apache.cassandra.dht.RandomPartitioner",
    "byte": "org.apache.cassandra.dht.ByteOrderedPartitioner",
    "order": "org.apache.cassandra.dht.OrderPreservingPartitioner"
}


class TestCqlshCopy(Tester):
    """
    Tests the COPY TO and COPY FROM features in cqlsh.
    @jira_ticket CASSANDRA-3906
    """

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            r'.*Unknown exception in client networking.*'
        )

    @pytest.fixture(scope='function', autouse=True)
    def fixture_temp_files(self):
        self._tempfiles = []
        yield
        self.delete_temp_files()

    @classmethod
    def setUpClass(cls):
        cls._cached_driver_methods = monkeypatch_driver()
        if locale.getpreferredencoding() != 'UTF-8':
            os.environ['LC_CTYPE'] = 'en_US.utf8'

    @classmethod
    def tearDownClass(cls):
        unmonkeypatch_driver(cls._cached_driver_methods)

    def get_temp_file(self, prefix=template, suffix=""):
        """
        On windows we cannot open temporary files after creating them unless we close them first.
        For this reason we must also create them with delete=False (or they would be deleted immediately when closed)
        and we want to make sure that the test object owns a reference to the file objects by adding them
        to self._tempfiles, so that they can be deleted when the test finishes.
        """
        ret = NamedTemporaryFile(delete=False, prefix=prefix, suffix=suffix)
        self._tempfiles.append(ret)
        if is_win():
            ret.close()
        return ret

    def delete_temp_files(self):
        for tempfile in self._tempfiles:
            if os.path.isfile(tempfile.name):
                if is_win():
                    tempfile.close()
                os.unlink(tempfile.name)

        for pattern in ['import_ks_*.err*', 'import_keyspace1_*.err*']:
            for err_file in glob.glob(pattern):
                os.unlink(err_file)

    def prepare(self, nodes=1, partitioner="murmur3", configuration_options=None, tokens=None, auth_enabled=False):
        p = PARTITIONERS[partitioner]
        if not self.cluster.nodelist():
            self.cluster.set_partitioner(p)
            if auth_enabled:
                if configuration_options is None:
                    configuration_options = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator'}
                else:
                    configuration_options['authenticator'] = 'org.apache.cassandra.auth.PasswordAuthenticator'

            if self.cluster.cassandra_version() >= '4' and not ('Murmur3' in p or 'Random' in p):
                if configuration_options is None:
                    configuration_options = {'allocate_tokens_for_local_replication_factor': None}
                else:
                    configuration_options['allocate_tokens_for_local_replication_factor'] = None

            if configuration_options:
                self.cluster.set_configuration_options(values=configuration_options)
            self.cluster.populate(nodes, tokens=tokens).start()
        else:
            assert self.cluster.partitioner == p, "Cannot reuse cluster: different partitioner"
            assert len(self.cluster.nodelist()) == nodes, "Cannot reuse cluster: different number of nodes"
            assert configuration_options is None

        self.node1 = self.cluster.nodelist()[0]

        if auth_enabled:
            self.node1.watch_log_for('Created default superuser')
            self.session = self.patient_cql_connection(self.node1, user='cassandra', password='cassandra')
        else:
            self.session = self.patient_cql_connection(self.node1)

        self.session.execute('DROP KEYSPACE IF EXISTS ks')
        self.ks = 'ks'
        create_ks(self.session, self.ks, 1)
        self.cqlshrc = self.create_default_cqlsh_properties()

    def create_default_cqlsh_properties(self):
        """
        Create a cqlshrc file containing some common copy options that are useful for
        all tests:

            - fix the page size and page timeout for COPY TO to 1000 rows and 60 seconds respectively,
            since the default, 10 seconds per 1000 rows, can cause rare timeouts on slow Jenkins machines.

        Note that any COPY options specified in the COPY command will override the options
        specified in cqlshrc, so individual tests can still override these options if required.

        :return: the full path to the cqlshrc file
        """
        cqlshrc = os.path.join(self.node1.get_path(), 'conf', '.cqlshrc')

        with open(cqlshrc, 'w') as f:
            f.write('[copy-to]\n')
            f.write('pagesize = 1000\n')
            f.write('pagetimeout = 60\n')

        return cqlshrc

    def run_cqlsh(self, cmds=None, cqlsh_options=None, use_debug=True, skip_cqlshrc=False,
                  auth_enabled=False, show_output=True, retry_on_request_timeout=True):
        """
        Run cqlsh on node1 adding the debug and cqlshrc to the clqsh options, unless the caller
        has specified its own options.
        """
        if cqlsh_options is None:
            cqlsh_options = []

        if not skip_cqlshrc:
            cqlsh_options.append('--cqlshrc={}'.format(self.cqlshrc))

        if use_debug:
            cqlsh_options.append('--debug')

        if auth_enabled:
            cqlsh_options.append('--username=cassandra')
            cqlsh_options.append('--password=cassandra')

        if retry_on_request_timeout:
            num_attempts = 0
            while num_attempts < 5:
                ret = util.run_cqlsh_safe(self.node1, cmds=cmds, cqlsh_options=cqlsh_options, expect_error=False)

                if not re.search(r"Client request timeout", ret[0]):
                    break

                num_attempts += 1
        else:
            ret = util.run_cqlsh_safe(self.node1, cmds=cmds, cqlsh_options=cqlsh_options, expect_error=False)

        if show_output:
            logger.debug('Output:\n{}'.format(ret[0]))  # show stdout of copy cmd
            logger.debug('Errors:\n{}'.format(ret[1]))  # show stderr of copy cmd

        return ret

    def parse_cqlsh_query(self, out, num_cols, timestamps_to_be_rounded=[], nullval=''):
        """
        Parse the standard output of a SELECT query done by running clqsh.
        out : the standard output of the cqlsh command
        num_cols = the number of columns that the query is expected to return
        timestamps_to_be_rounded : the indexes of the columns for which we should round timestamps to milliseconds
        """
        results = []
        for line in out.split('\n')[3:]:  # skip blank line, header and table separator
            res = [f.strip() for f in line.split('|')]
            res = [re.sub('null', nullval, f) for f in res]
            if len(res) != num_cols:
                continue  # The line does not container results (header or other messages)

            for i in timestamps_to_be_rounded:
                res[i] = drop_microseconds(res[i])

            results.append(res)

        return results

    @property
    def default_time_format(self):
        """
        The default time format used by cqlshlib/formatting.py. It must match DEFAULT_TIMESTAMP_FORMAT
        for versions 2.2+ and for 2.1 it's hard-coded. Milliseconds were added in version 3.4 and later by
        CASSANDRA-10428, hence the difference in values before and after this version.
        """
        if self.cluster.cassandra_version() < LooseVersion('3.4'):
            logger.debug('Using legacy time format for version ' + str(self.cluster.cassandra_version()))
            self._default_time_format = '%Y-%m-%d %H:%M:%S%z'
        else:
            logger.debug('Using latest time format for version ' + str(self.cluster.cassandra_version()))
            self._default_time_format = '%Y-%m-%d %H:%M:%S.%f%z'

        return self._default_time_format

    @staticmethod
    def format_blob(blob):
        return '0x{}'.format(''.join('%02x' % c for c in blob))

    def all_datatypes_prepare(self):
        self.prepare()

        self.session.execute('CREATE TYPE name_type (firstname text, lastname text)')
        self.session.execute('''
            CREATE TYPE address_type (name frozen<name_type>, number int, street text, phones set<text>)
            ''')

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
                o varint,
                p list<int>,
                q set<text>,
                r map<timestamp, text>,
                s tuple<int, text, boolean>,
                t frozen<address_type>,
                u frozen<list<list<address_type>>>,
                v frozen<map<map<int,int>,set<text>>>,
                w frozen<set<set<inet>>>,
                x map<text, frozen<list<text>>>,
                y map<int, blob>,
                z list<blob>,
                za set<blob>
            )''')

        self.session.cluster.register_user_type('ks', 'name_type', Name)
        self.session.cluster.register_user_type('ks', 'address_type', Address)

        date1 = Datetime(2005, 7, 14, 12, 30, 0, 0, UTC(), time_format=self.default_time_format)
        date2 = Datetime(2005, 7, 14, 13, 30, 0, 0, UTC(), time_format=self.default_time_format)
        date3 = Datetime(2005, 7, 14, 14, 30, 0, 0, UTC(), time_format=self.default_time_format)

        addr1 = Address(Name('name1', 'last1'), 1, 'street 1', ImmutableSet(['1111 2222', '3333 4444']))
        addr2 = Address(Name('name2', 'last2'), 2, 'street 2', ImmutableSet(['5555 6666', '7777 8888']))
        addr3 = Address(Name('name3', 'last3'), 3, 'street 3', ImmutableSet(['1111 2222', '3333 4444']))
        addr4 = Address(Name('name4', 'last4'), 4, 'street 4', ImmutableSet(['5555 6666', '7777 8888']))

        self.data = ('ascii',  # a ascii
                     2 ** 40,  # b bigint
                     bytearray.fromhex('beef'),  # c blob
                     True,  # d boolean
                     Decimal(3.14),  # e decimal
                     2.444,  # f double
                     1.1,  # g float
                     '127.0.0.1',  # h inet
                     25,  # i int
                     'ヽ(´ー｀)ノ',  # j text
                     date1,  # k timestamp
                     uuid1(),  # l timeuuid
                     uuid4(),  # m uuid
                     'asdf',  # n varchar
                     2 ** 65,  # o varint
                     [1, 2, 3],  # p list<int>,
                     ImmutableSet(['3', '2', '1', '']),  # q set<text>,
                     ImmutableDict([(date1, '1'), (date2, '2'), (date3, '')]),  # r map<timestamp, text>,
                     (1, '1', True),  # s tuple<int, text, boolean>,
                     addr1,  # t frozen<address_type>,
                     [[addr1, addr2], [addr3, addr4]],  # u frozen<list<list<address_type>>>,
                     # v frozen<map<map<int,int>,set<text>>>
                     ImmutableDict([(ImmutableDict([(1, 1), (2, 2)]), ImmutableSet(['1', '2', '3']))]),
                     # w frozen<set<set<inet>>>, because of the SortedSet.__lt__() implementation, make sure the
                     # first set is contained in the second set or else they will not sort consistently
                     # and this will cause comparison problems when comparing with csv strings therefore failing
                     # some tests
                     ImmutableSet([ImmutableSet(['127.0.0.1']), ImmutableSet(['127.0.0.1', '127.0.0.2'])]),
                     {'key1': ['value1', 'value2', '']},  # map<text, frozen<list<text>>>
                     {3: bytes.fromhex('74776f')},  # y
                     [bytes.fromhex('74776f')],  # z
                     {bytes.fromhex('74776f')}  # za
                     )

    def assertCsvResultEqual(self, csv_filename, results, table_name=None,
                             columns=None, cql_type_names=None, sort_data=True):

        csv_results = list(csv_rows(csv_filename))
        self.maxDiff = None

        if sort_data:
            csv_results = sorted(csv_results)
            results = sorted(results)

        try:
            assert csv_results == results
        except Exception as e:
            if len(csv_results) != len(results):
                logger.warning("Different # of entries. CSV: {}, vs query results : {}".format(len(csv_results), len(results)))
            else:
                line_matches = True
                for i in range(0, len(csv_results)):
                    if not line_matches:
                        break
                    for x in range(0, len(csv_results[i])):
                        if csv_results[i][x] != results[i][x]:
                            if line_matches:
                                line_matches = False
                            logger.warning("Value in csv at [{}][{}]: {}".format(i, x, str(csv_results[i][x])))
                            logger.warning("Value in query at [{}][{}]: {}".format(i, x, str(results[i][x])))

            raise e

    def stringify_results(self, results, format_fn=str):
        """
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        """
        processed = []

        for row in results:
            processed.append([format_fn(v) for v in row])

        return processed

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

        out, err, _ = self.run_cqlsh(cmds="PAGING OFF; SELECT * FROM ks.testlist")
        results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[])

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.run_cqlsh(cmds="COPY ks.testlist TO '{name}'".format(name=tempfile.name))

        self.assertCsvResultEqual(tempfile.name, results, 'testlist')

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

        out, err, _ = self.run_cqlsh(cmds="PAGING OFF; SELECT * FROM ks.testtuple")
        results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[])

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.run_cqlsh(cmds="COPY ks.testtuple TO '{name}'".format(name=tempfile.name))

        self.assertCsvResultEqual(tempfile.name, results, 'testtuple')

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
        args = [(i,) for i in range(10000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = self.stringify_results(self.session.execute("SELECT * FROM testdelimiter"))

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testdelimiter TO '{name}'".format(name=tempfile.name)
        cmds += " WITH DELIMITER = '{d}'".format(d=delimiter)
        self.run_cqlsh(cmds=cmds)

        self.assertCsvResultEqual(tempfile.name, results, 'testdelimiter')

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

    def custom_null_indicator_template(self, indicator=None, copy_from_options=None):
        """
        @param indicator the null indicator to be used in COPY, set to None to use the default indicator

        A parametrized test that tests COPY with a given null indicator:

        - insert some data including rows with missing values
        - export the data and check that the csv file contains the expected null indicator for the missing values
        - truncate the table and import the csv file
        - check that the data imported is the same as originally inserted

        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testnullindicator (
                a int primary key,
                b text,
                c int,
                d float,
                e timestamp,
                f list<int>
            )""")
        insert_non_null = self.session.prepare("INSERT INTO testnullindicator (a, b, c, d, e, f) " +
                                               "VALUES (?, ?, ?, ?, ?, ?)")
        execute_concurrent_with_args(self.session, insert_non_null,
                                     [(1, 'eggs', 1, 1.1,
                                       datetime.datetime(2015, 1, 1, 0, 00, 0, 0, UTC()), [1, 2, 3]),
                                      (100, 'sausage', 100, 2.2, None, None)])
        insert_null = self.session.prepare("INSERT INTO testnullindicator (a) VALUES (?)")
        execute_concurrent_with_args(self.session, insert_null, [(2,), (200,)])

        if copy_from_options is None:
            copy_from_options = dict()

        if indicator:
            copy_from_options['NULL'] = indicator

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testnullindicator TO '{name}'".format(name=tempfile.name)
        if indicator:
            cmds += " WITH NULL = '{d}'".format(d=indicator)
        logger.debug(cmds)
        out, _, _ = self.run_cqlsh(cmds=cmds)
        logger.debug(out)

        nullval = indicator if indicator is not None else ''
        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testnullindicator")
        results = self.parse_cqlsh_query(out=out, num_cols=6, timestamps_to_be_rounded=[4], nullval=nullval)
        self.assertCsvResultEqual(tempfile.name, results, 'testnullindicator')

        # Now import back the csv file
        self.session.execute('TRUNCATE ks.testnullindicator')
        logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testnullindicator FROM '{name}'".format(name=tempfile.name)
        if copy_from_options:
            first = True
            for k, v in copy_from_options.items():
                cmds += ' {} {} = {}'.format('WITH' if first else 'AND', k, v)
                first = False
        logger.debug(cmds)
        self.run_cqlsh(cmds=cmds)

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testnullindicator")
        results_imported = self.parse_cqlsh_query(out=out, num_cols=6, timestamps_to_be_rounded=[4], nullval=nullval)
        assert results == results_imported

    def test_default_null_indicator(self):
        """
        Test the default null indicator.

        @jira_ticket CASSANDRA-11549
        """
        self.custom_null_indicator_template()

    def test_default_null_indicator_no_prepared_statements(self):
        """
        Test the default null indicator without prepared statements.

        @jira_ticket CASSANDRA-11631
        """
        self.custom_null_indicator_template(copy_from_options={'PREPAREDSTATEMENTS': 'False'})

    def test_undefined_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = undefined.
        """
        self.custom_null_indicator_template('undefined')

    def test_undefined_as_null_indicator_no_prepared_statements(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = undefined and no prepared statements.
        """
        self.custom_null_indicator_template('undefined', copy_from_options={'PREPAREDSTATEMENTS': 'False'})

    def test_null_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = 'null'.
        """
        self.custom_null_indicator_template('null')

    def test_reading_collections_with_empty_values(self):
        """
        Inserting null values in collections, for example in lists, results in assertion errors server side.
        Therefore, rather tha converting empty values to None like for top level types, a parse error will be
        raised to make debugging the problem easier. Note that we only check for empty values, we ignore the
        null indicator.

        @jira_ticket CASSANDRA-11631
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testnullvalsincollections (
                a int primary key,
                b list<int>
            )""")

        tempfile = self.get_temp_file()
        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            writer.writerow({'a': 1, 'b': "[1,2,3]"})
            writer.writerow({'a': 2, 'b': "[1,,3]"})

        logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testnullvalsincollections FROM '{name}'".format(name=tempfile.name)
        out, err, _ = self.run_cqlsh(cmds=cmds)
        logger.debug(out)
        logger.debug(err)
        assert "ParseError - Failed to parse [1,,3] : Empty values are not allowed" in err

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
                a text primary key,
                b int
            )""")
        insert_statement = self.session.prepare("INSERT INTO testheader (a, b) VALUES (?, ?)")
        args = [('b', 10), ('c', 20), ('d', 30)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testheader TO '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        self.run_cqlsh(cmds=cmds)

        with open(tempfile.name, 'r') as csvfile:
            csv_values = list(csv.reader(csvfile))

        assert sorted(csv_values) == [['a', 'b'], ['b', '10'], ['c', '20'], ['d', '30']]

    def _test_reading_counter_template(self, copy_options=None):
        """
        Test that COPY can read a csv file of COUNTER values by:

        - creating a table,
        - writing a CSV with COUNTER data with header,
        - importing the contents of the CSV file using COPY with header,
        - checking that the contents of the table are the written values.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS testcounter (
                a int,
                b text,
                c counter,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()

        data = [[1, '1', 20], [2, '2', 40], [3, '3', 60], [4, '4', 80]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
            writer.writeheader()
            for a, b, c in data:
                writer.writerow({'a': a, 'b': b, 'c': c})

        self.session.execute("TRUNCATE TABLE testcounter")
        cmds = "COPY ks.testcounter FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        if copy_options:
            for opt, val in copy_options.items():
                cmds += " AND {} = {}".format(opt, val)

        logger.debug("Running {}".format(cmds))
        self.run_cqlsh(cmds=cmds)

        result = self.session.execute("SELECT * FROM testcounter")
        result_as_list = rows_to_list(result)
        assert data == sorted(result_as_list)

    def test_reading_counter(self):
        """
        Test that COPY can read a csv file of COUNTER values.

        @jira_ticket CASSANDRA-9043
        """
        self._test_reading_counter_template()

    @since('2.2.5')
    def test_reading_counter_without_batching(self):
        """
        Test that COPY can read a csv file of COUNTER values with batching disabled,
        that is MAXBATCHSIZE set to 1.

        @jira_ticket CASSANDRA-11474
        """
        self._test_reading_counter_template(copy_options={'MAXBATCHSIZE': '1'})

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

        tempfile = self.get_temp_file()

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            writer.writeheader()
            for a, b in data:
                writer.writerow({'a': a, 'b': b})

        cmds = "COPY ks.testheader FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        self.run_cqlsh(cmds=cmds)

        result = self.session.execute("SELECT * FROM testheader")
        result_as_list = [tuple(r) for r in rows_to_list(result)]
        assert [tuple(d) for d in data] == sorted(result_as_list)

    @since('2.2', max_version='3.X')
    def test_datetimeformat_round_trip(self):
        """
        @jira_ticket CASSANDRA-10633
        @jira_ticket CASSANDRA-9303

        Test COPY TO and COPY FORM with the time format specified in the WITH option by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY TO WITH DATETIMEFORMAT,
        - checking the time format written to csv.
        - importing the CSV back into the table
        - comparing the table contents before and after the import

        CASSANDRA-9303 renamed TIMEFORMAT to DATETIMEFORMAT
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testdatetimeformat (
                a int primary key,
                b timestamp
            )""")
        insert_statement = self.session.prepare("INSERT INTO testdatetimeformat (a, b) VALUES (?, ?)")
        args = [(1, datetime.datetime(2015, 1, 1, 0o7, 00, 0, 0, UTC())),
                (2, datetime.datetime(2015, 6, 10, 12, 30, 30, 500, UTC())),
                (3, datetime.datetime(2015, 12, 31, 23, 59, 59, 999, UTC()))]
        execute_concurrent_with_args(self.session, insert_statement, args)
        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testdatetimeformat")
        exported_results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[1])

        format = '%Y/%m/%d %H:%M:%S'

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testdatetimeformat TO '{name}'".format(name=tempfile.name)
        cmds += " WITH DATETIMEFORMAT = '{}'".format(format)
        copy_to_out, copy_to_err, _ = self.run_cqlsh(cmds=cmds)

        with open(tempfile.name, 'r') as csvfile:
            csv_values = list(csv.reader(csvfile))

        assert sorted(csv_values) == [['1', '2015/01/01 07:00:00'],
                                      ['2', '2015/06/10 12:30:30'],
                                      ['3', '2015/12/31 23:59:59']]

        self.session.execute("TRUNCATE testdatetimeformat")
        cmds = "COPY ks.testdatetimeformat FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH DATETIMEFORMAT = '{}'".format(format)
        copy_from_out, copy_from_err, _ = self.run_cqlsh(cmds=cmds)

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testdatetimeformat")
        imported_results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[1])
        assert sorted(exported_results) == sorted(imported_results)

    @since('4.0')
    def test_datetimeformat_round_trip_40(self):
        """
        @jira_ticket CASSANDRA-10633
        @jira_ticket CASSANDRA-9303

        Test COPY TO and COPY FORM with the time format specified in the WITH option by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY TO WITH DATETIMEFORMAT,
        - checking the time format written to csv.
        - importing the CSV back into the table
        - comparing the table contents before and after the import

        CASSANDRA-9303 renamed TIMEFORMAT to DATETIMEFORMAT
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testdatetimeformat (
                a int primary key,
                b timestamp
            )""")
        insert_statement = self.session.prepare("INSERT INTO testdatetimeformat (a, b) VALUES (?, ?)")
        args = [(1, datetime.datetime(2015, 1, 1, 0o7, 00, 0, 0, UTC())),
                (2, datetime.datetime(2015, 6, 10, 12, 30, 30, 500, UTC())),
                (3, datetime.datetime(2015, 12, 31, 23, 59, 59, 999, UTC()))]
        execute_concurrent_with_args(self.session, insert_statement, args)
        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testdatetimeformat")
        exported_results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[1])

        format = '%Y-%m-%d %H:%M:%S%z'

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testdatetimeformat TO '{name}'".format(name=tempfile.name)
        cmds += " WITH DATETIMEFORMAT = '{}' AND NUMPROCESSES=1".format(format)
        copy_to_out, copy_to_err, _ = self.run_cqlsh(cmds=cmds)

        with open(tempfile.name, 'r') as csvfile:
            csv_values = list(csv.reader(csvfile))

        assert sorted(csv_values) == [['1', '2015-01-01 07:00:00+0000'],
                                      ['2', '2015-06-10 12:30:30+0000'],
                                      ['3', '2015-12-31 23:59:59+0000']]

        self.session.execute("TRUNCATE testdatetimeformat")
        cmds = "COPY ks.testdatetimeformat FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH DATETIMEFORMAT = '{}' AND NUMPROCESSES=1".format(format)
        copy_from_out, copy_from_err, _ = self.run_cqlsh(cmds=cmds)

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testdatetimeformat")
        imported_results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[1])

        assert sorted(exported_results) == sorted(imported_results)

    @since('3.2')
    def test_reading_with_ttl(self):
        """
        @jira_ticket CASSANDRA-9494
        Test COPY FROM with TTL specified in the WITH option by:

        - creating a table,
        - writing a csv,
        - importing the contents of the CSV file using COPY TO WITH TTL,
        - checking the data has been imported,
        - checking again after TTL * 2 seconds that the data has expired.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testttl (
                a int primary key,
                b int
            )""")

        tempfile = self.get_temp_file()

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            for a, b in data:
                writer.writerow({'a': a, 'b': b})

        self.run_cqlsh(cmds="COPY ks.testttl FROM '{name}' WITH TTL = '5'".format(name=tempfile.name))

        result = rows_to_list(self.session.execute("SELECT * FROM testttl"))
        assert data == sorted(result)

        time.sleep(10)

        result = rows_to_list(self.session.execute("SELECT * FROM testttl"))
        assert [] == result

    def test_reading_with_skip_and_max_rows(self):
        """
        Test importing a rows from a CSV file with maxrows and skiprows:

        - create a large CSV file via stress write and COPY TO
        - For a specified number of rows:
        -- truncate the table
        -- import the CSV file with max rows set to this number
        -- compare the number of rows imported via select count

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        tempfile = self.get_temp_file()
        stress_table = 'keyspace1.standard1'
        num_file_rows = 10000

        logger.debug('Running stress to generate a large CSV via COPY TO')
        self.node1.stress(['write', 'n={}'.format(num_file_rows), 'no-warmup', '-rate', 'threads=50'])
        self.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))
        assert num_file_rows == len(open(tempfile.name).readlines())

        def do_test(num_rows, skip_rows):
            logger.debug('Preparing to test {} max rows and {} skip rows by truncating table'.format(num_rows, skip_rows))
            self.session.execute("TRUNCATE {}".format(stress_table))
            result = rows_to_list(self.session.execute("SELECT * FROM {}".format(stress_table)))
            assert [] == result

            logger.debug('Importing {} rows'.format(num_rows))
            self.run_cqlsh(cmds="COPY {} FROM '{}' WITH MAXROWS = '{}' AND SKIPROWS='{}'"
                           .format(stress_table, tempfile.name, num_rows, skip_rows))

            expected_rows = num_rows if 0 <= num_rows < num_file_rows else num_file_rows
            expected_rows -= min(num_file_rows, max(0, skip_rows))
            assert [[expected_rows]] == rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))
            logger.debug('Imported {} as expected'.format(expected_rows))

        # max rows tests
        do_test(-1, 0)
        do_test(0, 0)
        do_test(1, 0)
        do_test(100, 0)
        do_test(num_file_rows, 0)
        do_test(num_file_rows + 1, 0)

        # skip rows tests
        do_test(-1, 100)
        do_test(num_file_rows, 100)
        do_test(100, 100)
        do_test(num_file_rows, num_file_rows)
        do_test(num_file_rows, num_file_rows + 1)
        do_test(num_file_rows, -1)

    def test_reading_with_skip_cols(self):
        """
        Test importing a CSV file but skipping some columns:

        - create a table
        - create a csv file with all column values
        - import the csv file with skip_columns
        - check only the columns that were not skipped are in the table

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testskipcols (
                a int primary key,
                b int,
                c int,
                d int,
                e int
            )""")

        tempfile = self.get_temp_file()
        data = [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c', 'd', 'e'])
            for a, b, c, d, e in data:
                writer.writerow({'a': a, 'b': b, 'c': c, 'd': d, 'e': e})

        def do_test(skip_cols, expected_results):
            self.session.execute('TRUNCATE ks.testskipcols')
            logger.debug("Importing csv file {} with skipcols '{}'".format(tempfile, skip_cols))
            out, err, _ = self.run_cqlsh(cmds="COPY ks.testskipcols FROM '{}' WITH SKIPCOLS = '{}'"
                                         .format(tempfile.name, skip_cols))
            logger.debug(out)
            assert expected_results == sorted(rows_to_list(self.session.execute("SELECT * FROM ks.testskipcols")))

        do_test('c, d ,e', [[1, 2, None, None, None], [6, 7, None, None, None]])
        do_test('b,', [[1, None, 3, 4, 5], [6, None, 8, 9, 10]])
        do_test('b', [[1, None, 3, 4, 5], [6, None, 8, 9, 10]])
        do_test('c', [[1, 2, None, 4, 5], [6, 7, None, 9, 10]])
        do_test(',e', [[1, 2, 3, 4, None], [6, 7, 8, 9, None]])
        do_test('e', [[1, 2, 3, 4, None], [6, 7, 8, 9, None]])
        do_test('a,b,c,d,e', [])
        do_test('a,', [])  # primary key cannot be skipped, should refuse to import with an error
        do_test('a', [])  # primary key cannot be skipped, should refuse to import with an error

    def test_reading_counters_with_skip_cols(self):
        """
        Test importing a CSV file for a counter table but skipping some columns:

        - create a table
        - create a csv file with all column values
        - import the csv file with skip_columns
        - check only the columns that were not skipped are in the table

        Because COPY FROM for counters is not idempotent we expect that the values inserted continually increase.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testskipcols (
                a int primary key,
                b counter,
                c counter,
                d counter,
                e counter
            )""")

        tempfile = self.get_temp_file()
        data = [[1, 1, 1, 1, 1], [2, 1, 1, 1, 1]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c', 'd', 'e'])
            for a, b, c, d, e in data:
                writer.writerow({'a': a, 'b': b, 'c': c, 'd': d, 'e': e})

        def do_test(skip_cols, expected_results):
            logger.debug("Importing csv file {} with skipcols '{}'".format(tempfile, skip_cols))
            out, err, _ = self.run_cqlsh(cmds="COPY ks.testskipcols FROM '{}' WITH SKIPCOLS = '{}'"
                                         .format(tempfile.name, skip_cols))
            logger.debug(out)
            assert expected_results == sorted(rows_to_list(self.session.execute("SELECT * FROM ks.testskipcols")))

        do_test('c, d ,e', [[1, 1, None, None, None], [2, 1, None, None, None]])
        do_test('b', [[1, 1, 1, 1, 1], [2, 1, 1, 1, 1]])
        do_test('b', [[1, 1, 2, 2, 2], [2, 1, 2, 2, 2]])
        do_test('e', [[1, 2, 3, 3, 2], [2, 2, 3, 3, 2]])

    def test_writing_with_token_boundaries(self):
        """
        Test COPY TO with the begin and end tokens specified in the WITH option by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY TO WITH MINTOKEN AND MAXTOKEN,
        - checking that only the values with the token in the specified range were exported.

        @jira_ticket CASSANDRA-9303
        """
        self._test_writing_with_token_boundaries(10, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(100, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(1000, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(10000, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(10000, 1000000000000000000, 1000000000000000001)
        self._test_writing_with_token_boundaries(10000, None, 2000000000000000000)
        self._test_writing_with_token_boundaries(10000, 1000000000000000000, None)
        self._test_writing_with_token_boundaries(100, 1000000000000000000, 1000000000000000000)
        self._test_writing_with_token_boundaries(100, 2000000000000000000, 1000000000000000000)

    def _test_writing_with_token_boundaries(self, num_records, begin_token, end_token):
        self.prepare(partitioner="murmur3")
        self.session.execute("CREATE TABLE testtokens(a text primary key)")

        insert_statement = self.session.prepare("INSERT INTO testtokens (a) VALUES (?)")
        execute_concurrent_with_args(self.session, insert_statement, [(str(i),) for i in range(num_records)])

        tempfile = self.get_temp_file()
        logger.debug('Exporting tokens {} - {} for {} records to csv file: {}'.format(begin_token, end_token,
                                                                               num_records, tempfile.name))
        cmds = "COPY ks.testtokens TO '{}'".format(tempfile.name)
        if begin_token and end_token:
            cmds += "WITH BEGINTOKEN = '{}' AND ENDTOKEN = '{}'".format(begin_token, end_token)
        elif begin_token:
            cmds += "WITH BEGINTOKEN = '{}'".format(begin_token)
        elif end_token:
            cmds += "WITH ENDTOKEN = '{}'".format(end_token)

        logger.debug(cmds)
        out, err, _ = self.run_cqlsh(cmds=cmds)
        logger.debug(err)
        logger.debug(out)

        max_long = 2 ** 63 - 1
        min_long = -max_long - 1
        if not begin_token:
            begin_token = min_long
        if not end_token:
            end_token = max_long

        tokens = [murmur3(str(i).encode()) for i in range(num_records)]
        result = sorted([(str(i), tokens[i]) for i in range(num_records) if begin_token <= tokens[i] <= end_token])

        with open(tempfile.name, 'r') as csvfile:
            csv_values = sorted([(v[0], tokens[int(v[0])]) for v in csv.reader(csvfile)])

        assert csv_values == result

    def test_reading_max_parse_errors(self):
        """
        Test that importing a csv file is aborted when we reach the maximum number of parse errors:

        - create a table
        - create a csv file with some invalid rows
        - import the csv file
        - check that we import fewer rows that the total number of valid rows and
        that we display the correct message

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testmaxparseerrors (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()
        num_rows = 500000
        max_parse_errors = 10

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
            for i in range(num_rows):
                if i % 2 == 0:
                    writer.writerow({'a': i, 'b': 0, 'c': 'abc'})  # invalid
                else:
                    writer.writerow({'a': i, 'b': 0, 'c': 2.0})  # valid

        logger.debug("Importing csv file {} with {} max parse errors".format(tempfile.name, max_parse_errors))
        out, err, _ = self.run_cqlsh(cmds="COPY ks.testmaxparseerrors FROM '{}' WITH MAXPARSEERRORS='{}'"
                                     .format(tempfile.name, max_parse_errors))

        assert 'Exceeded maximum number of parse errors {}'.format(max_parse_errors) in err
        num_rows_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.testmaxparseerrors"))[0][0]
        logger.debug("Imported {} rows".format(num_rows_imported))
        assert num_rows_imported < (num_rows / 2)  # less than the maximum number of valid rows in the csv

    def test_reading_max_insert_errors(self):
        """
        Test that importing a csv file is aborted when we reach the maximum number of insert errors:

        - create a table
        - create a csv file with some data
        - fail one chunk permanently (via CQLSH_COPY_TEST_FAILURES so that chunk_size rows will fail a # of times higher
          than the maximum number of attempts)
        - import the csv file
        - check that:
          - if chunk_size is bigger than max_insert_errors the import is aborted (we import fewer rows that the total
            number of allowed rows and we display the correct error message)
          - otherwise the import operation completes for all rows except for the failed chunk

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testmaxinserterrors (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()
        num_rows = 10000

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
            for i in range(num_rows):
                writer.writerow({'a': i, 'b': 0, 'c': 2.0})

        failures = {'failing_batch': {'id': 3, 'failures': 2}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)

        def do_test(max_insert_errors, chunk_size):
            self.session.execute("TRUNCATE ks.testmaxinserterrors")
            num_expected_rows = num_rows - chunk_size  # one chunk will fail

            logger.debug("Importing csv file {} with {} max insert errors and chunk size {}"
                  .format(tempfile.name, max_insert_errors, chunk_size))
            # Note: we use one attempt because each attempt counts as a failure
            out, err, _ = self.run_cqlsh(cmds="COPY ks.testmaxinserterrors FROM '{}' WITH MAXINSERTERRORS='{}' "
                                         "AND CHUNKSIZE='{}' AND MAXATTEMPTS='1'"
                                         .format(tempfile.name, max_insert_errors, chunk_size))

            num_rows_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.testmaxinserterrors"))[0][0]
            logger.debug("Imported {}".format(num_rows_imported))
            if max_insert_errors < chunk_size:
                assert 'Exceeded maximum number of insert errors {}'.format(max_insert_errors) in err
                assert num_rows_imported <= num_expected_rows, "{} < {}".format(num_rows_imported, num_expected_rows)
            else:
                assert 'Exceeded maximum number of insert errors {}'.format(max_insert_errors) not in err
                assert 'Failed to process {} rows'.format(chunk_size) in err
                assert num_expected_rows == num_rows_imported

        do_test(50, 100)
        do_test(100, 50)
        do_test(50, 50)

    def test_reading_with_parse_errors(self):
        """
        Test importing a CSV file where not all rows can be parsed:

        - create a table
        - create a csv file with some invalid rows
        - import the csv file
        - check that the valid rows are imported and the invalid rows are saved in a bad file.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testparseerrors (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()

        def do_test(num_chunks, chunk_size, num_failing_per_chunk, err_file):
            invalid_rows = []
            valid_rows = []
            with open(tempfile.name, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
                for i in range(num_chunks):
                    for k in range(chunk_size):
                        if k < num_failing_per_chunk:  # invalid
                            if i == 0 and k == 0:  # fail on a primary key (only once)
                                writer.writerow({'a': 'bb', 'b': k, 'c': 1.0})
                                invalid_rows.append(['bb', str(k), '1.0'])
                            else:  # fail on a value
                                writer.writerow({'a': i, 'b': k, 'c': 'abc'})
                                invalid_rows.append([str(i), str(k), 'abc'])
                        else:
                            writer.writerow({'a': i, 'b': k, 'c': 2.0})  # valid
                            valid_rows.append([str(i), str(k), '2.0'])

            err_file_name = err_file.name if err_file else 'import_ks_testparseerrors.err'
            self.session.execute("TRUNCATE testparseerrors")

            logger.debug("Importing csv file {} with err_file {} and {}/{}/{}"
                  .format(tempfile.name, err_file_name, num_chunks, chunk_size, num_failing_per_chunk))
            cmd = "COPY ks.testparseerrors FROM '{}' WITH CHUNKSIZE={}".format(tempfile.name, chunk_size)
            if err_file:
                cmd += " AND ERRFILE='{}'".format(err_file.name)
            self.run_cqlsh(cmds=cmd)

            results = self.stringify_results(self.session.execute("SELECT * FROM ks.testparseerrors"))
            logger.debug('Checking valid rows')
            assert sorted(valid_rows) == sorted(results)
            logger.debug('Checking invalid rows')
            self.assertCsvResultEqual(err_file_name, invalid_rows, cql_type_names=['text', 'int', 'text'])

        do_test(100, 2, 1, self.get_temp_file())
        do_test(10, 50, 1, self.get_temp_file())
        do_test(10, 100, 10, self.get_temp_file())
        do_test(10, 100, 100, self.get_temp_file())

        # at least two default files to make sure old default err file gets renamed to .YYYYMMDD_HHMMSS
        do_test(100, 2, 1, None)
        do_test(10, 50, 1, None)

    def test_reading_with_wrong_number_of_columns(self):
        """
        Test importing a CSV file where not all rows have the correct number of columns:

        - create a table
        - create a csv file with some invalid rows
        - import the csv file
        - check that the valid rows are imported and the invalid rows are saved in a bad file.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testwrongnumcols (
                a int,
                b int,
                c float,
                d float,
                e float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()
        err_file = self.get_temp_file()

        invalid_rows = []
        valid_rows = []
        with open(tempfile.name, 'w') as csvfile:  # c, d is missing
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'e'])
            writer.writerow({'a': 0, 'b': 0, 'e': 1})
            invalid_rows.append(['0', '0', '1'])

            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c', 'd', 'e'])
            for i in range(1, 100):
                writer.writerow({'a': i, 'b': i, 'c': 2.0, 'd': 3.0, 'e': 4.0})
                valid_rows.append([i, i, 2.0, 3.0, 4.0])

        logger.debug("Importing csv file {} with err_file {}".format(tempfile.name, err_file.name))
        cmd = "COPY ks.testwrongnumcols FROM '{}' WITH ERRFILE='{}'".format(tempfile.name, err_file.name)
        self.run_cqlsh(cmds=cmd)

        results = rows_to_list(self.session.execute("SELECT * FROM ks.testwrongnumcols"))
        logger.debug('Checking valid rows')
        assert sorted(valid_rows) == sorted(results)
        logger.debug('Checking invalid rows')
        self.assertCsvResultEqual(err_file.name, invalid_rows, 'testwrongnumcols', columns=['a', 'b', 'e'])

        os.unlink(err_file.name)

    def test_reading_with_multiple_files(self):
        """
        Test importing multiple CSV files

        - create a table
        - create a several csv files
        - import the csv files
        - check that all rows were imported

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testmultifiles (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        num_rows_per_file = 100
        num_files = 10
        tempfiles = []

        for i in range(num_files):
            tempfiles.append(self.get_temp_file(prefix='testreadmult{}'.format(i), suffix='.csv'))

        for i in range(num_files):
            with open(tempfiles[i].name, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
                for k in range(num_rows_per_file):
                    writer.writerow({'a': i, 'b': k, 'c': 2.0})

        def import_and_check(temp_files_str):
            self.session.execute("TRUNCATE testmultifiles")

            logger.debug("Importing csv files {}".format(temp_files_str))
            self.run_cqlsh(cmds="COPY ks.testmultifiles FROM '{}'".format(temp_files_str))

            assert [[num_rows_per_file * len(tempfiles)]] == rows_to_list(self.session.execute("SELECT COUNT(*) FROM testmultifiles"))

        import_and_check(','.join([tempfile.name for tempfile in tempfiles]))
        import_and_check(os.path.join(gettempdir(), 'testreadmult*.csv'))
        import_and_check(','.join([os.path.join(gettempdir(), 'testreadmult[0-4]*.csv'),
                                   os.path.join(gettempdir(), 'testreadmult[5-9]*.csv')]))

    def test_writing_with_max_output_size(self):
        """
        Test writing to multiple CSV files:

        - create a table and populate it with some data
        - export the data with maxoutputsize
        - check that the correct number of CSV files has been created and that
          they have the expected number of lines

        @jira_ticket CASSANDRA-9303
        """
        num_records = 10000
        stress_table = 'keyspace1.standard1'

        self.prepare()
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        def do_test(max_size, header):
            tempfile = self.get_temp_file(prefix='testwritemult', suffix='.csv')
            logger.debug('Exporting to csv file: {} with max size {} and header {}'
                  .format(tempfile.name, max_size, header))
            cmd = "COPY {} TO '{}' WITH MAXOUTPUTSIZE='{}'".format(stress_table, tempfile.name, max_size)
            if header:
                cmd += " AND HEADER='True'"
            self.run_cqlsh(cmds=cmd)

            output_files = glob.glob(os.path.join(gettempdir(), 'testwritemult*.csv*'))
            num_lines = []
            for f in output_files:
                num_lines.append(len(open(os.path.join(gettempdir(), f)).readlines()))
                os.unlink(f)

            num_expected_files = int(num_records / max_size) if (num_records % max_size == 0) else int(num_records / max_size) + 1
            assert num_expected_files == len(output_files)
            if header:
                assert (num_records + 1) == sum(num_lines)
            else:
                assert num_records == sum(num_lines)

            for i, n in enumerate(sorted(num_lines, reverse=True)):
                if i < int(num_records / max_size):
                    num_expected_lines = max_size + 1 if (i == 0 and header) else max_size
                    assert num_expected_lines == n
                else:
                    assert (num_records % max_size) == n

        do_test(1000, False)
        do_test(1000, True)
        do_test(900, False)
        do_test(500, False)
        do_test(100, False)

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

        tempfile = self.get_temp_file()

        self.run_cqlsh("COPY ks.testorder (a, c, b) TO '{name}'".format(name=tempfile.name))

        reference_file = self.get_temp_file()
        with open(reference_file.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])

        assert_csvs_items_equal(tempfile.name, reference_file.name)

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

        tempfile = self.get_temp_file()
        write_rows_to_csv(tempfile.name, data)

        self.run_cqlsh("COPY ks.testorder (a, c, b) FROM '{name}'".format(name=tempfile.name))

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testorder")
        results = self.parse_cqlsh_query(out=out, num_cols=3)

        reference_file = self.get_temp_file()
        with open(reference_file.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])

        self.assertCsvResultEqual(reference_file.name, results, 'testorder')

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

        tempfile = self.get_temp_file()
        write_rows_to_csv(tempfile.name, data)

        stmt = ("""COPY ks.testquoted ("IdNumber", "select") FROM '{name}'"""
                if specify_column_names else
                """COPY ks.testquoted FROM '{name}'""").format(name=tempfile.name)

        self.run_cqlsh(stmt)

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testquoted")
        results = self.parse_cqlsh_query(out=out, num_cols=2)

        self.assertCsvResultEqual(tempfile.name, results, 'testquoted')

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

    def test_quoted_column_names_writing(self):
        """
        Test that COPY can write to a table with quoted
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

        for specify_column_names in (True, False):
            tempfile = self.get_temp_file()
            stmt = ("""COPY ks.testquoted ("IdNumber", "select") TO '{name}'"""
                    if specify_column_names else
                    """COPY ks.testquoted TO '{name}'""").format(name=tempfile.name)
            self.run_cqlsh(stmt)

            reference_file = self.get_temp_file()
            write_rows_to_csv(reference_file.name, data)

            assert_csvs_items_equal(tempfile.name, reference_file.name)

    def test_data_validation_on_read_template(self):
        """
        Test that reading from CSV files fails when there is a type mismatch
        between the value being loaded and the type of the column or when data is missing
        in the file. Perform the following:

        - create a table,
        - write a CSV file containing the data passed in as parameter
        - COPY that csv file into the table

        If expected_err is not None, this test will succeed when the COPY command fails and it
        returns a matching error.
        If expected_err is None, this test will succeed when the COPY command prints
        no errors and the table matches the loaded CSV file.

        @jira_ticket CASSANDRA-9302
        @jira_ticket CASSANDRA-10854
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testvalidate (
                a int,
                b int,
                c int,
                PRIMARY KEY(a, b)
            )""")

        data_err_pairs = [
            # sanity check that the test works
            ([[1, 1, 2]], None),

            # test copying a float to an int column
            ([[1, 1, 2.14]], 'Failed to import'),

            # test copying a uuid to an int column
            ([[1, 1, uuid4()]], 'Failed to import'),

            # test copying a text value to an int column
            ([[1, 1, 'test']], 'Failed to import'),

            # test using an empty partition key
            ([['', 1, 'test']], "Failed to import 1 rows: ParseError - Cannot insert null value for primary key column"),

            # test using an empty clustering key
            ([[1, '', 'test']], "Failed to import 1 rows: ParseError - Cannot insert null value for primary key column"),
        ]

        for (data, expected_err) in data_err_pairs:
            self.session.execute("TRUNCATE testvalidate")

            tempfile = self.get_temp_file()
            logger.debug('Writing {}'.format(tempfile.name))
            write_rows_to_csv(tempfile.name, data)

            cmd = """COPY ks.testvalidate (a, b, c) FROM '{name}'""".format(name=tempfile.name)
            # We want to assert that there is no error when we don't expect one but cqlsh prints
            # some debug messages to stderr, hence we must turn debug off
            out, err, _ = self.run_cqlsh(cmd, use_debug=False)
            results = self.stringify_results(self.session.execute("SELECT * FROM testvalidate"))

            if expected_err:
                assert expected_err in err
                assert not results
            else:
                assert not err
                self.assertCsvResultEqual(tempfile.name, results, 'testvalidate')

    @since('2.2')
    def test_read_wrong_column_names(self):
        """
        Test that if the wrong column name is specified in the COPY FROM command,
        then an appropriate error is returned by:

        - creating a table,
        - write a CSV file containing some data
        - COPY that csv file into the table with an incorrect column name

        @jira_ticket CASSANDRA-11333
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testwrongcolumns (
                a int,
                b int,
                c int,
                PRIMARY KEY(a, b)
            )""")

        tempfile = self.get_temp_file()
        logger.debug('Writing {}'.format(tempfile.name))
        write_rows_to_csv(tempfile.name, [[1, 1, 1]])

        cmd = """COPY ks.testwrongcolumns (a, b, d) FROM '{}'""".format(tempfile.name)
        out, err, _ = self.run_cqlsh(cmd)
        logger.debug(out)
        logger.debug(err)
        results = list(self.session.execute("SELECT * FROM testwrongcolumns"))

        assert 'Invalid column name d' in err
        assert 'child process(es) died unexpectedly' not in err
        assert not results

    def test_all_datatypes_write(self):
        """
        Test that, after COPYing a table containing all CQL datatypes to a CSV
        file, that the table contains the same values as the CSV by:

        - creating and populating a table containing all datatypes,
        - COPYing the contents of that table to a CSV file, and
        - asserting that the CSV file contains the same data as the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, za)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        def _test(prepared_statements):
            tempfile = self.get_temp_file()
            logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
            self.run_cqlsh(cmds="COPY ks.testdatatype TO '{}' WITH PREPAREDSTATEMENTS = {}"
                           .format(tempfile.name, prepared_statements))

            out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testdatatype")
            results = self.parse_cqlsh_query(out=out, num_cols=len(self.data), timestamps_to_be_rounded=[10, 17])

            self.assertCsvResultEqual(tempfile.name, results, 'testdatatype')

        _test(True)
        _test(False)

    def test_all_datatypes_read(self):
        """
        Test that, after COPYing a CSV file to a table containing all CQL
        datatypes, that the table contains the same values as the CSV by:

        - creating a table containing all datatypes,
        - writing a corresponding CSV file containing each datatype,
        - COPYing the CSV file into the table, and
        - asserting that the CSV file contains the same data as the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        tempfile = self.get_temp_file()

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            # serializing blob bytearray in friendly format
            data_set = list(self.data)

            data_set[2] = self.format_blob(self.data[2])
            # Here we convert containers of blobs to strings that match exactly the output of the SELECT *
            # because otherwise the comparison fails due to extra quotes added by the csv writer around the blobs
            # that were converted to strings. White spaces do matter
            data_set[24] = '{3: ' + self.format_blob(self.data[24][3]) + '}'
            data_set[25] = '[' + ', '.join(self.format_blob(b) for b in self.data[25]) + ']'
            data_set[26] = '{' + ', '.join(self.format_blob(b) for b in self.data[26]) + '}'
            writer.writerow(data_set)

        def _test(prepared_statements):
            logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
            out, err, _ = self.run_cqlsh(cmds="COPY ks.testdatatype FROM '{}' WITH PREPAREDSTATEMENTS = {}"
                           .format(tempfile.name, prepared_statements))

            out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testdatatype")
            results = self.parse_cqlsh_query(out=out, num_cols=len(self.data), timestamps_to_be_rounded=[10, 17])

            self.assertCsvResultEqual(tempfile.name, results, 'testdatatype')

        _test(True)
        _test(False)

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

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, za)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.run_cqlsh(cmds="COPY ks.testdatatype TO '{}'".format(tempfile.name))

        exported_results = list(self.session.execute("SELECT * FROM testdatatype"))

        def _test(prepared_statements):
            self.session.execute('TRUNCATE ks.testdatatype')

            self.run_cqlsh(cmds="COPY ks.testdatatype FROM '{}' WITH PREPAREDSTATEMENTS = {}"
                           .format(tempfile.name, prepared_statements))

            imported_results = list(self.session.execute("SELECT * FROM testdatatype"))

            assert exported_results == imported_results

        _test(True)
        _test(False)

    def test_boolstyle_round_trip(self):
        """
        Test that a CSV file with booleans in a different style successfully round-trips
        via COPY by:

        - creating and populating a table containing boolean values,
        - copying that table to a CSV file,
        - checking the contents of the CSV,
        - truncating the table,
        - copying the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.

        @jira_ticket CASSANDRA-9303
        """
        def do_round_trip(trueval, falseval, invalid=False):
            logger.debug('Exporting to csv file: {} with bool style {},{}'.format(tempfile.name, trueval, falseval))
            _, err, _ = self.run_cqlsh(cmds="COPY ks.testbooleans TO '{}' WITH BOOLSTYLE='{}, {}'"
                                       .format(tempfile.name, trueval, falseval))
            if invalid:
                expected_err = "Invalid boolean styles [{}, {}]".format(
                    ', '.join(["'{}'".format(s.strip()) for s in trueval.split(',')]),
                    ', '.join(["'{}'".format(s.strip()) for s in falseval.split(',')]))
                assert expected_err in err
                return

            tempfile_rows_as_list = list(csv_rows(tempfile.name))
            assert [['0', falseval], ['1', trueval]] == sorted(tempfile_rows_as_list)
            exported_results = list(self.session.execute("SELECT * FROM testbooleans"))

            logger.debug('Importing from csv file: {}'.format(tempfile.name))
            self.session.execute('TRUNCATE ks.testbooleans')
            self.run_cqlsh(cmds="COPY ks.testbooleans FROM '{}' WITH BOOLSTYLE='{}, {}'"
                           .format(tempfile.name, trueval, falseval))

            imported_results = list(self.session.execute("SELECT * FROM testbooleans"))
            assert sorted(exported_results) == sorted(imported_results)

        self.prepare()
        self.session.execute("""
            CREATE TABLE testbooleans (
                a int PRIMARY KEY,
                b boolean
            )""")

        insert_statement = self.session.prepare("INSERT INTO testbooleans (a, b) VALUES (?, ?)")
        self.session.execute(insert_statement, [0, False])
        self.session.execute(insert_statement, [1, True])
        tempfile = self.get_temp_file()

        do_round_trip('True', 'False')
        do_round_trip('TRUE', 'FALSE')
        do_round_trip('yes', 'no')
        do_round_trip('1', '0')
        do_round_trip('TRUE', 'no')
        do_round_trip('True', '0')

        do_round_trip('TRUE', 'TRUE', invalid=True)
        do_round_trip('TRUE', '', invalid=True)
        do_round_trip('', 'FALSE', invalid=True)
        do_round_trip('', '', invalid=True)
        do_round_trip('yes, no', 'maybe', invalid=True)

    def test_number_separators_round_trip(self):
        """
        Test that a CSV file containing numbers with decimal and thousands separators in a different format
        successfully round-trips via COPY by:

        - creating and populating a table containing a numbers,
        - copying that table to a CSV file,
        - checking the contents of the CSV,
        - truncating the table,
        - copying the written CSV file back into the table, and
        - asserting that the previously selected contents of the table match the
        current contents of the table.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()

        if self.cluster.version() < '2.2':
            self.session.execute("""
                CREATE TABLE testnumberseps (
                    a int PRIMARY KEY,
                    b int,
                    c bigint,
                    d varint,
                    e decimal,
                    f float,
                    g double
                )""")

            insert_statement = self.session.prepare("INSERT INTO testnumberseps (a, b, c, d, e, f, g)"
                                                    " VALUES (?, ?, ?, ?, ?, ?, ?)")
            self.session.execute(insert_statement, [0, 10, 10, 10, Decimal(10), 10, 10])
            self.session.execute(insert_statement, [1, 1000, 1000, 1000, Decimal(5.5), 5.5, 5.12345678])
            self.session.execute(insert_statement, [2, 1000000, 1000000, 1000000, Decimal("0.001"), 0.001, 0.001])
            self.session.execute(insert_statement, [3, 1000000005, 1000000005, 1000000005,
                                                    Decimal("1234.56"), 1234.56, 123456789.56])
            self.session.execute(insert_statement, [4, -1000000005, -1000000005, -1000000005,
                                                    Decimal("-1234.56"), -1234.56, -1234.56])
            self.session.execute(insert_statement, [1000000, 0, 0, 0, Decimal(0), 0, 0])

            # comma as thousands sep and dot as decimal sep
            expected_vals_usual = [
                ['0', '10', '10', '10', '10', '10', '10'],
                ['1', '1,000', '1,000', '1,000', '5.5', '5.5', '5.12346'],
                ['2', '1,000,000', '1,000,000', '1,000,000', '0.001', '0.001', '0.001'],
                ['3', '1,000,000,005', '1,000,000,005', '1,000,000,005', '1,234.56', '1,234.56006', '123,456,789.56'],
                ['4', '-1,000,000,005', '-1,000,000,005', '-1,000,000,005', '-1,234.56', '-1,234.56006', '-1,234.56'],
                ['1,000,000', '0', '0', '0', '0', '0', '0']
            ]

            # dot as thousands sep and comma as decimal sep
            expected_vals_inverted = [
                ['0', '10', '10', '10', '10', '10', '10'],
                ['1', '1.000', '1.000', '1.000', '5,5', '5,5', '5,12346'],
                ['2', '1.000.000', '1.000.000', '1.000.000', '0,001', '0,001', '0,001'],
                ['3', '1.000.000.005', '1.000.000.005', '1.000.000.005', '1.234,56', '1.234,56006', '123.456.789,56'],
                ['4', '-1.000.000.005', '-1.000.000.005', '-1.000.000.005', '-1.234,56', '-1.234,56006', '-1.234,56'],
                ['1.000.000', '0', '0', '0', '0', '0', '0']
            ]

        else:
            self.session.execute("""
                CREATE TABLE testnumberseps (
                    a int PRIMARY KEY,
                    b tinyint,
                    c smallint,
                    d int,
                    e bigint,
                    f varint,
                    g decimal,
                    h float,
                    i double
                )""")

            insert_statement = self.session.prepare("INSERT INTO testnumberseps (a, b, c, d, e, f, g, h, i)"
                                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            self.session.execute(insert_statement, [0, 10, 10, 10, 10, 10, Decimal(10), 10, 10])
            self.session.execute(insert_statement, [1, 127, 255, 1000, 1000, 1000, Decimal(5.5), 5.5, 5.12345678])
            self.session.execute(insert_statement, [2, 127, 255, 1000000, 1000000, 1000000,
                                                    Decimal("0.001"), 0.001, 0.001])
            self.session.execute(insert_statement, [3, 127, 255, 1000000005, 1000000005, 1000000005,
                                                    Decimal("1234.56"), 1234.56, 123456789.56])
            self.session.execute(insert_statement, [4, 127, 255, -1000000005, -1000000005, -1000000005,
                                                    Decimal("-1234.56"), -1234.56, -1234.56])
            self.session.execute(insert_statement, [1000000, 0, 0, 0, 0, 0, Decimal(0), 0, 0])

            # comma as thousands sep and dot as decimal sep
            # the precision for double values was increased from 5 to 12 in 3.6, see CASSANDRA-11255
            double_val_1 = '5.12346' if self.cluster.version() < LooseVersion('3.6') else '5.12345678'
            double_val_2 = '123,456,789.56' if self.cluster.version() < LooseVersion('3.6') else '123,456,789.560000002384'
            expected_vals_usual = [
                ['0', '10', '10', '10', '10', '10', '10', '10', '10'],
                ['1', '127', '255', '1,000', '1,000', '1,000', '5.5', '5.5', double_val_1],
                ['2', '127', '255', '1,000,000', '1,000,000', '1,000,000', '0.001', '0.001', '0.001'],
                ['3', '127', '255', '1,000,000,005', '1,000,000,005', '1,000,000,005',
                 '1,234.56', '1,234.56006', double_val_2],
                ['4', '127', '255', '-1,000,000,005', '-1,000,000,005', '-1,000,000,005',
                 '-1,234.56', '-1,234.56006', '-1,234.56'],
                ['1,000,000', '0', '0', '0', '0', '0', '0', '0', '0']
            ]

            # dot as thousands sep and comma as decimal sep
            double_val_1 = '5,12346' if self.cluster.version() < LooseVersion('3.6') else '5,12345678'
            double_val_2 = '123.456.789,56' if self.cluster.version() < LooseVersion('3.6') else '123.456.789,560000002384'
            expected_vals_inverted = [
                ['0', '10', '10', '10', '10', '10', '10', '10', '10'],
                ['1', '127', '255', '1.000', '1.000', '1.000', '5,5', '5,5', double_val_1],
                ['2', '127', '255', '1.000.000', '1.000.000', '1.000.000', '0,001', '0,001', '0,001'],
                ['3', '127', '255', '1.000.000.005', '1.000.000.005', '1.000.000.005',
                 '1.234,56', '1.234,56006', double_val_2],
                ['4', '127', '255', '-1.000.000.005', '-1.000.000.005', '-1.000.000.005',
                 '-1.234,56', '-1.234,56006', '-1.234,56'],
                ['1.000.000', '0', '0', '0', '0', '0', '0', '0', '0']
            ]

        tempfile = self.get_temp_file()

        def do_test(expected_vals, thousands_sep, decimal_sep):
            logger.debug('Exporting to csv file: {} with thousands_sep {} and decimal_sep {}'
                  .format(tempfile.name, thousands_sep, decimal_sep))
            self.run_cqlsh(cmds="COPY ks.testnumberseps TO '{}' WITH THOUSANDSSEP='{}' AND DECIMALSEP='{}'"
                           .format(tempfile.name, thousands_sep, decimal_sep))

            out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testnumberseps")
            exported_results = self.parse_cqlsh_query(out=out, num_cols=len(expected_vals[0]))

            self.maxDiff = None
            assert sorted(expected_vals) == sorted(list(csv_rows(tempfile.name)))

            logger.debug('Importing from csv file: {} with thousands_sep {} and decimal_sep {}'
                  .format(tempfile.name, thousands_sep, decimal_sep))
            self.session.execute('TRUNCATE ks.testnumberseps')
            self.run_cqlsh(cmds="COPY ks.testnumberseps FROM '{}' WITH THOUSANDSSEP='{}' AND DECIMALSEP='{}'"
                           .format(tempfile.name, thousands_sep, decimal_sep))

            out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testnumberseps")
            imported_results = self.parse_cqlsh_query(out=out, num_cols=len(expected_vals[0]))

            assert len(expected_vals) == len(imported_results)

            assert sorted(exported_results) == sorted(imported_results)

        do_test(expected_vals_usual, ',', '.')
        do_test(expected_vals_inverted, '.', ',')

    @since('3.4')
    def test_round_trip_with_sub_second_precision(self):
        """
        Test that we can import and export timestamp values with millisecond precision:

        - create a csv file and import it
        - export the data and check the values are as expected

        @jira_ticket CASSANDRA-10428
        """
        self.prepare()
        self.session.execute("create TABLE testsubsecond(id int PRIMARY KEY, subid timestamp)")

        tempfile1 = self.get_temp_file()
        tempfile2 = self.get_temp_file()
        with open(tempfile1.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([1, '1943-06-19 11:21:01+0000'])
            writer.writerow([2, '1943-06-19 11:21:01.123+0000'])
            writer.writerow([3, '1943-06-19 11:21:01.123456+0000'])

        logger.debug('Importing from csv file: {}'.format(tempfile1.name))
        self.run_cqlsh(cmds="COPY ks.testsubsecond FROM '{}'".format(tempfile1.name))

        logger.debug('Exporting to csv file: {}'.format(tempfile2.name))
        self.run_cqlsh(cmds="COPY ks.testsubsecond TO '{}'".format(tempfile2.name))

        csv_results = sorted(list(csv_rows(tempfile2.name)))
        assert [['1', '1943-06-19 11:21:01.000+0000'],
                               ['2', '1943-06-19 11:21:01.123+0000'],
                               ['3', '1943-06-19 11:21:01.124+0000']] == csv_results

    @since('3.6')
    def test_round_trip_with_different_number_precision(self):
        """
        Test that we can import and export double and float values with a default precision (12 for doubles
        and 5 for floats) or with a precision as specified by the user:

        - create a csv file and import it
        - export the data to another csv file
        - check the first and last csv file contents match
        - repeat with different precisions

        @jira_ticket CASSANDRA-11255
        """
        self.prepare()
        self.session.execute("create TABLE testfloatprecision(id int PRIMARY KEY, val1 float, val2 double)")

        def do_test(float_precision, double_precision):
            tempfile1 = self.get_temp_file()
            tempfile2 = self.get_temp_file()
            float_format_str = "{{0:.{}g}}".format(float_precision if float_precision is not None else 5)
            double_format_str = "{{0:.{}g}}".format(double_precision if double_precision is not None else 12)

            with open(tempfile1.name, 'w') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([1, float_format_str.format(1.12345), double_format_str.format(1.123456789123)])

            logger.debug('Importing from {}'.format(tempfile1.name))
            self.run_cqlsh(cmds="COPY ks.testfloatprecision FROM '{}'".format(tempfile1.name))

            cmd = "COPY ks.testfloatprecision TO '{}'".format(tempfile2.name)
            if double_precision is not None or float_precision is not None:
                cmd += " WITH"
            if double_precision is not None:
                cmd += " DOUBLEPRECISION={}".format(double_precision)
                if float_precision is not None:
                    cmd += " AND"
            if float_precision is not None:
                cmd += " FLOATPRECISION={}".format(float_precision)

            logger.debug('Exporting to {} with {}'.format(tempfile2.name, cmd))
            self.run_cqlsh(cmds=cmd)

            assert sorted(list(csv_rows(tempfile1.name))) == sorted(list(csv_rows(tempfile2.name)))

        do_test(None, None)
        do_test(None, 10)
        do_test(3, None)
        do_test(0, 0)
        do_test(1, 1)
        do_test(3, 3)
        do_test(5, 5)
        do_test(5, 12)
        do_test(5, 15)

    def test_round_trip_with_num_processes(self):
        """
        Test exporting a large number of rows into a csv file with a fixed number of child processes.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()

        num_records = 10000
        num_processes = 4
        stress_table = 'keyspace1.standard1'

        logger.debug('Running stress without any user profile')
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {}'.format(tempfile.name))
        out, _, _ = self.run_cqlsh(cmds="COPY {} TO '{}' WITH NUMPROCESSES='{}'"
                                   .format(stress_table, tempfile.name, num_processes))
        logger.debug(out)
        assert 'Using {} child processes'.format(num_processes) in out
        assert num_records == len(open(tempfile.name).readlines())

        self.session.execute("TRUNCATE {}".format(stress_table))
        logger.debug('Importing from csv file: {}'.format(tempfile.name))
        out, _, _ = self.run_cqlsh(cmds="COPY {} FROM '{}' WITH NUMPROCESSES='{}'"
                                   .format(stress_table, tempfile.name, num_processes))
        logger.debug(out)
        assert 'Using {} child processes'.format(num_processes) in out
        assert [[num_records]] == rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}"
                                                                            .format(stress_table)))

    def test_round_trip_with_rate_file(self):
        """
        Test a round trip with a large number of rows and a rate file. Make sure the rate file contains
        output statistics

        @jira_ticket CASSANDRA-9303
        """
        num_rows = 200000
        report_frequency = 0.1  # every 100 milliseconds
        stress_table = 'keyspace1.standard1'
        ratefile = self.get_temp_file()
        tempfile = self.get_temp_file()

        def check_rate_file():
            lines = [line.rstrip('\n') for line in open(ratefile.name)]
            logger.debug(lines)
            assert lines[-1].startswith('Processed: {} rows;'.format(num_rows))

        self.prepare()

        logger.debug('Running stress')
        self.node1.stress(['write', 'n={}'.format(num_rows), 'no-warmup', '-rate', 'threads=50'])

        logger.debug('Exporting to csv file: {}'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} TO '{}' WITH RATEFILE='{}' AND REPORTFREQUENCY='{}'"
                       .format(stress_table, tempfile.name, ratefile.name, report_frequency))

        # check all records were exported
        assert num_rows == len(open(tempfile.name).readlines())

        check_rate_file()

        # clean-up
        os.unlink(ratefile.name)
        self.session.execute("TRUNCATE {}".format(stress_table))

        logger.debug('Importing from csv file: {}'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} FROM '{}' WITH RATEFILE='{}' AND REPORTFREQUENCY='{}'"
                       .format(stress_table, tempfile.name, ratefile.name, report_frequency))

        # check all records were imported
        assert [[num_rows]] == rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}"
                                                                         .format(stress_table)))

        check_rate_file()

    def test_copy_options_from_config_file(self):
        """
        Test that we can specify configuration options in a config file, optionally using multiple sections,
        and that we can still overwrite options from the command line.
        We must pass the debug flag --debug to cqlsh so that we can retrieve options from stdout in order to check,
        see maybe_read_config_file() in copy.py.

        @jira_ticket CASSANDRA-9303
        """
        tempfile = self.get_temp_file()
        self.prepare(nodes=1)

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n=1K', 'no-warmup', '-rate', 'threads=50'])

        def create_config_file(config_lines):
            config_file = self.get_temp_file()
            logger.debug('Creating config file {}'.format(config_file.name))

            with open(config_file.name, 'w') as config:
                for line in config_lines:
                    config.write(line + os.linesep)
                config.close()

            return config_file.name

        def extract_options(out):
            prefix = 'Using options: '
            for l in out.split('\n'):
                if l.startswith(prefix):
                    return l[len(prefix):].strip().strip("'").replace("'", "\"")
            return ''

        def check_options(out, expected_options):
            opts = extract_options(out)
            logger.debug('Options: {}'.format(opts))
            d = json.loads(opts)
            for k, v in expected_options:
                assert v == d[k]

        def do_test(config_lines, expected_options):
            config_file = create_config_file(config_lines)

            cmd = "COPY {} {} '{}'".format(stress_table, direction, tempfile.name)
            if not use_default:
                cmd += " WITH CONFIGFILE = '{}'".format(config_file)

            cqlsh_options = []
            if use_default:
                cqlsh_options.append('--cqlshrc={}'.format(config_file))

            logger.debug('{} with options {}'.format(cmd, cqlsh_options))
            out, _, _ = self.run_cqlsh(cmds=cmd, cqlsh_options=cqlsh_options, skip_cqlshrc=True)
            logger.debug(out)
            check_options(out, expected_options)

        for use_default in [True, False]:
            for direction in ['TO', 'FROM']:
                do_test(['[copy]', 'header = True', 'maxattempts = 10'],
                        [('header', 'True'), ('maxattempts', '10')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy:{}]'.format(stress_table), 'maxattempts = 9'],
                        [('header', 'True'), ('maxattempts', '9')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy-from]', 'maxattempts = 9',
                         '[copy-to]', 'maxattempts = 8'],
                        [('header', 'True'), ('maxattempts', '8' if direction == 'TO' else '9')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy-from]', 'maxattempts = 9',
                         '[copy-to]', 'maxattempts = 8',
                         '[copy:{}]'.format(stress_table), 'maxattempts = 7'],
                        [('header', 'True'), ('maxattempts', '7')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy-from]', 'maxattempts = 9',
                         '[copy-to]', 'maxattempts = 8',
                         '[copy:{}]'.format(stress_table), 'maxattempts = 7',
                         '[copy-from:{}]'.format(stress_table), 'maxattempts = 6',
                         '[copy-to:{}]'.format(stress_table), 'maxattempts = 5'],
                        [('header', 'True'), ('maxattempts', '5' if direction == 'TO' else '6')])

    def test_wrong_number_of_columns(self):
        """
        Test that a COPY statement will fail when trying to import from a CSV
        file with the wrong number of columns by:

        - creating a table with a single column,
        - writing a CSV file with two columns,
        - attempting to COPY the CSV file into the table, and
        - asserting that the COPY operation failed.

        @jira_ticket CASSANDRA-9302
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcolumns (
                a int PRIMARY KEY,
                b int
            )""")

        data = [[1, 2, 3]]
        tempfile = self.get_temp_file()
        write_rows_to_csv(tempfile.name, data)

        logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
        out, err, _ = self.run_cqlsh("COPY ks.testcolumns FROM '{name}'".format(name=tempfile.name))

        assert not self.session.execute("SELECT * FROM testcolumns")
        assert 'Failed to import' in err

    def _test_round_trip(self, nodes, partitioner, num_records=10000):
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
        self.prepare(nodes=nodes, partitioner=partitioner)
        self.session.execute("""
            CREATE TABLE testcopyto (
                a text PRIMARY KEY,
                b int,
                c float,
                d uuid
            )""")

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(str(i), i, float(i) + 0.5, uuid4()) for i in range(num_records)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testcopyto"))

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {}'.format(tempfile.name))
        out, err, _ = self.run_cqlsh(cmds="COPY ks.testcopyto TO '{}'".format(tempfile.name))
        logger.debug(out)

        # check all records were exported
        assert num_records == sum(1 for line in open(tempfile.name))

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        logger.debug('Importing from csv file: {}'.format(tempfile.name))
        out, err, _ = self.run_cqlsh(cmds="COPY ks.testcopyto FROM '{}'".format(tempfile.name))
        logger.debug(out)

        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        assert sorted(results) == sorted(new_results)

    def test_round_trip_murmur3(self):
        self._test_round_trip(nodes=3, partitioner="murmur3")

    def test_round_trip_random(self):
        self._test_round_trip(nodes=3, partitioner="random")

    def test_round_trip_order_preserving(self):
        self._test_round_trip(nodes=3, partitioner="order")

    def test_round_trip_byte_ordered(self):
        self._test_round_trip(nodes=3, partitioner="byte")

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

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {name}'.format(name=tempfile.name))

        commandfile = self.get_temp_file()
        with open(commandfile.name, 'w') as f:
            f.write('USE ks;\n')
            f.write("COPY ks.testcopyto TO '{name}' WITH HEADER=false;".format(name=tempfile.name))

        self.run_cqlsh(cmds="SOURCE '{name}'".format(name=commandfile.name))

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))

        commandfile = self.get_temp_file()
        with open(commandfile.name, 'w') as f:
            f.write('USE ks;\n')
            f.write("COPY ks.testcopyto FROM '{name}' WITH HEADER=false;".format(name=tempfile.name))

        self.run_cqlsh(cmds="SOURCE '{name}'".format(name=commandfile.name))
        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        assert sorted(results) == sorted(new_results)

    def _test_bulk_round_trip(self, nodes, partitioner,
                              num_operations, profile=None,
                              stress_table='keyspace1.standard1',
                              configuration_options=None,
                              skip_count_checks=False,
                              copy_to_options=None,
                              copy_from_options=None):
        """
        Test exporting a large number of rows into a csv file.

        If skip_count_checks is True then it means we cannot use "SELECT COUNT(*)" as it may time out but
        it also means that we can be sure that one cassandra-stress operation is one record and hence
        num_records=num_operations.

        Perform the following:
        - create the records with cassandra-stress
        - export the records to a csv file
        - truncate the table and import the csv file
        - export the records to another csv file
        - check that the length of the two csv files is the same

        Therefore, 3 COPY operations are run in total. Return a list of tuples, containing stdout and stderr
        for all 3 copy operations.
        """
        if configuration_options is None:
            configuration_options = {}
        if copy_to_options is None:
            copy_to_options = {}

        # The default truncate timeout of 10 seconds that is set in init_default_config() is not
        # enough for truncating larger tables, see CASSANDRA-11157
        if 'truncate_request_timeout_in_ms' not in configuration_options:
            configuration_options['truncate_request_timeout_in_ms'] = 60000

        self.prepare(nodes=nodes, partitioner=partitioner, configuration_options=configuration_options)

        ret = []

        def create_records():
            if not profile:
                logger.debug('Running stress without any user profile')
                self.node1.stress(['write', 'n={} cl=ALL'.format(num_operations), 'no-warmup', '-rate', 'threads=50'])
            else:
                logger.debug('Running stress with user profile {}'.format(profile))
                self.node1.stress(['user', 'profile={}'.format(profile), 'ops(insert=1)',
                                   'n={} cl=ALL'.format(num_operations), 'no-warmup', '-rate', 'threads=50'])

            if skip_count_checks:
                return num_operations
            else:
                count_statement = SimpleStatement("SELECT COUNT(*) FROM {}".format(stress_table), consistency_level=ConsistencyLevel.ALL,
                                                  retry_policy=FlakyRetryPolicy(max_retries=3))
                ret = rows_to_list(self.session.execute(count_statement))[0][0]
                logger.debug('Generated {} records'.format(ret))
                assert ret >= num_operations, 'cassandra-stress did not import enough records'
                return ret

        def run_copy_to(filename):
            logger.debug('Exporting to csv file: {}'.format(filename.name))
            start = datetime.datetime.now()
            copy_to_cmd = "CONSISTENCY ALL; COPY {} TO '{}'".format(stress_table, filename.name)
            if copy_to_options:
                copy_to_cmd += ' WITH ' + ' AND '.join('{} = {}'.format(k, v) for k, v in copy_to_options.items())
            logger.debug('Running {}'.format(copy_to_cmd))
            result = self.run_cqlsh(cmds=copy_to_cmd)
            ret.append(result)
            logger.debug("COPY TO took {} to export {} records".format(datetime.datetime.now() - start, num_records))

        def run_copy_from(filename):
            logger.debug('Importing from csv file: {}'.format(filename.name))
            start = datetime.datetime.now()
            copy_from_cmd = "COPY {} FROM '{}'".format(stress_table, filename.name)
            if copy_from_options:
                copy_from_cmd += ' WITH ' + ' AND '.join('{} = {}'.format(k, v) for k, v in copy_from_options.items())
            logger.debug('Running {}'.format(copy_from_cmd))
            result = self.run_cqlsh(cmds=copy_from_cmd)
            ret.append(result)
            logger.debug("COPY FROM took {} to import {} records".format(datetime.datetime.now() - start, num_records))

        num_records = create_records()

        # Copy to the first csv files
        tempfile1 = self.get_temp_file()
        run_copy_to(tempfile1)

        # check all records generated were exported
        with io.open(tempfile1.name, encoding="utf-8", newline='') as csvfile:
            assert num_records == sum(1 for _ in csv.reader(csvfile, quotechar='"', escapechar='\\'))

        # import records from the first csv file
        logger.debug('Truncating {}...'.format(stress_table))
        self.session.execute("TRUNCATE {}".format(stress_table))
        run_copy_from(tempfile1)

        # export again to a second csv file
        tempfile2 = self.get_temp_file()
        run_copy_to(tempfile2)

        # check the length of both files is the same to ensure all exported records were imported
        assert sum(1 for _ in open(tempfile1.name)) == sum(1 for _ in open(tempfile2.name))

        return ret

    def test_bulk_round_trip_default(self):
        """
        Test bulk import with default stress import (one row per operation)

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=100000)

    def test_bulk_round_trip_non_prepared_statements(self):
        """
        Test bulk import with default stress import (one row per operation) and without
        prepared statements.

        @jira_ticket CASSANDRA-11053
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=100000,
                                   copy_from_options={'PREPAREDSTATEMENTS': False})

    def test_bulk_round_trip_blogposts(self):
        """
        Test bulk import with a user profile that inserts 10 rows per operation and has a replication factor 3

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=10000,
                                   configuration_options={'batch_size_warn_threshold_in_kb': '10'},
                                   profile=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'blogposts.yaml'),
                                   stress_table='stresscql.blogposts')

    def test_bulk_round_trip_blogposts_with_max_connections(self):
        """
        Same as test_bulk_round_trip_blogposts but limit the maximum number of concurrent connections a host will
        accept to simulate a failed connection to a replica that is up. Here we are interested in testing COPY TO,
        where we should have at most worker_processes * nodes connections + 1 connections, the +1 is the cqlsh
        connection. For COPY FROM the driver handles retries, we use only 2 worker processes to make sure it succeeds.

        @jira_ticket CASSANDRA-10938
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=10000,
                                   configuration_options={'native_transport_max_concurrent_connections': '12',
                                                          'batch_size_warn_threshold_in_kb': '10'},
                                   profile=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'blogposts.yaml'),
                                   stress_table='stresscql.blogposts',
                                   copy_to_options={'NUMPROCESSES': 5, 'MAXATTEMPTS': 20},
                                   copy_from_options={'NUMPROCESSES': 2})

    def test_bulk_round_trip_with_timeouts(self):
        """
        Test bulk import with very short read and write timeout values, this should exercise the
        retry and back-off policies. We cannot check the counts because "SELECT COUNT(*)" could timeout
        on Jenkins making the test flacky.

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(nodes=1, partitioner="murmur3", num_operations=100000,
                                   configuration_options={'range_request_timeout_in_ms': '200',
                                                          'write_request_timeout_in_ms': '100'},
                                   copy_from_options={'MAXINSERTERRORS': -1},
                                   skip_count_checks=True)

    def test_bulk_round_trip_with_low_ingestrate(self):
        """
        Test bulk import with default stress import (one row per operation) and a low
        ingestrate of only 1500 rows per second.

        @jira_ticket CASSANDRA-9303
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=10000,
                                   copy_from_options={'INGESTRATE': 1500})

    def test_bulk_round_trip_with_single_core(self):
        """
        Perform a round trip on a simulated single core machine. When determining the number of cores,
        copyutil.py will return the number carried by the environment variable CQLSH_COPY_TEST_NUM_CORES if it has
        been set.

        @jira_ticket CASSANDRA-11053
        """
        os.environ['CQLSH_COPY_TEST_NUM_CORES'] = '1'
        ret = self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=100000)
        if self.cluster.version() >= LooseVersion('3.6'):
            logger.debug('Checking that number of cores detected is correct')
            for out in ret:
                assert "Detected 1 core" in out[0]

    @since('3.0.5')
    def test_bulk_round_trip_with_backoff(self):
        """
        Test bulk import with default stress import (one row per operation) and COPY options
        that exercise the new back-off policy introduced by CASSANDRA-11320.

        @jira_ticket CASSANDRA-11320
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=250000,
                                   copy_from_options={'MAXINFLIGHTMESSAGES': 64, 'MAXPENDINGCHUNKS': 1})

    def prepare_copy_to_with_failures(self):
        """
        Create a cluster for testing COPY TO with failure injection, we need at least 3 token ranges
        so if VNODES are disabled we need to manually fix them and specify the correct start and end
        tokens for injecting failures. If VNODES are enabled instead, we will have several ranges
        so we pick an arbitrary range.

        @jira_ticket CASSANDRA-10858
        """
        if not self.dtest_config.use_vnodes:
            tokens = sorted(self.cluster.balanced_tokens(3))
            logger.debug('Using tokens {}'.format(tokens))
            self.prepare(nodes=3, tokens=tokens)
            start = tokens[1]
            end = tokens[2]
        else:
            self.prepare(nodes=1)
            metadata = self.session.cluster.metadata
            metadata.token_map.rebuild_keyspace(self.ks, build_if_absent=True)
            ring = [t.value for t in list(metadata.token_map.tokens_to_hosts_by_ks[self.ks].keys())]
            assert len(ring) >= 3, 'Not enough ranges in the ring for this test'
            ring.sort()
            idx = len(ring) // 2
            start = ring[idx]
            end = ring[idx + 1]

        logger.debug("Using failure range: {}, {}".format(start, end))
        return start, end

    def test_copy_to_with_more_failures_than_max_attempts(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copyutil.py to deviate its behavior from performing normal queries.
        Here we set a token range that will fail more times than the maximum number of attempts, therefore
        we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        start, end = self.prepare_copy_to_with_failures()

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        failures = {'failing_range': {'start': start, 'end': end, 'num_failures': 5}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)

        logger.debug('Exporting to csv file: {} with {} and 3 max attempts'
              .format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err, _ = self.run_cqlsh(cmds="COPY {} TO '{}' WITH MAXATTEMPTS='3'"
                                     .format(stress_table, tempfile.name))
        logger.debug(out)
        logger.debug(err)

        assert 'some records might be missing' in err
        assert len(open(tempfile.name).readlines()) < num_records

    def test_copy_to_with_fewer_failures_than_max_attempts(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copyutil.py to deviate its behavior from performing normal queries.
        Here we set a token range that will fail fewer times than the maximum number of attempts, therefore
        we expect this COPY TO job to succeed.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        start, end = self.prepare_copy_to_with_failures()

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        failures = {'failing_range': {'start': start, 'end': end, 'num_failures': 3}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        logger.debug('Exporting to csv file: {} with {} and 5 max attemps'
              .format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err, _ = self.run_cqlsh(cmds="COPY {} TO '{}' WITH MAXATTEMPTS='5'"
                                     .format(stress_table, tempfile.name))
        logger.debug(out)
        logger.debug(err)

        assert 'some records might be missing' not in err
        assert num_records == len(open(tempfile.name).readlines())

    def test_copy_to_with_child_process_crashing(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copyutil.py to deviate its behavior from performing normal queries.
        Here we set a token range that will cause a child process processing this range to exit, therefore
        we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        start, end = self.prepare_copy_to_with_failures()

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        failures = {'exit_range': {'start': start, 'end': end}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)

        logger.debug('Exporting to csv file: {} with {}'
              .format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err, _ = self.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))
        logger.debug(out)
        logger.debug(err)

        assert 'some records might be missing' in err
        assert len(open(tempfile.name).readlines()) < num_records

    def test_copy_from_with_more_failures_than_max_attempts(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copyutil.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a batch to fail more times than the maximum number of attempts,
        therefore we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))

        self.session.execute("TRUNCATE {}".format(stress_table))

        failures = {'failing_batch': {'id': 30, 'failures': 5}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        logger.debug('Importing from csv file {} with {}'.format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err, _ = self.run_cqlsh(cmds="COPY {} FROM '{}' WITH CHUNKSIZE='1' AND MAXATTEMPTS='3'"
                                     .format(stress_table, tempfile.name))
        logger.debug(out)
        logger.debug(err)

        assert 'Failed to process' in err
        num_records_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
        assert num_records_imported < num_records

    def test_copy_from_with_fewer_failures_than_max_attempts(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copyutil.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a batch to fail fewer times than the maximum number of attempts,
        therefore we expect this COPY TO job to succeed.

        We also set a low ingest rate to ensure we exercise the code path that might split a retry if it
        exceeds the intest rate.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))

        self.session.execute("TRUNCATE {}".format(stress_table))

        failures = {'failing_batch': {'id': 3, 'failures': 3}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        logger.debug('Importing from csv file {} with {}'.format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err, _ = self.run_cqlsh(cmds="COPY {} FROM '{}' WITH CHUNKSIZE=100 AND MAXATTEMPTS=5 AND INGESTRATE=101"
                                     .format(stress_table, tempfile.name))
        logger.debug(out)
        logger.debug(err)

        assert 'Failed to process' not in err
        num_records_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
        assert num_records == num_records_imported

    def test_copy_from_with_child_process_crashing(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copyutil.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a child process to exit, therefore we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))

        self.session.execute("TRUNCATE {}".format(stress_table))

        failures = {'exit_batch': {'id': 30}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        logger.debug('Importing from csv file {} with {}'.format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err, _ = self.run_cqlsh(cmds="COPY {} FROM '{}' WITH CHUNKSIZE='1'"
                                     .format(stress_table, tempfile.name))
        logger.debug(out)
        logger.debug(err)

        assert err is not None
        num_records_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
        assert num_records_imported < num_records

    @since('3.0')
    def test_copy_from_with_unacked_batches(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copyutil.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will not be sent to the server, which will cause the parent process to miss
        acknowledged batches from child processes, we expect this COPY TO job to fail after a pause of 'childtimeout'
        seconds, currently 30 seconds.

        @jira_ticket CASSANDRA-12740
        """
        num_records = 1000
        self.prepare(nodes=1)

        logger.debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), 'no-warmup', '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))

        self.session.execute("TRUNCATE {}".format(stress_table))

        failures = {'unsent_batch': {'id': 30}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        logger.debug('Importing from csv file {} with {}'.format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err, _ = self.run_cqlsh(cmds="COPY {} FROM '{}' WITH CHUNKSIZE=1 AND CHILDTIMEOUT=30 AND REQUESTTIMEOUT=15"
                                     .format(stress_table, tempfile.name))
        logger.debug(out)
        logger.debug(err)

        assert 'No records inserted in 30 seconds, aborting' in err
        num_records_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
        assert num_records_imported < num_records

    @since('2.2.5')
    def test_copy_from_with_large_cql_rows(self):
        """
        Test importing CQL rows that are larger than batch_size_warn_threshold_in_kb and
        batch_size_fail_threshold_in_kb. Test with and without prepared statements.

        @jira_ticket CASSANDRA-11474
        """
        num_records = 100
        self.prepare(nodes=1, configuration_options={'batch_size_warn_threshold_in_kb': '1',   # warn with 1kb and fail
                                                     'batch_size_fail_threshold_in_kb': '5'})  # with 5kb size batches

        logger.debug('Running stress')
        stress_table_name = 'standard1'
        self.ks = 'keyspace1'
        stress_ks_table_name = self.ks + '.' + stress_table_name
        self.node1.stress(['write', 'n={}'.format(num_records),
                           'no-warmup',
                           '-rate', 'threads=50',
                           '-col', 'n=FIXED(10)', 'SIZE=FIXED(1024)'])  # 10 columns of 1kb each

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_ks_table_name, tempfile.name))

        # Import using prepared statements (the default) and verify
        self.session.execute("TRUNCATE {}".format(stress_ks_table_name))

        logger.debug('Importing from csv file {}'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} FROM '{}' WITH MAXBATCHSIZE=1".format(stress_ks_table_name, tempfile.name))

        results = self.stringify_results(self.session.execute("SELECT * FROM {}".format(stress_ks_table_name)),
                                         format_fn=self.format_blob)
        self.assertCsvResultEqual(tempfile.name, results, stress_table_name)

        # Import without prepared statements and verify
        self.session.execute("TRUNCATE {}".format(stress_ks_table_name))

        logger.debug('Importing from csv file with MAXBATCHSIZE=1 {}'.format(tempfile.name))
        self.run_cqlsh(cmds="COPY {} FROM '{}' WITH MAXBATCHSIZE=1 AND PREPAREDSTATEMENTS=FALSE"
                       .format(stress_ks_table_name, tempfile.name))

        results = self.stringify_results(self.session.execute("SELECT * FROM {}".format(stress_ks_table_name)),
                                         format_fn=self.format_blob)
        self.assertCsvResultEqual(tempfile.name, results, stress_table_name)

    def test_copy_from_with_brackets_in_UDT(self):
        """
        Test that we can import a user defined type even when it contains brackets in its values.

        @jira_ticket CASSANDRA-11633
        """

        self.prepare()

        self.session.execute('CREATE TYPE udt_with_special_chars (val1 text, val2 text, val3 text)')
        self.session.execute('CREATE TABLE testspecialcharsinudt (a int PRIMARY KEY, b frozen<udt_with_special_chars>)')

        class MyType(namedtuple('MyType', ('val1', 'val2', 'val3'))):
            __slots__ = ()

            def __repr__(self):
                return "{{val1: '{}', val2: '{}', val3: '{}'}}"\
                    .format(self.val1 if self.val1 else '',
                            self.val2 if self.val2 else '',
                            self.val3 if self.val3 else '')

        self.session.cluster.register_user_type('ks', 'udt_with_special_chars', MyType)

        tempfile = self.get_temp_file()

        rows = [[1, MyType('N[ 56 58', '', '')],
                [2, MyType('N{ 56 58', '', '')],
                [3, MyType('N( 56 58', '', '')],
                [4, MyType('N[ 56 58]', '', '')],
                [5, MyType('N{ 56 58]', '', '')]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            for row in rows:
                writer.writerow(row)

        def _test(preparedStatements):
            logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
            cmds = "COPY ks.testspecialcharsinudt FROM '{}' WITH PREPAREDSTATEMENTS = {}"\
                .format(tempfile.name, preparedStatements)
            logger.debug(cmds)
            self.run_cqlsh(cmds=cmds)

            # we set nullval to the literal string '' to ensure the formatting output on trunk
            # matches the __repr__ of MyType() and we need the '' around values to ensure we write
            # quoted values in the csv
            out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testspecialcharsinudt")
            results = self.parse_cqlsh_query(out=out, num_cols=2, nullval="''")

            self.assertCsvResultEqual(tempfile.name, results, 'testspecialcharsinudt')

        _test(True)
        _test(False)

    @since('2.2')
    def test_round_trip_with_authentication(self):
        """
        Test that COPY works when authentication is enabled and when invoked via the SOURCE command
        or the --file cqlsh option.

        @jira_ticket CASSANDRA-12642
        """
        self.prepare(auth_enabled=True)
        self.session.execute('CREATE TABLE ks.testauth (a int PRIMARY KEY, b text)')

        num_records = 10
        for i in range(num_records):
            self.session.execute("INSERT INTO ks.testauth (a,b) VALUES ({}, 'abc')".format(i))

        tempfile = self.get_temp_file()

        # do an ordinary COPY TO AND FROM roundtrip
        logger.debug('Exporting to csv file: {}'.format(tempfile.name))
        ret = self.run_cqlsh(cmds="COPY ks.testauth TO '{}'".format(tempfile.name), auth_enabled=True)
        assert num_records == len(open(tempfile.name).readlines()), \
            "Failed to export {} rows\nSTDOUT:\n{}\nSTDERR:\n{}\n".format(num_records, ret.stderr, ret.stdout)

        self.session.execute("TRUNCATE testauth")
        logger.debug('Importing from csv file: {}'.format(tempfile.name))
        ret = self.run_cqlsh(cmds="COPY ks.testauth FROM '{}'".format(tempfile.name), auth_enabled=True)
        assert [[num_records]] == rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.testauth")), \
            "Failed to import {} rows\nSTDOUT:\n{}\nSTDERR:\n{}\n".format(num_records, ret.stderr, ret.stdout)

        # do another COPY TO AND FROM roundtrip but invoke copy via the source command
        copy_to_cql = self.get_temp_file()
        with open(copy_to_cql.name, 'w') as f:
            f.write("COPY ks.testauth TO '{}';".format(tempfile.name))

        copy_from_cql = self.get_temp_file()
        with open(copy_from_cql.name, 'w') as f:
            f.write("COPY ks.testauth FROM '{}';".format(tempfile.name))

        logger.debug('Exporting to csv file {} via source of {}'.format(tempfile.name, copy_to_cql.name))
        ret = self.run_cqlsh(cmds="SOURCE '{}'".format(copy_to_cql.name), auth_enabled=True)
        assert num_records == len(open(tempfile.name).readlines()), \
            "Failed to export {} rows\nSTDOUT:\n{}\nSTDERR:\n{}\n".format(num_records, ret.stderr, ret.stdout)

        self.session.execute("TRUNCATE testauth")
        logger.debug('Importing from csv file {} via source of {}'.format(tempfile.name, copy_from_cql.name))
        ret = self.run_cqlsh(cmds="SOURCE '{}'".format(copy_from_cql.name), auth_enabled=True)
        assert [[num_records]] == rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.testauth")), \
            "Failed to import {} rows\nSTDOUT:\n{}\nSTDERR:\n{}\n".format(num_records, ret.stderr, ret.stdout)

        # do another COPY TO AND FROM roundtrip but invoke copy via the -f cqlsh option
        logger.debug('Exporting to csv file {} via --file={}'.format(tempfile.name, copy_to_cql.name))
        ret = self.run_cqlsh(cmds='', cqlsh_options=['--file={}'.format(copy_to_cql.name)], auth_enabled=True)
        assert num_records == len(open(tempfile.name).readlines()), \
            "Failed to export {} rows\nSTDOUT:\n{}\nSTDERR:\n{}\n".format(num_records, ret.stderr, ret.stdout)

        self.session.execute("TRUNCATE testauth")
        logger.debug('Importing from csv file {} via --file={}'.format(tempfile.name, copy_from_cql.name))
        ret = self.run_cqlsh(cmds='', cqlsh_options=['--file={}'.format(copy_from_cql.name)], auth_enabled=True)
        assert [[num_records]] == rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.testauth")), \
            "Failed to import {} rows\nSTDOUT:\n{}\nSTDERR:\n{}\n".format(num_records, ret.stderr, ret.stdout)

    @since('2.2')
    def test_reading_pk_timestamps_with_counters(self):
        """
        When using counters we don't need to convert any value except for partition keys,
        and this test ensures that we can parse timestamps when they are in the partition key
        and the table contains a counter value.

        @jira_ticket CASSANDRA-12863
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE test_pk_timestamps_with_counters
            (columnname text, day timestamp,
            israndom boolean, columnvalue text, counter counter,
            PRIMARY KEY ((columnname, day, israndom), columnvalue)
            )""")

        records = ['origins|2016-10-01 00:00:00+0000|False|ACTUAL|6\n',
                   'origins|2016-10-01 00:00:00+0000|False|ADGMOB|4\n',
                   'origins|2016-10-01 00:00:00+0000|False|ANONPM|4\n',
                   'origins|2016-10-01 00:00:00+0000|False|CSRT2L|76\n',
                   'origins|2016-10-01 00:00:00+0000|False|DIAGOP|18\n',
                   'origins|2016-10-01 00:00:00+0000|False|E-SOFT|17\n',
                   'origins|2016-10-01 00:00:00+0000|False|E-TASK|10\n']

        tempfile = self.get_temp_file()
        with open(tempfile.name, 'w') as f:
            f.writelines(records)

        logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.test_pk_timestamps_with_counters FROM '{name}' WITH delimiter = '|'".format(name=tempfile.name)
        self.run_cqlsh(cmds=cmds)

        res = rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.test_pk_timestamps_with_counters"))[0][0]
        assert len(records) == res, "Failed to import one or more rows, expected {} but got {}".format(len(records), res)

    def test_copy_from_with_wrong_order_or_missing_UDT_fields(self):
        """
        Test that we can import a user defined type even when the sub-fields
        in the csv are specified in the wrong order or some fields are missing.

        @jira_ticket CASSANDRA-12959
        """
        self.prepare()

        self.session.execute('CREATE TYPE udt_with_multiple_fields (val1 text, val2 frozen<set<text>>)')
        self.session.execute('CREATE TABLE testwrongorderinudt (a int PRIMARY KEY, b frozen<udt_with_multiple_fields>)')

        class MyType(namedtuple('MyType', ('val1', 'val2'))):
            __slots__ = ()

            def __repr__(self):
                return "{{val1: '{}', val2: '{}'}}"\
                    .format(self.val1 if self.val1 else '',
                            self.val2 if self.val2 else '')

        self.session.cluster.register_user_type('ks', 'udt_with_multiple_fields', MyType)

        tempfile = self.get_temp_file()

        with open(tempfile.name, 'w') as f:
            f.write('1,"{val2: {\'val2_1\', \'val2_2\'}, val1: \'val1\'}"\n')
            f.write('2,"{val2: {\'val2_1\', \'val2_2\'}}"\n')
            f.write('3,"{val1: \'val1\'}"\n')

        def _test(prepared_statements):
            self.session.execute('TRUNCATE testwrongorderinudt')
            logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
            cmds = "COPY ks.testwrongorderinudt FROM '{}' WITH PREPAREDSTATEMENTS = {}"\
                .format(tempfile.name, prepared_statements)
            logger.debug(cmds)
            self.run_cqlsh(cmds=cmds)

            results = rows_to_list(self.session.execute("SELECT * FROM testwrongorderinudt where a = 1"))
            assert MyType('val1', SortedSet(['val2_1', 'val2_2'])) == results[0][1]

            results = rows_to_list(self.session.execute("SELECT * FROM testwrongorderinudt where a = 2"))
            assert MyType(None, SortedSet(['val2_1', 'val2_2'])) == results[0][1]

            results = rows_to_list(self.session.execute("SELECT * FROM testwrongorderinudt where a = 3"))
            assert MyType('val1', None) == results[0][1]

        _test(True)
        _test(False)

    @since('2.2')
    def test_reading_text_pk_counters(self):
        """
        When using counters we don't need to convert any value except for partition keys,
        and this test ensures that we can convert text related types into a binary partition key.

        @jira_ticket CASSANDRA-12909
        """
        self.prepare()
        self.session.execute("""
           CREATE TABLE test_reading_text_pk_counters (
                ascii_id ascii,
                text_id text,
                inet_id inet,
                varchar_id varchar,
                user_id timeuuid,
                counter_id ascii,
                count counter,
                PRIMARY KEY ((ascii_id, text_id, inet_id, varchar_id, user_id), counter_id)
            )""")

        tempfile = self.get_temp_file()
        with open(tempfile.name, 'w') as f:
            f.write('text1,text2,127.0.0.1,text3,f7ce3ac0-a66e-11e6-b58e-4e29450fd577,SA,2\n')
            f.write('text4,ヽ(´ー｀)ノ,127.0.0.2,text6,f7ce3ac0-a66e-11e6-b58e-4e29450fd577,SA,2\n')

        logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.test_reading_text_pk_counters FROM '{name}'".format(name=tempfile.name)
        self.run_cqlsh(cmds=cmds)

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.test_reading_text_pk_counters")
        res = self.parse_cqlsh_query(out=out, num_cols=7)
        self.assertCsvResultEqual(tempfile.name, res, 'test_reading_text_pk_counters')

    @since('2.2')
    def test_reading_text_pk_no_prepared_statements(self):
        """
        When using non-prepared statements we don't need to convert any value except for partition keys,
        and this test ensures that we can convert text related types into a binary partition key.

        @jira_ticket CASSANDRA-12909
        """
        self.prepare()
        self.session.execute("""
           CREATE TABLE test_reading_text_pk_no_prepared_statements (
                ascii_id ascii,
                text_id text,
                inet_id inet,
                varchar_id varchar,
                user_id timeuuid,
                val text,
                PRIMARY KEY ((ascii_id, text_id, inet_id, varchar_id, user_id))
            )""")

        tempfile = self.get_temp_file()
        with open(tempfile.name, 'w') as f:
            f.write('text1,text2,127.0.0.1,text3,f7ce3ac0-a66e-11e6-b58e-4e29450fd577,SA\n')
            f.write('text4,ヽ(´ー｀)ノ,127.0.0.2,text6,f7ce3ac0-a66e-11e6-b58e-4e29450fd577,SA\n')

        logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.test_reading_text_pk_no_prepared_statements FROM '{name}' WITH PREPAREDSTATEMENTS=FALSE"\
            .format(name=tempfile.name)
        self.run_cqlsh(cmds=cmds)

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.test_reading_text_pk_no_prepared_statements")
        res = self.parse_cqlsh_query(out=out, num_cols=6, timestamps_to_be_rounded=[])

        self.assertCsvResultEqual(tempfile.name, res, 'test_reading_text_pk_no_prepared_statements')

    @since('3.0')
    def test_reading_empty_strings_for_different_types(self):
        """
        Users can use the NULL=<marker> option to force importing empty strings in the primary key
        as long as the marker is set to something that indicates that the value should be safely skipped (typically
        something not present in the csv). A problem was reported when trying to import a csv file with
        many missing values, including strings and numbers, because the empty strings could not be converted
        to numbers.

        @jira_ticket CASSANDRA-12794
        """
        self.prepare()
        self.session.execute("""
           CREATE TABLE test_many_empty_strings (
                a text,
                b text,
                c text,
                d text,
                o uuid,
                 i1 bigint,
                 i2 bigint,
                 t text,
                 i3 bigint,
                 PRIMARY KEY ((a, b, c, d), o)
            )""")

        tempfile = self.get_temp_file()
        with open(tempfile.name, 'w') as f:
            f.write(',,,a1,645e7d3c-aef7-4e3c-b834-24b792cf2e55,,,,r1\n')

        def _test(prepared_statements):
            logger.debug('Importing from csv file: {name}'.format(name=tempfile.name))
            cmds = "COPY ks.test_many_empty_strings FROM '{}' WITH NULL='-' AND PREPAREDSTATEMENTS = {}"\
                .format(tempfile.name, prepared_statements)
            self.run_cqlsh(cmds=cmds)

            out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.test_many_empty_strings")
            res = self.parse_cqlsh_query(out=out, num_cols=9)

            self.assertCsvResultEqual(tempfile.name, res, 'test_many_empty_strings')

        _test(True)
        _test(False)

    @pytest.mark.depends_driver
    @since('3.0')
    def test_unusual_dates(self):
        """
        Test that we can export and import dates that are outside of the Python
        usual range (before 1900 or after 9999). Python can support dates from 1
        to 9999 but strftime has problems formatting dates before 1900.

        Perform the following:

        - create and populate a table,
        - COPY that table to a CSV file,
        - SELECT the contents of the table,
        - TRUNCATE the table,
        - COPY the written CSV file back into the table
        - assert that the previously-SELECTed contents of the table match the
        current contents of the table.

        @jira_ticket CASSANDRA-13185
        """
        self.prepare()
        self.session.execute("""
                CREATE TABLE testunusualdates (
                    a int PRIMARY KEY,
                    b timestamp
                )""")

        args = [['1', '9999-12-31 23:59:59+0000'],
                ['2', '10000-01-01 00:00:01+0000'],
                ['3', '10000-01-01 00:00:01+0800'],
                ['4', '10000-01-01 00:00:01-0800'],
                ['5', '1900-01-01 00:00:00+0000'],
                ['6', '1899-12-31 23:59:59+0000'],
                ['7', '1899-12-31 23:59:59+0800'],
                ['8', '1899-12-31 23:59:59-0800'],
                ]

        # the results that are expected to be returned by the SELECT * below, they are in UTC
        expected_results = [
            [1, datetime.datetime(9999, 12, 31, 23, 59, 59)],  # this is 253402300799000
            [2, 253402300801000],  # nr. 1 + 2 seconds
            [3, datetime.datetime(9999, 12, 31, 16, 0, 1)],
            [4, 253402329601000],  # nr. 1 + 8 hours and 2 seconds
            [5, datetime.datetime(1900, 1, 1, 0, 0)],
            [6, datetime.datetime(1899, 12, 31, 23, 59, 59)],
            [7, datetime.datetime(1899, 12, 31, 15, 59, 59)],
            [8, datetime.datetime(1900, 1, 1, 7, 59, 59)],
        ]

        for a, b in args:
            self.session.execute("INSERT INTO testunusualdates (a, b) VALUES ({}, '{}')".format(a, b))

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testunusualdates")
        results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[1])
        assert expected_results == rows_to_list(results)

        tempfile = self.get_temp_file()
        logger.debug('Exporting to csv file: {}'.format(tempfile.name))
        out, err, _ = self.run_cqlsh(cmds="COPY ks.testunusualdates TO '{}'".format(tempfile.name))
        logger.debug(out)

        # check all records were exported
        self.assertCsvResultEqual(tempfile.name, results, 'testunusualdates')

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testunusualdates")
        logger.debug('Importing from csv file: {}'.format(tempfile.name))
        out, err, _ = self.run_cqlsh(cmds="COPY ks.testunusualdates FROM '{}'".format(tempfile.name))
        logger.debug(out)

        out, err, _ = self.run_cqlsh(cmds="SELECT * FROM ks.testunusualdates")
        new_results = self.parse_cqlsh_query(out=out, num_cols=2, timestamps_to_be_rounded=[1])

        assert results == new_results

    @since('3.0')
    def test_importing_invalid_data_for_collections(self):
        """
        Test that no invalid data is imported for collections and that an appropriate error is reported.

        @jira_ticket CASSANDRA-13071
        """
        self.prepare()

        def _check(file_name, table_name, expected_results):
            # import the CSV file with COPY FROM
            logger.debug('Importing from csv file: {}'.format(file_name))
            out, err, _ = self.run_cqlsh(cmds="COPY ks.{} FROM '{}'".format(table_name, file_name))
            logger.debug(out)

            assert 'ParseError - Failed to parse' in err

            results = rows_to_list(self.session.execute("SELECT * FROM {}".format(table_name)))
            logger.debug(results)
            assert expected_results == results

        def _test_invalid_data_for_sets():
            logger.debug('Testing invalid data for sets')
            self.session.execute("""
                            CREATE TABLE testinvaliddataforsets (
                                key text,
                                value frozen<set<text>>,
                                PRIMARY KEY (key)
                            )""")

            tempfile = self.get_temp_file()
            with open(tempfile.name, 'w') as f:
                f.write('key1,"{\'test1\', \'test2\'}"\n')
                f.write('key2,"{\'test1\', \'test2\']"\n')
                f.write('key3,not_a_set\n')
                f.write('key4,"not_a_set"\n')
                f.write("key5,'not_a_set'\n")

            expected_results = [['key1', SortedSet(['test1', 'test2'])]]
            _check(tempfile.name, 'testinvaliddataforsets', expected_results)

        def _test_invalid_data_for_lists():
            logger.debug('Testing invalid data for lists')
            self.session.execute("""
                            CREATE TABLE testinvaliddataforlists (
                                key text,
                                value list<text>,
                                PRIMARY KEY (key)
                            )""")

            tempfile = self.get_temp_file()
            with open(tempfile.name, 'w') as f:
                f.write('key1,"[\'test1\', \'test2\']"\n')
                f.write('key2,"[\'test1\', \'test2\'}"\n')
                f.write('key3,not_a_list\n')
                f.write('key4,"not_a_list"\n')
                f.write("key5,'not_a_list'\n")

            expected_results = [['key1', list(['test1', 'test2'])]]
            _check(tempfile.name, 'testinvaliddataforlists', expected_results)

        def _test_invalid_data_for_maps():
            logger.debug('Testing invalid data for maps')
            self.session.execute("""
                            CREATE TABLE testinvaliddataformaps (
                                key text,
                                value map<text, text>,
                                PRIMARY KEY (key)
                            )""")

            tempfile = self.get_temp_file()
            with open(tempfile.name, 'w') as f:
                f.write('key1,"{\'key1\': \'test1\', \'key2\': \'test2\'}"\n')
                f.write('key2,"{\'key1\': \'test1\', \'key2\': \'test2\']"\n')
                f.write('key3,not_a_map\n')
                f.write('key4,"not_a_map"\n')
                f.write("key5,'not_a_map'\n")

            expected_results = [['key1', dict([('key1', 'test1'), ('key2', 'test2')])]]
            _check(tempfile.name, 'testinvaliddataformaps', expected_results)

        _test_invalid_data_for_sets()
        _test_invalid_data_for_lists()
        _test_invalid_data_for_maps()
