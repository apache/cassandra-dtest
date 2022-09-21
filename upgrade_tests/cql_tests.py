import itertools
import math
import random
import struct
import time
import pytest
import logging

from collections import OrderedDict
from distutils.version import LooseVersion
from uuid import UUID, uuid4

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import ProtocolException, SyntaxException
from cassandra.query import SimpleStatement
from cassandra.util import sortedset

from dtest import RUN_STATIC_UPGRADE_MATRIX, MAJOR_VERSION_4
from thrift_bindings.thrift010.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.thrift010.ttypes import (CfDef, Column, ColumnDef,
                                        ColumnOrSuperColumn, ColumnParent,
                                        Deletion, Mutation, SlicePredicate,
                                        SliceRange)
from thrift_test import get_thrift_client
from tools.assertions import (assert_all, assert_invalid, assert_length_equal,
                              assert_none, assert_one, assert_row_count)
from tools.data import rows_to_list
from tools.misc import add_skip
from .upgrade_base import UpgradeTester
from .upgrade_manifest import build_upgrade_pairs, CASSANDRA_4_0, CASSANDRA_4_1

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestCQL(UpgradeTester):

    def is_40_or_greater(self):
        return LooseVersion(self.UPGRADE_PATH.upgrade_meta.family) >= CASSANDRA_4_0

    def test_static_cf(self):
        """ Test static CF syntax """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            # Inserts
            cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
            cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

            # Queries
            assert_one(cursor, "SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000", ['Frodo', 'Baggins'])

            assert_one(cursor, "SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000", [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins'])

            assert_all(cursor, "SELECT * FROM users", [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee'],
                                                       [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']])

            # Test batch inserts
            cursor.execute("""
                BEGIN BATCH
                    INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                    UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                    DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                    DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                APPLY BATCH
            """)

            assert_all(cursor, "SELECT * FROM users", [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None], [UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None]])

    @since('2.0', max_version='2.99')  # 3.0+ not compatible with protocol version 2
    def test_large_collection_errors(self):
        """ For large collections, make sure that we are printing warnings """
        for version in self.get_node_versions():
            if version >= '3.0':
                pytest.skip('version {} not compatible with protocol version 2'.format(version))

        # We only warn with protocol 2
        cursor = self.prepare(protocol_version=2)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]
        self.fixture_dtest_setup.ignore_log_patterns = ["Detected collection for table"]

        cursor.execute("""
            CREATE TABLE maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE maps")

            # Insert more than the max, which is 65535
            for i in range(70000):
                cursor.execute("UPDATE maps SET properties[{}] = 'x' WHERE userid = 'user'".format(i))

            # Query for the data and throw exception
            cursor.execute("SELECT properties FROM maps WHERE userid = 'user'")
            node1.watch_log_for("Detected collection for table ks.maps with 70000 elements, more than the 65535 limit. "
                                "Only the first 65535 elements will be returned to the client. "
                                "Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.")

    def test_noncomposite_static_cf(self):
        """ Test non-composite static CF syntax """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname ascii,
                lastname ascii,
                age int
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            # Inserts
            cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
            cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

            # Queries
            assert_one(cursor, "SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000", ['Frodo', 'Baggins'])

            assert_one(cursor, "SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                       [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins'])
            # FIXME There appears to be some sort of problem with reusable cells
            # when executing this query.  It's likely that CASSANDRA-9705 will
            # fix this, but I'm not 100% sure.
            assert_one(cursor, "SELECT * FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479",
                       [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee'])
            assert_all(cursor, "SELECT * FROM users",
                       [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee'],
                        [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']])

            # Test batch inserts
            cursor.execute("""
                BEGIN BATCH
                    INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                    UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                    DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                    DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                APPLY BATCH
            """)

            assert_all(cursor, "SELECT * FROM users", [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None],
                                                       [UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None]])

    def test_dynamic_cf(self):
        """ Test non-composite dynamic CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid uuid,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE clicks")

            # Inserts
            cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)")
            cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)")
            cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://bar.bar', 128)")
            cursor.execute("UPDATE clicks SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 and url = 'http://bar.foo'")
            cursor.execute("UPDATE clicks SET time = 12 WHERE userid IN (f47ac10b-58cc-4372-a567-0e02b2c3d479, 550e8400-e29b-41d4-a716-446655440000) and url = 'http://foo-3'")

            # Queries
            assert_all(cursor, "SELECT url, time FROM clicks WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                       [['http://bar.bar', 128], ['http://foo-2.bar', 24], ['http://foo-3', 12], ['http://foo.bar', 42]])

            assert_all(cursor, "SELECT * FROM clicks WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479",
                       [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://bar.foo', 24],
                        [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://foo-3', 12]])

            assert_all(cursor, "SELECT time FROM clicks", [[24], [12], [128], [24], [12], [42]])

            if not self.is_40_or_greater():
                # Check we don't allow empty values for url since this is the full underlying cell name (#6152)
                assert_invalid(cursor, "INSERT INTO clicks (userid, url, time) VALUES (810e8500-e29b-41d4-a716-446655440000, '', 42)")

    def test_dense_cf(self):
        """ Test composite 'dense' CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE connections (
                userid uuid,
                ip text,
                port int,
                time bigint,
                PRIMARY KEY (userid, ip, port)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE connections")

            # Inserts
            cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.1', 80, 42)")
            cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 80, 24)")
            cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 90, 42)")
            cursor.execute("UPDATE connections SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.2' AND port = 80")

            # we don't have to include all of the clustering columns (see CASSANDRA-7990)
            cursor.execute("INSERT INTO connections (userid, ip, time) VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, '192.168.0.3', 42)")
            cursor.execute("UPDATE connections SET time = 42 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'")

            # Queries
            assert_all(cursor, "SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                       [['192.168.0.1', 80, 42], ['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]])

            assert_all(cursor, "SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip >= '192.168.0.2'",
                       [['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]])

            assert_all(cursor, "SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip = '192.168.0.2'",
                       [['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]])

            assert_none(cursor, "SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip > '192.168.0.2'")

            assert_one(cursor, "SELECT ip, port, time FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'",
                       ['192.168.0.3', None, 42])
            assert_one(cursor, "SELECT ip, port, time FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'",
                       ['192.168.0.4', None, 42])

            # Deletion
            cursor.execute("DELETE time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND ip = '192.168.0.2' AND port = 80")

            res = list(cursor.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000"))
            #Without compact storage deleting just the column leaves the row behind
            assert_length_equal(res, 2)

            cursor.execute("DELETE FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
            assert_none(cursor, "SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")

            cursor.execute("DELETE FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'")
            assert_none(cursor, "SELECT * FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'")

    def test_sparse_cf(self):
        """ Test composite 'sparse' CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE timeline (
                userid uuid,
                posted_month int,
                posted_day int,
                body ascii,
                posted_by ascii,
                PRIMARY KEY (userid, posted_month, posted_day)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE timeline")

            frodo_id = UUID('550e8400-e29b-41d4-a716-446655440000')
            sam_id = UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')

            # Inserts
            cursor.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (%s, 1, 12, 'Something else', 'Frodo Baggins')", (frodo_id,))
            cursor.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (%s, 1, 24, 'Something something', 'Frodo Baggins')", (frodo_id,))
            cursor.execute("UPDATE timeline SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = %s AND posted_month = 1 AND posted_day = 3", (sam_id,))
            cursor.execute("UPDATE timeline SET body = 'Yet one more message' WHERE userid = %s AND posted_month = 1 and posted_day = 30", (frodo_id,))

            # Queries
            assert_one(cursor, "SELECT body, posted_by FROM timeline WHERE userid = {} AND posted_month = 1 AND posted_day = 24".format(frodo_id), ['Something something', 'Frodo Baggins'])

            assert_all(cursor, "SELECT posted_day, body, posted_by FROM timeline WHERE userid = {} AND posted_month = 1 AND posted_day > 12".format(frodo_id), [
                [24, 'Something something', 'Frodo Baggins'],
                [30, 'Yet one more message', None]])

            assert_all(cursor, "SELECT posted_day, body, posted_by FROM timeline WHERE userid = {} AND posted_month = 1".format(frodo_id), [
                [12, 'Something else', 'Frodo Baggins'],
                [24, 'Something something', 'Frodo Baggins'],
                [30, 'Yet one more message', None]])

    def test_limit_ranges(self):
        """ Validate LIMIT option for 'range queries' in SELECT statements """
        cursor = self.prepare(ordered=True)

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE clicks")

            # Inserts
            for id in range(0, 100):
                for tld in ['com', 'org', 'net']:
                    cursor.execute("INSERT INTO clicks (userid, url, time) VALUES ({}, 'http://foo.{}', 42)".format(id, tld))

            # Queries
            assert_one(cursor, "SELECT * FROM clicks WHERE token(userid) >= token(2) LIMIT 1", [2, 'http://foo.com', 42])

            assert_one(cursor, "SELECT * FROM clicks WHERE token(userid) > token(2) LIMIT 1", [3, 'http://foo.com', 42])

    def test_limit_multiget(self):
        """ Validate LIMIT option for 'multiget' in SELECT statements """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE clicks")

            # Inserts
            for id in range(0, 100):
                for tld in ['com', 'org', 'net']:
                    cursor.execute("INSERT INTO clicks (userid, url, time) VALUES ({}, 'http://foo.{}', 42)".format(id, tld))

            # Check that we do limit the output to 1 *and* that we respect query
            # order of keys (even though 48 is after 2) prior to 2.1.17

            if self.get_node_version(is_upgraded) >= '2.1.17':
                # the coordinator is the upgraded 2.2+ node or a node with CASSSANDRA-12420
                assert_one(cursor, "SELECT * FROM clicks WHERE userid IN (48, 2) LIMIT 1", [2, 'http://foo.com', 42])

            else:
                # the coordinator is the non-upgraded 2.1 node
                assert_one(cursor, "SELECT * FROM clicks WHERE userid IN (48, 2) LIMIT 1", [48, 'http://foo.com', 42])

    def test_simple_tuple_query(self):
        """
        @jira_ticket CASSANDRA-8613
        """
        cursor = self.prepare()

        cursor.execute("create table bard (a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE bard")

            cursor.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 2, 0, 0, 0);""")
            cursor.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 1, 0, 0, 0);""")
            cursor.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 0, 0, 0);""")
            cursor.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")
            cursor.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 2, 2, 2);""")
            cursor.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 3, 3, 3);""")
            cursor.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")

            assert_all(cursor, "SELECT * FROM bard WHERE b=0 AND (c, d, e) > (1, 1, 1) ALLOW FILTERING;", [[0, 0, 2, 2, 2], [0, 0, 3, 3, 3]])

    def test_limit_sparse(self):
        """ Validate LIMIT option for sparse table in SELECT statements """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                day int,
                month text,
                year int,
                PRIMARY KEY (userid, url)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE clicks")

            # Inserts
            for id in range(0, 100):
                for tld in ['com', 'org', 'net']:
                    cursor.execute("INSERT INTO clicks (userid, url, day, month, year) VALUES ({}, 'http://foo.{}', 1, 'jan', 2012)".format(id, tld))

            # Queries
            # Check we do get as many rows as requested
            res = list(cursor.execute("SELECT * FROM clicks LIMIT 4"))
            assert_length_equal(res, 4)

    def test_counters(self):
        """ Validate counter support """
        cursor = self.prepare()

        logger.debug('*** VERSION FAMILY: ' + str(self.UPGRADE_PATH.upgrade_meta.family))
        logger.debug('*** VERSION: ' + str(self.cluster.version()))

        create_table_query = """
            CREATE TABLE clicks (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
            )
        """

        if self.cluster.version() >= LooseVersion('4.0'):
            cursor.execute(create_table_query)
        else:
            cursor.execute(create_table_query + '  WITH COMPACT STORAGE')

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE clicks")

            cursor.execute("UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'")

            assert_one(cursor, "SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'", [1])

            cursor.execute("UPDATE clicks SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'")
            assert_one(cursor, "SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'", [-3])

            cursor.execute("UPDATE clicks SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'")
            assert_one(cursor, "SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'", [-2])

            cursor.execute("UPDATE clicks SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'")
            assert_one(cursor, "SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'", [-4])

    def test_indexed_with_eq(self):
        """ Check that you can query for an indexed column even with a key EQ clause """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        cursor.execute("CREATE INDEX byAge ON users(age)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            # Inserts
            cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
            cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

            # Queries
            assert_none(cursor, "SELECT firstname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND age = 33")

            assert_one(cursor, "SELECT firstname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND age = 33", ['Samwise'])

    def test_select_key_in(self):
        """ Query for KEY IN (...) """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            # Inserts
            cursor.execute("""
                    INSERT INTO users (userid, firstname, lastname, age)
                    VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)
            """)
            cursor.execute("""
                    INSERT INTO users (userid, firstname, lastname, age)
                    VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, 'Samwise', 'Gamgee', 33)
            """)

            # Select
            res = list(cursor.execute("""
                    SELECT firstname, lastname FROM users
                    WHERE userid IN (550e8400-e29b-41d4-a716-446655440000, f47ac10b-58cc-4372-a567-0e02b2c3d479)
            """))

            assert_length_equal(res, 2)

    def test_exclusive_slice(self):
        """ Test SELECT respects inclusive and exclusive bounds """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            # Inserts
            for x in range(0, 10):
                cursor.execute("INSERT INTO test (k, c, v) VALUES (0, %s, %s)", (x, x))

            # Queries
            assert_all(cursor, "SELECT v FROM test WHERE k = 0", [[x] for x in range(10)])

            assert_all(cursor, "SELECT v FROM test WHERE k = 0 AND c >= 2 AND c <= 6", [[x] for x in range(2, 7)])

            assert_all(cursor, "SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6", [[x] for x in range(3, 7)])

            assert_all(cursor, "SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6", [[x] for x in range(2, 6)])

            assert_all(cursor, "SELECT v FROM test WHERE k = 0 AND c > 2 AND c < 6", [[x] for x in range(3, 6)])

            # With LIMIT
            assert_all(cursor, "SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2", [[3], [4]])

            assert_all(cursor, "SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2", [[5], [4]])

    def test_in_clause_wide_rows(self):
        """ Check IN support for 'wide rows' in SELECT statement """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # composites
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test1")
            cursor.execute("TRUNCATE test2")

            # Inserts
            for x in range(0, 10):
                cursor.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

            assert_all(cursor, "SELECT v FROM test1 WHERE k = 0 AND c IN (5, 2, 8)", [[2], [5], [8]])

            # Inserts
            for x in range(0, 10):
                cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, 0, {}, {})".format(x, x))

            # Check first we don't allow IN everywhere
            if self.get_node_version(is_upgraded) >= '2.2':
                # the coordinator is the upgraded 2.2+ node
                assert_none(cursor, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")
            else:
                # the coordinator is the non-upgraded 2.1 node
                assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")

                assert_all(cursor, "SELECT v FROM test2 WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)", [[2], [5], [8]])

    def test_order_by(self):
        """ Check ORDER BY support in SELECT statement """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # composites
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test1")
            cursor.execute("TRUNCATE test2")

            # Inserts
            for x in range(0, 10):
                cursor.execute("INSERT INTO test1 (k, c, v) VALUES (0, {}, {})".format(x, x))

            assert_all(cursor, "SELECT v FROM test1 WHERE k = 0 ORDER BY c DESC", [[x] for x in reversed(list(range(10)))])

            # Inserts
            for x in range(0, 4):
                for y in range(0, 2):
                    cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, {}, {}, {})".format(x, y, x * 2 + y))

            # Check first we don't always ORDER BY
            assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c DESC")
            assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c2 DESC")
            assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY k DESC")

            assert_all(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC", [[x] for x in reversed(list(range(8)))])

            assert_all(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c1", [[x] for x in range(8)])

    def test_more_order_by(self):
        """
        More ORDER BY checks
        @jira_ticket CASSANDRA-4160
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE COLUMNFAMILY Test (
                row text,
                number int,
                string text,
                PRIMARY KEY (row, number)
            ) WITH COMPACT STORAGE
        """)

        cursor.execute("""
            CREATE COLUMNFAMILY test2 (
                row text,
                number int,
                number2 int,
                string text,
                PRIMARY KEY (row, number, number2)
            ) WITH COMPACT STORAGE
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 1, 'one');")
            cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 2, 'two');")
            cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 3, 'three');")
            cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 4, 'four');")

            assert_all(cursor, "SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number ASC;", [[1], [2]])

            assert_all(cursor, "SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number ASC;", [[3], [4]])

            assert_all(cursor, "SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number DESC;", [[2], [1]])

            assert_all(cursor, "SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number DESC;", [[4], [3]])

            assert_all(cursor, "SELECT number FROM Test WHERE row='row' AND number > 3 ORDER BY number DESC;", [[4]])

            assert_all(cursor, "SELECT number FROM Test WHERE row='row' AND number <= 3 ORDER BY number DESC;", [[3], [2], [1]])

            # composite clustering
            cursor.execute("INSERT INTO test2 (row, number, number2, string) VALUES ('a', 1, 0, 'a');")
            cursor.execute("INSERT INTO test2 (row, number, number2, string) VALUES ('a', 2, 0, 'a');")
            cursor.execute("INSERT INTO test2 (row, number, number2, string) VALUES ('a', 2, 1, 'a');")
            cursor.execute("INSERT INTO test2 (row, number, number2, string) VALUES ('a', 3, 0, 'a');")
            cursor.execute("INSERT INTO test2 (row, number, number2, string) VALUES ('a', 3, 1, 'a');")
            cursor.execute("INSERT INTO test2 (row, number, number2, string) VALUES ('a', 4, 0, 'a');")

            assert_all(cursor, "SELECT number, number2 FROM test2 WHERE row='a' AND number < 3 ORDER BY number ASC;", [[1, 0], [2, 0], [2, 1]])

            assert_all(cursor, "SELECT number, number2 FROM test2 WHERE row='a' AND number >= 3 ORDER BY number ASC;", [[3, 0], [3, 1], [4, 0]])

            assert_all(cursor, "SELECT number, number2 FROM test2 WHERE row='a' AND number < 3 ORDER BY number DESC;", [[2, 1], [2, 0], [1, 0]])

            assert_all(cursor, "SELECT number, number2 FROM test2 WHERE row='a' AND number >= 3 ORDER BY number DESC;", [[4, 0], [3, 1], [3, 0]])

            assert_all(cursor, "SELECT number, number2 FROM test2 WHERE row='a' AND number > 3 ORDER BY number DESC;", [[4, 0]])

            assert_all(cursor, "SELECT number, number2 FROM test2 WHERE row='a' AND number <= 3 ORDER BY number DESC;", [[3, 1], [3, 0], [2, 1], [2, 0], [1, 0]])

    def test_order_by_validation(self):
        """
        Check we don't allow order by on row key
        @jira_ticket CASSANDRA-4246
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY (k1, k2)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            q = "INSERT INTO test (k1, k2, v) VALUES (%d, %d, %d)"
            cursor.execute(q % (0, 0, 0))
            cursor.execute(q % (1, 1, 1))
            cursor.execute(q % (2, 2, 2))

            assert_invalid(cursor, "SELECT * FROM test ORDER BY k2")

    def test_order_by_with_in(self):
        """
        Check that order-by works with IN
        @jira_ticket CASSANDRA-4327
        """
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                value varchar,
                PRIMARY KEY (my_id, col1)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.default_fetch_size = None

            cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key1', 1, 'a')")
            cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key2', 3, 'c')")
            cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key3', 2, 'b')")
            cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key4', 4, 'd')")

            query = "SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"
            assert_all(cursor, query, [[1], [2], [3]])

            query = "SELECT col1, my_id FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"
            assert_all(cursor, query, [[1, 'key1'], [2, 'key3'], [3, 'key2']])

            query = "SELECT my_id, col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"
            assert_all(cursor, query, [['key1', 1], ['key3', 2], ['key2', 3]])

    def test_reversed_comparator(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC);
        """)

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v text,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE test2")

            # Inserts
            for x in range(0, 10):
                cursor.execute("INSERT INTO test (k, c, v) VALUES (0, {}, {})".format(x, x))

            assert_all(cursor, "SELECT c, v FROM test WHERE k = 0 ORDER BY c ASC", [[x, x] for x in range(0, 10)])

            assert_all(cursor, "SELECT c, v FROM test WHERE k = 0 ORDER BY c DESC", [[x, x] for x in range(9, -1, -1)])

            # Inserts
            for x in range(0, 10):
                for y in range(0, 10):
                    cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, {}, {}, '{}{}')".format(x, y, x, y))

            assert_invalid(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 ASC")
            assert_invalid(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 DESC")

            assert_all(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC", [[x, y, '{}{}'.format(x, y)] for x in range(0, 10) for y in range(9, -1, -1)])

            assert_all(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 DESC", [[x, y, '{}{}'.format(x, y)] for x in range(0, 10) for y in range(9, -1, -1)])

            assert_all(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 ASC", [[x, y, '{}{}'.format(x, y)] for x in range(9, -1, -1) for y in range(0, 10)])

            assert_invalid(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c2 DESC, c1 ASC")

    def test_null_support(self):
        """ Test support for nulls """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 set<text>,
                PRIMARY KEY (k, c)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            # Inserts
            cursor.execute("INSERT INTO test (k, c, v1, v2) VALUES (0, 0, null, {'1', '2'})")
            cursor.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, 1)")

            assert_all(cursor, "SELECT * FROM test", [[0, 0, None, set(['1', '2'])], [0, 1, 1, None]])

            cursor.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, null)")
            cursor.execute("INSERT INTO test (k, c, v2) VALUES (0, 0, null)")

            assert_all(cursor, "SELECT * FROM test", [[0, 0, None, None], [0, 1, None, None]])

            assert_invalid(cursor, "INSERT INTO test (k, c, v2) VALUES (0, 2, {1, null})")
            assert_invalid(cursor, "SELECT * FROM test WHERE k = null")
            assert_invalid(cursor, "INSERT INTO test (k, c, v2) VALUES (0, 0, { 'foo', 'bar', null })")

    def test_nameless_index(self):
        """ Test CREATE INDEX without name and validate the index can be dropped """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE users (
                id text PRIMARY KEY,
                birth_year int,
            )
        """)

        cursor.execute("CREATE INDEX on users(birth_year)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Tom', 42)")
            cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Paul', 24)")
            cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Bob', 42)")

            assert_all(cursor, "SELECT id FROM users WHERE birth_year = 42", [['Tom'], ['Bob']])

    def test_deletion(self):
        """
        Test simple deletion and in particular check for #4193 bug
        @jira_ticket CASSANDRA-4193
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE testcf (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id)
            );
        """)

        # Compact case
        cursor.execute("""
            CREATE TABLE testcf2 (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id, name)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE testcf")
            cursor.execute("TRUNCATE testcf2")

            q = "INSERT INTO testcf (username, id, name, stuff) VALUES (%s, %s, %s, %s);"
            row1 = ('abc', 2, 'rst', 'some value')
            row2 = ('abc', 4, 'xyz', 'some other value')
            cursor.execute(q, row1)
            cursor.execute(q, row2)

            assert_all(cursor, "SELECT * FROM testcf", [list(row1), list(row2)])

            cursor.execute("DELETE FROM testcf WHERE username='abc' AND id=2")

            assert_all(cursor, "SELECT * FROM testcf", [list(row2)])

            q = "INSERT INTO testcf2 (username, id, name, stuff) VALUES (%s, %s, %s, %s);"
            row1 = ('abc', 2, 'rst', 'some value')
            row2 = ('abc', 4, 'xyz', 'some other value')
            cursor.execute(q, row1)
            cursor.execute(q, row2)

            assert_all(cursor, "SELECT * FROM testcf2", [list(row1), list(row2)])

            cursor.execute("DELETE FROM testcf2 WHERE username='abc' AND id=2")

            assert_all(cursor, "SELECT * FROM testcf", [list(row2)])

    def test_count(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE events (
                kind text,
                time int,
                value1 int,
                value2 int,
                PRIMARY KEY(kind, time)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE events")

            full = "INSERT INTO events (kind, time, value1, value2) VALUES ('ev1', %d, %d, %d)"
            no_v2 = "INSERT INTO events (kind, time, value1) VALUES ('ev1', %d, %d)"

            cursor.execute(full % (0, 0, 0))
            cursor.execute(full % (1, 1, 1))
            cursor.execute(no_v2 % (2, 2))
            cursor.execute(full % (3, 3, 3))
            cursor.execute(no_v2 % (4, 4))
            cursor.execute("INSERT INTO events (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)")

            assert_all(cursor, "SELECT COUNT(*) FROM events WHERE kind = 'ev1'", [[5]])

            assert_all(cursor, "SELECT COUNT(1) FROM events WHERE kind IN ('ev1', 'ev2') AND time=0", [[2]])

    def test_batch(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE users (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            query = SimpleStatement("""
                BEGIN BATCH
                    INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                    UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
                    INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
                    DELETE name FROM users WHERE userid = 'user1';
                APPLY BATCH;
            """, consistency_level=ConsistencyLevel.QUORUM)
            cursor.execute(query)

    def test_token_range(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int,
                v int
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            c = 100
            for i in range(0, c):
                cursor.execute("INSERT INTO test (k, c, v) VALUES ({}, {}, {})".format(i, i, i))

            rows = cursor.execute("SELECT k FROM test")
            inOrder = [x[0] for x in rows]
            assert_length_equal(inOrder, c)

            min_token = -2 ** 63
            res = list(cursor.execute("SELECT k FROM test WHERE token(k) >= {}".format(min_token)))

            assert_length_equal(res, c)

            # assert_invalid(cursor, "SELECT k FROM test WHERE token(k) >= 0")
            # cursor.execute("SELECT k FROM test WHERE token(k) >= 0")
            assert_all(cursor, "SELECT k FROM test WHERE token(k) >= token({}) AND token(k) < token({})".format(inOrder[32], inOrder[65]), [[inOrder[x]] for x in range(32, 65)])

    def test_timestamp_and_ttl(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c text,
                d text
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, c) VALUES (1, 'test')")
            cursor.execute("INSERT INTO test (k, c) VALUES (2, 'test') USING TTL 400")

            res = list(cursor.execute("SELECT k, c, writetime(c), ttl(c) FROM test"))

            assert_length_equal(res, 2)

            for r in res:
                assert isinstance(r[2], (int, int))
                if r[0] == 1:
                    assert r[3] is None, res
                else:
                    assert isinstance(r[3], (int, int))

            # wrap writetime(), ttl() in other functions (test for CASSANDRA-8451)
            res = list(cursor.execute("SELECT k, c, blobAsBigint(bigintAsBlob(writetime(c))), ttl(c) FROM test"))

            assert_length_equal(res, 2)

            for r in res:
                assert isinstance(r[2], (int, int))
                if r[0] == 1:
                    assert r[3] is None, res
                else:
                    assert isinstance(r[3], (int, int))

            res = list(cursor.execute("SELECT k, c, writetime(c), blobAsInt(intAsBlob(ttl(c))) FROM test"))

            assert_length_equal(res, 2)

            for r in res:
                assert isinstance(r[2], (int, int))
                if r[0] == 1:
                    assert r[3] is None, res
                else:
                    assert isinstance(r[3], (int, int))

            assert_invalid(cursor, "SELECT k, c, writetime(k) FROM test")

            res = cursor.execute("SELECT k, d, writetime(d) FROM test WHERE k = 1")
            assert_one(cursor, "SELECT k, d, writetime(d) FROM test WHERE k = 1", [1, None, None])

    def test_no_range_ghost(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        # Example from #3505
        cursor.execute("CREATE KEYSPACE ks1 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        cursor.execute("""
            CREATE COLUMNFAMILY ks1.users (
                KEY varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                birth_year bigint)
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE ks1.users")

            for k in range(0, 5):
                cursor.execute("INSERT INTO test (k, v) VALUES (%d, 0)" % k)

            assert_all(cursor, "SELECT k FROM test", [[k] for k in range(0, 5)], ignore_order=True)

            cursor.execute("DELETE FROM test WHERE k=2")

            assert_all(cursor, "SELECT k FROM test", [[k] for k in range(0, 5) if k != 2], ignore_order=True)

            # Example from #3505
            cursor.execute("USE ks1")

            cursor.execute("INSERT INTO users (KEY, password) VALUES ('user1', 'ch@ngem3a')")
            cursor.execute("UPDATE users SET gender = 'm', birth_year = 1980 WHERE KEY = 'user1'")

            assert_all(cursor, "SELECT * FROM users WHERE KEY='user1'", [['user1', 1980, 'm', 'ch@ngem3a']])

            cursor.execute("TRUNCATE users")

            assert_all(cursor, "SELECT * FROM users", [])

            assert_all(cursor, "SELECT * FROM users WHERE KEY='user1'", [])

    def test_undefined_column_handling(self):
        cursor = self.prepare(ordered=True)

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v1, v2) VALUES (0, 0, 0)")
            cursor.execute("INSERT INTO test (k, v1) VALUES (1, 1)")
            cursor.execute("INSERT INTO test (k, v1, v2) VALUES (2, 2, 2)")

            assert_all(cursor, "SELECT v2 FROM test", [[0], [None], [2]])

            assert_all(cursor, "SELECT v2 FROM test WHERE k = 1", [[None]])

    def test_range_tombstones(self):
        """ Test deletion by 'composite prefix' (range tombstones) """
        # Uses 3 nodes just to make sure RowMutation are correctly serialized
        cursor = self.prepare(nodes=3)

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c1, c2)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test1")

            rows = 5
            col1 = 2
            col2 = 2
            cpr = col1 * col2
            for i in range(0, rows):
                for j in range(0, col1):
                    for k in range(0, col2):
                        n = (i * cpr) + (j * col2) + k
                        cursor.execute("INSERT INTO test1 (k, c1, c2, v1, v2) VALUES ({}, {}, {}, {}, {})".format(i, j, k, n, n))

            for i in range(0, rows):

                assert_all(cursor, "SELECT v1, v2 FROM test1 where k = %d" % i, [[x, x] for x in range(i * cpr, (i + 1) * cpr)])

            for i in range(0, rows):
                cursor.execute("DELETE FROM test1 WHERE k = %d AND c1 = 0" % i)

            for i in range(0, rows):
                assert_all(cursor, "SELECT v1, v2 FROM test1 WHERE k = %d" % i, [[x, x] for x in range(i * cpr + col1, (i + 1) * cpr)])

            self.cluster.flush()
            time.sleep(0.2)

            for i in range(0, rows):
                assert_all(cursor, "SELECT v1, v2 FROM test1 WHERE k = %d" % i, [[x, x] for x in range(i * cpr + col1, (i + 1) * cpr)])

    def test_range_tombstones_compaction(self):
        """ Test deletion by 'composite prefix' (range tombstones) with compaction """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 text,
                PRIMARY KEY (k, c1, c2)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test1")

            for c1 in range(0, 4):
                for c2 in range(0, 2):
                    cursor.execute("INSERT INTO test1 (k, c1, c2, v1) VALUES (0, %d, %d, '%s')" % (c1, c2, '%i%i' % (c1, c2)))

            self.cluster.flush()

            cursor.execute("DELETE FROM test1 WHERE k = 0 AND c1 = 1")

            self.cluster.flush()
            self.cluster.compact()

            assert_all(cursor, "SELECT v1 FROM test1 WHERE k = 0", [['{}{}'.format(c1, c2)] for c1 in range(0, 4) for c2 in range(0, 2) if c1 != 1])

    def test_delete_row(self):
        """ Test deletion of rows """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                 k int,
                 c1 int,
                 c2 int,
                 v1 int,
                 v2 int,
                 PRIMARY KEY (k, c1, c2)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            q = "INSERT INTO test (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)"
            cursor.execute(q % (0, 0, 0, 0, 0))
            cursor.execute(q % (0, 0, 1, 1, 1))
            cursor.execute(q % (0, 0, 2, 2, 2))
            cursor.execute(q % (0, 1, 0, 3, 3))

            cursor.execute("DELETE FROM test WHERE k = 0 AND c1 = 0 AND c2 = 0")

            res = list(cursor.execute("SELECT * FROM test"))

            assert_length_equal(res, 3)

    def test_range_query_2ndary(self):
        """
        Test range queries with 2ndary indexes
        @jira_ticket CASSANDRA-4257
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE indextest (id int primary key, row int, setid int);")
        cursor.execute("CREATE INDEX indextest_setid_idx ON indextest (setid)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE indextest")

            q = "INSERT INTO indextest (id, row, setid) VALUES (%d, %d, %d);"
            cursor.execute(q % (0, 0, 0))
            cursor.execute(q % (1, 1, 0))
            cursor.execute(q % (2, 2, 0))
            cursor.execute(q % (3, 3, 0))

            assert_invalid(cursor, "SELECT * FROM indextest WHERE setid = 0 AND row < 1;")

            assert_all(cursor, "SELECT * FROM indextest WHERE setid = 0 AND row < 1 ALLOW FILTERING;", [[0, 0, 0]])

    def test_set(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags set<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE user")

            q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
            cursor.execute(q % "tags = tags + { 'foo' }")
            cursor.execute(q % "tags = tags + { 'bar' }")
            cursor.execute(q % "tags = tags + { 'foo' }")
            cursor.execute(q % "tags = tags + { 'foobar' }")
            cursor.execute(q % "tags = tags - { 'bar' }")

            assert_all(cursor, "SELECT tags FROM user", [[set(['foo', 'foobar'])]])

            q = "UPDATE user SET {} WHERE fn='Bilbo' AND ln='Baggins'"
            cursor.execute(q.format("tags = { 'a', 'c', 'b' }"))

            assert_all(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [[set(['a', 'b', 'c'])]])

            time.sleep(.01)

            cursor.execute(q.format("tags = { 'm', 'n' }"))

            assert_all(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [[set(['m', 'n'])]])

            cursor.execute("DELETE tags['m'] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
            assert_all(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [[set(['n'])]])

            cursor.execute("DELETE tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
            assert_all(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [])

    def test_map(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                m map<text, int>,
                PRIMARY KEY (fn, ln)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE user")

            q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
            cursor.execute(q % "m['foo'] = 3")
            cursor.execute(q % "m['bar'] = 4")
            cursor.execute(q % "m['woot'] = 5")
            cursor.execute(q % "m['bar'] = 6")
            cursor.execute("DELETE m['foo'] FROM user WHERE fn='Tom' AND ln='Bombadil'")

            assert_all(cursor, "SELECT m FROM user", [[{'woot': 5, 'bar': 6}]])

            q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
            cursor.execute(q % "m = { 'a' : 4 , 'c' : 3, 'b' : 2 }")

            assert_all(cursor, "SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'", [[{'a': 4, 'b': 2, 'c': 3}]])

            time.sleep(.01)

            # Check we correctly overwrite
            cursor.execute(q % "m = { 'm' : 4 , 'n' : 1, 'o' : 2 }")

            assert_all(cursor, "SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'", [[{'m': 4, 'n': 1, 'o': 2}]])

            cursor.execute(q % "m = {}")
            assert_all(cursor, "SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'", [])

    def test_list(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags list<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE user")

            q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
            cursor.execute(q % "tags = tags + [ 'foo' ]")
            cursor.execute(q % "tags = tags + [ 'bar' ]")
            cursor.execute(q % "tags = tags + [ 'foo' ]")
            cursor.execute(q % "tags = tags + [ 'foobar' ]")

            assert_one(cursor, "SELECT tags FROM user", [['foo', 'bar', 'foo', 'foobar']])

            q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
            cursor.execute(q % "tags = [ 'a', 'c', 'b', 'c' ]")
            assert_one(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [['a', 'c', 'b', 'c']])

            cursor.execute(q % "tags = [ 'm', 'n' ] + tags")
            assert_one(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [['m', 'n', 'a', 'c', 'b', 'c']])

            cursor.execute(q % "tags[2] = 'foo', tags[4] = 'bar'")
            assert_one(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [['m', 'n', 'foo', 'c', 'bar', 'c']])

            cursor.execute("DELETE tags[2] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
            assert_one(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [['m', 'n', 'c', 'bar', 'c']])

            cursor.execute(q % "tags = tags - [ 'bar' ]")
            assert_one(cursor, "SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'", [['m', 'n', 'c', 'c']])

    def test_multi_collection(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE foo(
                k uuid PRIMARY KEY,
                L list<int>,
                M map<text, int>,
                S set<int>
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE foo")

            cursor.execute("UPDATE ks.foo SET L = [1, 3, 5] WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
            cursor.execute("UPDATE ks.foo SET L = L + [7, 11, 13] WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
            cursor.execute("UPDATE ks.foo SET S = {1, 3, 5} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
            cursor.execute("UPDATE ks.foo SET S = S + {7, 11, 13} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
            cursor.execute("UPDATE ks.foo SET M = {'foo': 1, 'bar' : 3} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
            cursor.execute("UPDATE ks.foo SET M = M + {'foobar' : 4} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")

            assert_all(cursor, "SELECT L, M, S FROM foo WHERE k = b017f48f-ae67-11e1-9096-005056c00008", [[
                [1, 3, 5, 7, 11, 13],
                OrderedDict([('bar', 3), ('foo', 1), ('foobar', 4)]),
                sortedset([1, 3, 5, 7, 11, 13])
            ]])

    def test_range_query(self):
        """
        Range test query from #4372
        @jira_ticket CASSANDRA-4372
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2');")
            cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1');")
            cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1');")
            cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3');")
            cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5');")

            assert_all(cursor, "SELECT a, b, c, d, e, f FROM test WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2;", [[1, 1, 1, 1, 2, '2'], [1, 1, 1, 1, 3, '3'], [1, 1, 1, 1, 5, '5']])

    def test_composite_row_key(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                c int,
                v int,
                PRIMARY KEY ((k1, k2), c)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            req = "INSERT INTO test (k1, k2, c, v) VALUES ({}, {}, {}, {})"
            for i in range(0, 4):
                cursor.execute(req.format(0, i, i, i))

            assert_all(cursor, "SELECT * FROM test", [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]])

            assert_all(cursor, "SELECT * FROM test WHERE k1 = 0 and k2 IN (1, 3)", [[0, 1, 1, 1], [0, 3, 3, 3]])

            assert_invalid(cursor, "SELECT * FROM test WHERE k2 = 3")

            if self.get_node_version(is_upgraded) < '2.2':
                # the coordinator is the upgraded 2.2+ node
                assert_invalid(cursor, "SELECT * FROM test WHERE k1 IN (0, 1) and k2 = 3")

            assert_all(cursor, "SELECT * FROM test WHERE token(k1, k2) = token(0, 1)", [[0, 1, 1, 1]])

            assert_all(cursor, "SELECT * FROM test WHERE token(k1, k2) > " + str(-((2 ** 63) - 1)), [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]])

    @since('2', max_version='3.99')
    def test_cql3_insert_thrift(self):
        """
        Check that we can insert from thrift into a CQL3 table
        @jira_ticket CASSANDRA-4377
        """
        cursor = self.prepare(start_rpc=True)

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            node = self.cluster.nodelist()[0]
            host, port = node.network_interfaces['thrift']
            client = get_thrift_client(host, port)
            client.transport.open()
            client.set_keyspace('ks')
            key = struct.pack('>i', 2)
            column_name_component = struct.pack('>i', 4)
            # component length + component + EOC + component length + component + EOC
            column_name = b'\x00\x04' + column_name_component + b'\x00' + b'\x00\x01' + 'v'.encode() + b'\x00'
            value = struct.pack('>i', 8)
            client.batch_mutate(
                {key: {'test': [Mutation(ColumnOrSuperColumn(column=Column(name=column_name, value=value, timestamp=100)))]}},
                ThriftConsistencyLevel.ONE)

            assert_one(cursor, "SELECT * FROM test", [2, 4, 8])

    @since('2', max_version='3.99')
    def test_cql3_non_compound_range_tombstones(self):
        """
        Checks that 3.0 serializes RangeTombstoneLists correctly
        when communicating with 2.2 nodes.
        @jira_ticket CASSANDRA-11930
        """
        session = self.prepare(start_rpc=True)
        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']

        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')

        # create a CF with mixed static and dynamic cols
        column_defs = [ColumnDef('static1'.encode(), 'Int32Type', None, None, None)]
        cfdef = CfDef(
            keyspace='ks',
            name='cf',
            column_type='Standard',
            comparator_type='AsciiType',
            key_validation_class='AsciiType',
            default_validation_class='AsciiType',
            column_metadata=column_defs)
        client.system_add_column_family(cfdef)

        session.cluster.control_connection.wait_for_schema_agreement()

        for is_upgraded, session, node in self.do_upgrade(session, return_nodes=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))

            upgrade_to_version = self.get_node_version(is_upgraded=True)
            if LooseVersion('3.0.0') <= upgrade_to_version <= LooseVersion('3.0.6'):
                pytest.skip(msg='CASSANDRA-11930 was fixed in 3.0.7 and 3.7')
            elif LooseVersion('3.1') <= upgrade_to_version <= LooseVersion('3.6'):
                pytest.skip(msg='CASSANDRA-11930 was fixed in 3.0.7 and 3.7')

            session.execute("TRUNCATE ks.cf")

            host, port = node.network_interfaces['thrift']
            client = get_thrift_client(host, port)
            client.transport.open()
            client.set_keyspace('ks')

            # insert a number of keys so that we'll get rows on both the old and upgraded nodes
            for key in ['key{}'.format(i).encode() for i in range(10)]:
                logger.debug("Using key " + key.decode())

                # insert "static" column
                client.batch_mutate(
                    {key: {'cf': [Mutation(ColumnOrSuperColumn(column=Column(name='static1'.encode(), value=struct.pack('>i', 1), timestamp=100)))]}},
                    ThriftConsistencyLevel.ALL)

                # insert "dynamic" columns
                for i, column_name in enumerate(('a', 'b', 'c', 'd', 'e')):
                    column_value = 'val{}'.format(i)
                    client.batch_mutate(
                        {key: {'cf': [Mutation(ColumnOrSuperColumn(column=Column(name=column_name.encode(), value=column_value.encode(), timestamp=100)))]}},
                        ThriftConsistencyLevel.ALL)

                # sanity check on the query
                fetch_slice = SlicePredicate(slice_range=SliceRange(''.encode(), ''.encode(), False, 100))
                row = client.get_slice(key, ColumnParent(column_family='cf'), fetch_slice, ThriftConsistencyLevel.ALL)
                assert 6 == len(row), row
                cols = OrderedDict([(cosc.column.name.decode(), cosc.column.value) for cosc in row])
                logger.debug(cols)
                assert ['a', 'b', 'c', 'd', 'e', 'static1'] == list(cols.keys())
                assert 'val0'.encode() == cols['a']
                assert 'val4'.encode() == cols['e']
                assert struct.pack('>i', 1) == cols['static1']

                # delete a slice of dynamic columns
                slice_range = SliceRange('b'.encode(), 'd'.encode(), False, 100)
                client.batch_mutate(
                    {key: {'cf': [Mutation(deletion=Deletion(timestamp=101, predicate=SlicePredicate(slice_range=slice_range)))]}},
                    ThriftConsistencyLevel.ALL)

                # check remaining columns
                row = client.get_slice(key, ColumnParent(column_family='cf'), fetch_slice, ThriftConsistencyLevel.ALL)
                assert 3 == len(row), row
                cols = OrderedDict([(cosc.column.name.decode(), cosc.column.value) for cosc in row])
                logger.debug(cols)
                assert ['a', 'e', 'static1'] == list(cols.keys())
                assert 'val0'.encode() == cols['a']
                assert 'val4'.encode() == cols['e']
                assert struct.pack('>i', 1) == cols['static1']

    def test_row_existence(self):
        """
        Check the semantic of CQL row existence (part of #4361)
        @jira_ticket CASSANDRA-4361
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, c, v1, v2) VALUES (1, 1, 1, 1)")

            assert_one(cursor, "SELECT * FROM test", [1, 1, 1, 1])

            assert_invalid(cursor, "DELETE c FROM test WHERE k = 1 AND c = 1")

            cursor.execute("DELETE v2 FROM test WHERE k = 1 AND c = 1")
            assert_one(cursor, "SELECT * FROM test", [1, 1, 1, None])

            cursor.execute("DELETE v1 FROM test WHERE k = 1 AND c = 1")
            assert_one(cursor, "SELECT * FROM test", [1, 1, None, None])

            cursor.execute("DELETE FROM test WHERE k = 1 AND c = 1")
            assert_none(cursor, "SELECT * FROM test", )

            cursor.execute("INSERT INTO test (k, c) VALUES (2, 2)")
            assert_one(cursor, "SELECT * FROM test", [2, 2, None, None])

    def test_only_pk(self):
        """
        Check table with only a PK (part of #4361)
        @jira_ticket CASSANDRA-4361
        """
        cursor = self.prepare(ordered=True)

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                PRIMARY KEY (k, c)
            )
        """)

        # Check for dense tables too
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE test2")

            q = "INSERT INTO test (k, c) VALUES (%s, %s)"
            for k in range(0, 2):
                for c in range(0, 2):
                    cursor.execute(q, (k, c))

            query = "SELECT * FROM test"
            assert_all(cursor, query, [[x, y] for x in range(0, 2) for y in range(0, 2)])

            q = "INSERT INTO test2 (k, c) VALUES (%s, %s)"
            for k in range(0, 2):
                for c in range(0, 2):
                    cursor.execute(q, (k, c))

            query = "SELECT * FROM test2"
            expected = [[x, y] for x in range(0, 2) for y in range(0, 2)]
            assert_all(cursor, query, expected)

    def test_no_clustering(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))

            for i in range(10):
                cursor.execute("INSERT INTO test (k, v) VALUES (%s, %s)", (i, i))

            cursor.default_fetch_size = None

            assert_all(cursor, "SELECT * FROM test", [[i, i] for i in range(10)], ignore_order=True)

    def test_date(self):
        """ Check dates are correctly recognized and validated """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timestamp
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, t) VALUES (0, '2011-02-03')")
            assert_invalid(cursor, "INSERT INTO test (k, t) VALUES (0, '2011-42-42')")

    def test_range_slice(self):
        """
        Test a regression from #1337
        @jira_ticket CASSANDRA-1337
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
            cursor.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

            assert_row_count(cursor, 'test', 2)

    def test_composite_index_with_pk(self):

        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content text,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """)

        cursor.execute("CREATE INDEX ON blogs(author)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE blogs")

            req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', '%s')"
            cursor.execute(req % (1, 0, 0, 'foo', 'bar1'))
            cursor.execute(req % (1, 0, 1, 'foo', 'bar2'))
            cursor.execute(req % (2, 1, 0, 'foo', 'baz'))
            cursor.execute(req % (3, 0, 1, 'gux', 'qux'))

            query = "SELECT blog_id, content FROM blogs WHERE author='foo'"
            assert_all(cursor, query, [[1, 'bar1'], [1, 'bar2'], [2, 'baz']])

            query = "SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo' ALLOW FILTERING"
            assert_one(cursor, query, [2, 'baz'])

            query = "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo' ALLOW FILTERING"
            assert_one(cursor, query, [2, 'baz'])

            query = "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING"
            assert_one(cursor, query, [2, 'baz'])

            query = "SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING"
            assert_none(cursor, query)

            query = "SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING"
            assert_none(cursor, query)

            assert_invalid(cursor, "SELECT content FROM blogs WHERE time2 >= 0 AND author='foo'")

            # as discussed in CASSANDRA-8148, some queries that should have required ALLOW FILTERING
            # in 2.0 have been fixed for 2.2
            if self.get_node_version(is_upgraded) < '2.2':
                # the coordinator is the non-upgraded 2.1 node
                cursor.execute("SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo'")
                cursor.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo'")
                cursor.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo'")
                cursor.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo'")
                cursor.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo'")
            else:
                # the coordinator is the upgraded 2.2+ node
                assert_invalid(cursor, "SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo'")
                assert_invalid(cursor, "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo'")
                assert_invalid(cursor, "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo'")
                assert_invalid(cursor, "SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo'")
                assert_invalid(cursor, "SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo'")

    def test_limit_bugs(self):
        """
        Test for LIMIT bugs from #4579
        @jira_ticket CASSANDRA-4579
        """
        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE testcf (
                a int,
                b int,
                c int,
                d int,
                e int,
                PRIMARY KEY (a, b)
            );
        """)

        cursor.execute("""
            CREATE TABLE testcf2 (
                a int primary key,
                b int,
                c int,
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE testcf")
            cursor.execute("TRUNCATE testcf2")

            cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (1, 1, 1, 1, 1);")
            cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (2, 2, 2, 2, 2);")
            cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (3, 3, 3, 3, 3);")
            cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (4, 4, 4, 4, 4);")

            assert_all(cursor, "SELECT * FROM testcf", [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2], [3, 3, 3, 3, 3], [4, 4, 4, 4, 4]])

            assert_all(cursor, "SELECT * FROM testcf LIMIT 1;", [[1, 1, 1, 1, 1]])

            assert_all(cursor, "SELECT * FROM testcf LIMIT 2;", [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2]])

            cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (1, 1, 1);")
            cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (2, 2, 2);")
            cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (3, 3, 3);")
            cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (4, 4, 4);")

            assert_all(cursor, "SELECT * FROM testcf2;", [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]])

            assert_all(cursor, "SELECT * FROM testcf2 LIMIT 1;", [[1, 1, 1]])

            assert_all(cursor, "SELECT * FROM testcf2 LIMIT 2;", [[1, 1, 1], [2, 2, 2]])

            assert_all(cursor, "SELECT * FROM testcf2 LIMIT 3;", [[1, 1, 1], [2, 2, 2], [3, 3, 3]])

            assert_all(cursor, "SELECT * FROM testcf2 LIMIT 4;", [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]])

            assert_all(cursor, "SELECT * FROM testcf2 LIMIT 5;", [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]])

    def test_npe_composite_table_slice(self):
        """
        Test for NPE when trying to select a slice from a composite table
        @jira_ticket CASSANDRA-4532
        """
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE compositetest(
                status ascii,
                ctime bigint,
                key ascii,
                nil ascii,
                PRIMARY KEY (status, ctime, key)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE compositetest")

            cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key1','')")
            cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key2','')")
            cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key3','')")
            cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key4','')")
            cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key5','')")
            cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345680,'key6','')")

            assert_invalid(cursor, "SELECT * FROM compositetest WHERE ctime>=12345679 AND key='key3' AND ctime<=12345680 LIMIT 3;")
            assert_invalid(cursor, "SELECT * FROM compositetest WHERE ctime=12345679  AND key='key3' AND ctime<=12345680 LIMIT 3;")

    def test_order_by_multikey(self):
        """
        Test for #4612 bug and more generally order by when multiple C* rows are queried
        @jira_ticket CASSANDRA-4612
        """
        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                col2 int,
                value varchar,
                PRIMARY KEY (my_id, col1, col2)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.default_fetch_size = None

            cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key1', 1, 1, 'a');")
            cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key2', 3, 3, 'a');")
            cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key3', 2, 2, 'b');")
            cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key4', 2, 1, 'b');")

            query = "SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1;"
            assert_all(cursor, query, [[1], [2], [3]])

            query = "SELECT col1, value, my_id, col2 FROM test WHERE my_id in('key3', 'key4') ORDER BY col1, col2;"
            assert_all(cursor, query, [[2, 'b', 'key4', 1], [2, 'b', 'key3', 2]])

            assert_invalid(cursor, "SELECT col1 FROM test ORDER BY col1;")
            assert_invalid(cursor, "SELECT col1 FROM test WHERE my_id > 'key1' ORDER BY col1;")

    def test_remove_range_slice(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            for i in range(0, 3):
                cursor.execute("INSERT INTO test (k, v) VALUES (%d, %d)" % (i, i))

            cursor.execute("DELETE FROM test WHERE k = 1")
            assert_all(cursor, "SELECT * FROM test", [[0, 0], [2, 2]])

    def test_indexes_composite(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                blog_id int,
                timestamp int,
                author text,
                content text,
                PRIMARY KEY (blog_id, timestamp)
            )
        """)

        cursor.execute("CREATE INDEX ON test(author)")
        time.sleep(1)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            req = "INSERT INTO test (blog_id, timestamp, author, content) VALUES (%d, %d, '%s', '%s')"
            cursor.execute(req % (0, 0, "bob", "1st post"))
            cursor.execute(req % (0, 1, "tom", "2nd post"))
            cursor.execute(req % (0, 2, "bob", "3rd post"))
            cursor.execute(req % (0, 3, "tom", "4nd post"))
            cursor.execute(req % (1, 0, "bob", "5th post"))

            query = "SELECT blog_id, timestamp FROM test WHERE author = 'bob'"
            assert_all(cursor, query, [[1, 0], [0, 0], [0, 2]])

            cursor.execute(req % (1, 1, "tom", "6th post"))
            cursor.execute(req % (1, 2, "tom", "7th post"))
            cursor.execute(req % (1, 3, "bob", "8th post"))

            query = "SELECT blog_id, timestamp FROM test WHERE author = 'bob'"
            assert_all(cursor, query, [[1, 0], [1, 3], [0, 0], [0, 2]])

            cursor.execute("DELETE FROM test WHERE blog_id = 0 AND timestamp = 2")

            query = "SELECT blog_id, timestamp FROM test WHERE author = 'bob'"
            assert_all(cursor, query, [[1, 0], [1, 3], [0, 0]])

    def test_refuse_in_with_indexes(self):
        """
        Test for the validation bug of #4709
        @jira_ticket CASSANDRA-4709
        """
        cursor = self.prepare()
        cursor.execute("create table t1 (pk varchar primary key, col1 varchar, col2 varchar);")
        cursor.execute("create index t1_c1 on t1(col1);")
        cursor.execute("create index t1_c2 on t1(col2);")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE t1")

            cursor.execute("insert into t1  (pk, col1, col2) values ('pk1','foo1','bar1');")
            cursor.execute("insert into t1  (pk, col1, col2) values ('pk1a','foo1','bar1');")
            cursor.execute("insert into t1  (pk, col1, col2) values ('pk1b','foo1','bar1');")
            cursor.execute("insert into t1  (pk, col1, col2) values ('pk1c','foo1','bar1');")
            cursor.execute("insert into t1  (pk, col1, col2) values ('pk2','foo2','bar2');")
            cursor.execute("insert into t1  (pk, col1, col2) values ('pk3','foo3','bar3');")
            assert_invalid(cursor, "select * from t1 where col2 in ('bar1', 'bar2');")

    def test_reversed_compact(self):
        """
        Test for #4716 bug and more generally for good behavior of ordering
        @jira_ticket CASSANDRA-4716
        """
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test1 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
              AND CLUSTERING ORDER BY (c DESC);
        """)

        cursor.execute("""
            CREATE TABLE test2 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test1")
            cursor.execute("TRUNCATE test2")

            for i in range(0, 10):
                cursor.execute("INSERT INTO test1(k, c, v) VALUES ('foo', %s, %s)", (i, i))

            query = "SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo'"
            assert_all(cursor, query, [[5], [4], [3]])

            query = "SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo'"
            assert_all(cursor, query, [[6], [5], [4], [3], [2]])

            query = "SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"
            assert_all(cursor, query, [[3], [4], [5]])

            query = "SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"
            assert_all(cursor, query, [[2], [3], [4], [5], [6]])

            query = "SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"
            assert_all(cursor, query, [[5], [4], [3]])

            query = "SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"
            assert_all(cursor, query, [[6], [5], [4], [3], [2]])

            for i in range(0, 10):
                cursor.execute("INSERT INTO test2(k, c, v) VALUES ('foo', %s, %s)", (i, i))

            query = "SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo'"
            assert_all(cursor, query, [[3], [4], [5]])

            query = "SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo'"
            assert_all(cursor, query, [[2], [3], [4], [5], [6]])

            query = "SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"
            assert_all(cursor, query, [[3], [4], [5]])

            query = "SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"
            assert_all(cursor, query, [[2], [3], [4], [5], [6]])

            query = "SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"
            assert_all(cursor, query, [[5], [4], [3]])

            query = "SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"
            assert_all(cursor, query, [[6], [5], [4], [3], [2]])

    def test_reversed_compact_multikey(self):
        """
        Test for the bug from #4760 and #4759
        @jira_ticket CASSANDRA-4760
        @jira_ticket CASSANDRA-4759
        """
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test (
                key text,
                c1 int,
                c2 int,
                value text,
                PRIMARY KEY(key, c1, c2)
                ) WITH COMPACT STORAGE
                  AND CLUSTERING ORDER BY(c1 DESC, c2 DESC);
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            for i in range(0, 3):
                for j in range(0, 3):
                    cursor.execute("INSERT INTO test(key, c1, c2, value) VALUES ('foo', %i, %i, 'bar');" % (i, j))

            # Equalities

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1"
            assert_all(cursor, query, [[1, 2], [1, 1], [1, 0]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC"
            assert_all(cursor, query, [[1, 0], [1, 1], [1, 2]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC"
            assert_all(cursor, query, [[1, 2], [1, 1], [1, 0]])

            # GT

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1"
            assert_all(cursor, query, [[2, 2], [2, 1], [2, 0]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC"
            assert_all(cursor, query, [[2, 0], [2, 1], [2, 2]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC"
            assert_all(cursor, query, [[2, 2], [2, 1], [2, 0]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1"
            assert_all(cursor, query, [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC"
            assert_all(cursor, query, [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC"
            assert_all(cursor, query, [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC"
            assert_all(cursor, query, [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]])

            # LT

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1"
            assert_all(cursor, query, [[0, 2], [0, 1], [0, 0]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC"
            assert_all(cursor, query, [[0, 0], [0, 1], [0, 2]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC"
            assert_all(cursor, query, [[0, 2], [0, 1], [0, 0]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1"
            assert_all(cursor, query, [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC"
            assert_all(cursor, query, [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC"
            assert_all(cursor, query, [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]])

            query = "SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC"
            assert_all(cursor, query, [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]])

    def test_collection_and_regular(self):

        cursor = self.prepare()

        cursor.execute("""
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>,
            c int
          )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, l, c) VALUES(3, [0, 1, 2], 4)")
            cursor.execute("UPDATE test SET l[0] = 1, c = 42 WHERE k = 3")
            assert_one(cursor, "SELECT l, c FROM test WHERE k = 3", [[1, 1, 2], 42])

    def test_batch_and_list(self):
        cursor = self.prepare()

        cursor.execute("""
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>
          )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("""
              BEGIN BATCH
                UPDATE test SET l = l + [ 1 ] WHERE k = 0;
                UPDATE test SET l = l + [ 2 ] WHERE k = 0;
                UPDATE test SET l = l + [ 3 ] WHERE k = 0;
              APPLY BATCH
            """)

            assert_one(cursor, "SELECT l FROM test WHERE k = 0", [[1, 2, 3]])

            cursor.execute("""
              BEGIN BATCH
                UPDATE test SET l = [ 1 ] + l WHERE k = 1;
                UPDATE test SET l = [ 2 ] + l WHERE k = 1;
                UPDATE test SET l = [ 3 ] + l WHERE k = 1;
              APPLY BATCH
            """)

            assert_one(cursor, "SELECT l FROM test WHERE k = 1", [[3, 2, 1]])

    def test_boolean(self):
        cursor = self.prepare()

        cursor.execute("""
          CREATE TABLE test (
            k boolean PRIMARY KEY,
            b boolean
          )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, b) VALUES (true, false)")
            assert_one(cursor, "SELECT * FROM test WHERE k = true", [True, False])

    def test_multiordering(self):
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test (
                k text,
                c1 int,
                c2 int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            for i in range(0, 2):
                for j in range(0, 2):
                    cursor.execute("INSERT INTO test(k, c1, c2) VALUES ('foo', %i, %i)" % (i, j))

            query = "SELECT c1, c2 FROM test WHERE k = 'foo'"
            assert_all(cursor, query, [[0, 1], [0, 0], [1, 1], [1, 0]])

            query = "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 DESC"
            assert_all(cursor, query, [[0, 1], [0, 0], [1, 1], [1, 0]])

            query = "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 DESC, c2 ASC"
            assert_all(cursor, query, [[1, 0], [1, 1], [0, 0], [0, 1]])

            assert_invalid(cursor, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 DESC")
            assert_invalid(cursor, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 ASC")
            assert_invalid(cursor, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 ASC")

    def test_returned_null(self):
        """
        Test for returned null.
        StorageProxy short read protection hadn't been updated after the changes made by CASSANDRA-3647,
        namely the fact that SliceQueryFilter groups columns by prefix before counting them.
        @jira_ticket CASSANDRA-4882
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 0, 0);")
            cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 1, 1);")
            cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 2, 2);")
            cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 3, 3);")

            query = "SELECT * FROM test WHERE k = 0 LIMIT 1;"
            assert_one(cursor, query, [0, 0, 2, 2])

    def test_multi_list_set(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l1 list<int>,
                l2 list<int>
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, l1, l2) VALUES (0, [1, 2, 3], [4, 5, 6])")
            cursor.execute("UPDATE test SET l2[1] = 42, l1[1] = 24  WHERE k = 0")

            assert_one(cursor, "SELECT l1, l2 FROM test WHERE k = 0", [[1, 24, 3], [4, 42, 6]])

    def test_composite_index_collections(self):
        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content set<text>,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """)

        cursor.execute("CREATE INDEX ON blogs(author)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE blogs")

            req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', %s)"
            cursor.execute(req % (1, 0, 0, 'foo', "{ 'bar1', 'bar2' }"))
            cursor.execute(req % (1, 0, 1, 'foo', "{ 'bar2', 'bar3' }"))
            cursor.execute(req % (2, 1, 0, 'foo', "{ 'baz' }"))
            cursor.execute(req % (3, 0, 1, 'gux', "{ 'qux' }"))

            query = "SELECT blog_id, content FROM blogs WHERE author='foo'"
            assert_all(cursor, query, [[1, set(['bar1', 'bar2'])], [1, set(['bar2', 'bar3'])], [2, set(['baz'])]])

    @pytest.mark.skip("https://issues.apache.org/jira/browse/CASSANDRA-14961")
    def test_truncate_clean_cache(self):
        cursor = self.prepare(ordered=True, use_cache=True)

        if self.node_version_above('2.1'):
            cursor.execute("""
                CREATE TABLE test (
                    k int PRIMARY KEY,
                    v1 int,
                    v2 int,
                ) WITH caching = {'keys': 'NONE', 'rows_per_partition': 'ALL'};
            """)
        else:
            cursor.execute("""
                CREATE TABLE test (
                    k int PRIMARY KEY,
                    v1 int,
                    v2 int,
                ) WITH CACHING = ALL;
            """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            for i in range(0, 3):
                cursor.execute("INSERT INTO test(k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i * 2))

            query = "SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)"
            assert_all(cursor, query, [[0, 0], [1, 2], [2, 4]])

            cursor.execute("TRUNCATE test")

            query = "SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)"
            assert_none(cursor, query)

    def test_range_with_deletes(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int,
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            nb_keys = 30
            nb_deletes = 5

            for i in range(0, nb_keys):
                cursor.execute("INSERT INTO test(k, v) VALUES ({}, {})".format(i, i))

            for i in random.sample(range(nb_keys), nb_deletes):
                cursor.execute("DELETE FROM test WHERE k = {}".format(i))

            res = list(cursor.execute("SELECT * FROM test LIMIT {}".format(int(nb_keys / 2))))
            assert_length_equal(res, nb_keys / 2)

    @since('2.0', max_version='4.1') # CASSANDRA-8877
    def test_collection_function(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l set<int>
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            assert_invalid(cursor, "SELECT ttl(l) FROM test WHERE k = 0")
            assert_invalid(cursor, "SELECT writetime(l) FROM test WHERE k = 0")

    def test_composite_partition_key_validation(self):
        """
        Test for bug from #5122
        @jira_ticket CASSANDRA-5122
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE foo (a int, b text, c uuid, PRIMARY KEY ((a, b)));")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE foo")

            cursor.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'aze', 4d481800-4c5f-11e1-82e0-3f484de45426)")
            cursor.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'ert', 693f5800-8acb-11e3-82e0-3f484de45426)")
            cursor.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'opl', d4815800-2d8d-11e0-82e0-3f484de45426)")

            assert_row_count(cursor, 'foo', 3)

            assert_invalid(cursor, "SELECT * FROM foo WHERE a=1")

    @since('2.2')
    def test_multi_in(self):
        self.__multi_in(False)

    @since('2.2')
    def test_multi_in_compact(self):
        self.__multi_in(True)

    def __multi_in(self, compact):
        cursor = self.prepare()

        data = [
            ('test', '06029', 'CT', 9, 'Ellington'),
            ('test', '06031', 'CT', 9, 'Falls Village'),
            ('test', '06902', 'CT', 9, 'Stamford'),
            ('test', '06927', 'CT', 9, 'Stamford'),
            ('test', '10015', 'NY', 36, 'New York'),
            ('test', '07182', 'NJ', 34, 'Newark'),
            ('test', '73301', 'TX', 48, 'Austin'),
            ('test', '94102', 'CA', 6, 'San Francisco'),

            ('test2', '06029', 'CT', 9, 'Ellington'),
            ('test2', '06031', 'CT', 9, 'Falls Village'),
            ('test2', '06902', 'CT', 9, 'Stamford'),
            ('test2', '06927', 'CT', 9, 'Stamford'),
            ('test2', '10015', 'NY', 36, 'New York'),
            ('test2', '07182', 'NJ', 34, 'Newark'),
            ('test2', '73301', 'TX', 48, 'Austin'),
            ('test2', '94102', 'CA', 6, 'San Francisco'),
        ]

        create = """
            CREATE TABLE zipcodes (
                group text,
                zipcode text,
                state text,
                fips_regions int,
                city text,
                PRIMARY KEY(group,zipcode,state,fips_regions)
            )"""

        if compact:
            create = create + " WITH COMPACT STORAGE"

        cursor.execute(create)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE zipcodes")

            for d in data:
                cursor.execute("INSERT INTO zipcodes (group, zipcode, state, fips_regions, city) VALUES ('%s', '%s', '%s', %s, '%s')" % d)

            res = list(cursor.execute("select zipcode from zipcodes"))
            assert_length_equal(res, 16)

            res = list(cursor.execute("select zipcode from zipcodes where group='test'"))
            assert_length_equal(res, 8)

            assert_invalid(cursor, "select zipcode from zipcodes where zipcode='06902'")

            res = list(cursor.execute("select zipcode from zipcodes where zipcode='06902' ALLOW FILTERING"))
            assert_length_equal(res, 2)

            res = list(cursor.execute("select zipcode from zipcodes where group='test' and zipcode='06902'"))
            assert_length_equal(res, 1)

            if is_upgraded:
                # the coordinator is the upgraded 2.2+ node

                res = list(cursor.execute("select zipcode from zipcodes where group='test' and zipcode IN ('06902','73301','94102')"))
                assert_length_equal(res, 3)

                res = list(cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA')"))
                assert_length_equal(res, 2)

                res = list(cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions = 9"))
                assert_length_equal(res, 1)

                res = list(cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') ORDER BY zipcode DESC"))
                assert_length_equal(res, 2)

                res = list(cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions > 0"))
                assert_length_equal(res, 2)

                assert_none(cursor, "select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions < 0")

    @since('2.2')
    def test_multi_in_compact_non_composite(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                key int,
                c int,
                v int,
                PRIMARY KEY (key, c)
            ) WITH COMPACT STORAGE
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (key, c, v) VALUES (0, 0, 0)")
            cursor.execute("INSERT INTO test (key, c, v) VALUES (0, 1, 1)")
            cursor.execute("INSERT INTO test (key, c, v) VALUES (0, 2, 2)")

            query = "SELECT * FROM test WHERE key=0 AND c IN (0, 2)"
            assert_all(cursor, query, [[0, 0, 0], [0, 2, 2]])

    def test_large_clustering_in(self):
        """
        @jira_ticket CASSANDRA-8410
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            insert_statement = cursor.prepare("INSERT INTO test (k, c, v) VALUES (?, ?, ?)")
            cursor.execute(insert_statement, (0, 0, 0))

            select_statement = cursor.prepare("SELECT * FROM test WHERE k=? AND c IN ?")
            in_values = list(range(10000))

            # try to fetch one existing row and 9999 non-existing rows
            rows = list(cursor.execute(select_statement, [0, in_values]))

            assert_length_equal(rows, 1)
            assert (0, 0, 0) == rows[0]

            # insert approximately 1000 random rows between 0 and 10k
            clustering_values = set([random.randint(0, 9999) for _ in range(1000)])
            clustering_values.add(0)
            args = [(0, i, i) for i in clustering_values]
            execute_concurrent_with_args(cursor, insert_statement, args)

            rows = list(cursor.execute(select_statement, [0, in_values]))
            assert_length_equal(rows, len(clustering_values))

    def test_timeuuid(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            assert_invalid(cursor, "INSERT INTO test (k, t) VALUES (0, 2012-11-07 18:18:22-0800)", expected=SyntaxException)

            for i in range(4):
                cursor.execute("INSERT INTO test (k, t) VALUES (0, now())")
                time.sleep(1)

            assert_row_count(cursor, 'test', 4)

            res = list(cursor.execute("SELECT * FROM test"))
            dates = [d[1] for d in res]

            assert_row_count(cursor, 'test', 4, where="k = 0 AND t >= {}".format(dates[0]))

            assert_row_count(cursor, 'test', 0, where="k = 0 AND t < {}".format(dates[0]))

            assert_row_count(cursor, 'test', 2, where="k = 0 AND t > {} AND t <= {}".format(dates[0], dates[2]))

            assert_row_count(cursor, 'test', 1, where="k = 0 AND t = {}".format(dates[0]))

            assert_invalid(cursor, "SELECT dateOf(k) FROM test WHERE k = 0 AND t = %s" % dates[0])

            cursor.execute("SELECT dateOf(t), unixTimestampOf(t) FROM test WHERE k = 0 AND t = %s" % dates[0])
            cursor.execute("SELECT t FROM test WHERE k = 0 AND t > maxTimeuuid(1234567) AND t < minTimeuuid('2012-11-07 18:18:22-0800')")
            # not sure what to check exactly so just checking the query returns

    def test_float_with_exponent(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                d double,
                f float
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, d, f) VALUES (0, 3E+10, 3.4E3)")
            cursor.execute("INSERT INTO test(k, d, f) VALUES (1, 3.E10, -23.44E-3)")
            cursor.execute("INSERT INTO test(k, d, f) VALUES (2, 3, -2)")

    def test_compact_metadata(self):
        """
        Test regression from #5189
        @jira_ticket CASSANDRA-5189
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE bar (
                id int primary key,
                i int
            ) WITH COMPACT STORAGE;
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE bar")

            cursor.execute("INSERT INTO bar (id, i) VALUES (1, 2);")
            assert_one(cursor, "SELECT * FROM bar", [1, 2])

    def test_query_compact_tables_during_upgrade(self):
        """
        Check that un-upgraded sstables for compact storage tables
        can be read after an upgrade. Checks for a regression where
        when the coordinator is on < 3.0, a replica at >= 3.0 returns
        0 results for any read request. When the >= 3.0 node is
        the coordinator, the problem does not manifest. Likewise, if
        the data is inserted after the replica is upgraded, or if
        upgradesstables is run after upgrade, the query succeeds, so
        the issue is with reading legacy format sstables in response to
        a legacy format read request
        @jira_ticket CASSANDRA-11087
        """
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE t1 (
                a int PRIMARY KEY,
                b int
            ) WITH COMPACT STORAGE;
        """)

        execute_concurrent_with_args(cursor,
                                     cursor.prepare("INSERT INTO t1 (a, b) VALUES (?, ?)"),
                                     [(i, i) for i in range(100)])
        self.install_nodetool_legacy_parsing()
        self.cluster.flush()

        def check_read_all(cursor):
            read_count = 0
            # first read each row separately - obviously, we should be able to retrieve all 100
            for i in range(100):
                res = cursor.execute("SELECT * FROM t1 WHERE a = {a}".format(a=i))
                read_count += len(rows_to_list(res))
            logger.debug("Querying for individual keys retrieved {c} results".format(c=read_count))
            assert read_count == 100
            # now a range slice, again all 100 rows should be retrievable
            res = rows_to_list(cursor.execute("SELECT * FROM t1"))
            read_count = len(res)
            logger.debug("Range request retrieved {c} rows".format(c=read_count))
            assert_length_equal(res, 100)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {state} node".format(state="upgraded" if is_upgraded else "old"))
            check_read_all(cursor)

        logger.debug("Querying upgraded node after running upgradesstables")
        node1 = self.cluster.nodelist()[0]
        node1.nodetool("upgradesstables -a")
        check_read_all(self.patient_exclusive_cql_connection(node1, keyspace="ks"))

    def test_clustering_indexing(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE posts (
                id1 int,
                id2 int,
                author text,
                time bigint,
                v1 text,
                v2 text,
                PRIMARY KEY ((id1, id2), author, time)
            )
        """)

        cursor.execute("CREATE INDEX ON posts(time)")
        cursor.execute("CREATE INDEX ON posts(id2)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE posts")

            cursor.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')")
            cursor.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')")
            cursor.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')")
            cursor.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')")
            cursor.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')")

            query = "SELECT v1 FROM posts WHERE time = 1"
            assert_all(cursor, query, [['B'], ['E']])

            query = "SELECT v1 FROM posts WHERE id2 = 1"
            assert_all(cursor, query, [['C'], ['E']])

            query = "SELECT v1 FROM posts WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0"
            assert_one(cursor, query, ['A'])

            # Test for CASSANDRA-8206
            cursor.execute("UPDATE posts SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1")

            query = "SELECT v1 FROM posts WHERE id2 = 0"
            assert_all(cursor, query, [['A'], ['B'], ['D']])

            query = "SELECT v1 FROM posts WHERE time = 1"
            assert_all(cursor, query, [['B'], ['E']])

    def test_edge_2i_on_complex_pk(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE indexed (
                pk0 int,
                pk1 int,
                ck0 int,
                ck1 int,
                ck2 int,
                value int,
                PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2)
            )
        """)

        cursor.execute("CREATE INDEX ON indexed(pk0)")
        cursor.execute("CREATE INDEX ON indexed(ck0)")
        cursor.execute("CREATE INDEX ON indexed(ck1)")
        cursor.execute("CREATE INDEX ON indexed(ck2)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE indexed")

            cursor.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (0, 1, 2, 3, 4, 5)")
            cursor.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (1, 2, 3, 4, 5, 0)")
            cursor.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (2, 3, 4, 5, 0, 1)")
            cursor.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (3, 4, 5, 0, 1, 2)")
            cursor.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (4, 5, 0, 1, 2, 3)")
            cursor.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (5, 0, 1, 2, 3, 4)")

            assert_all(cursor, "SELECT value FROM indexed WHERE pk0 = 2", [[1]])

            assert_all(cursor, "SELECT value FROM indexed WHERE ck0 = 0", [[3]])

            assert_all(cursor, "SELECT value FROM indexed WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0", [[2]])

            assert_all(cursor, "SELECT value FROM indexed WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING", [[4]])

    def test_end_of_component_as_end_key(self):
        """
        Test to make sure that an end-of-component is no longer being used as the end key of the range when
        a secondary index is involved.
        @jira_ticket CASSANDRA-5240
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test(
                interval text,
                seq int,
                id int,
                severity int,
                PRIMARY KEY ((interval, seq), id)
            ) WITH CLUSTERING ORDER BY (id DESC);
        """)

        cursor.execute("CREATE INDEX ON test(severity);")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 1, 1);")
            cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 2, 1);")
            cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 3, 2);")
            cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 4, 3);")
            cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 1, 3);")
            cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 2, 3);")
            cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 3, 1);")
            cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 4, 2);")

            query = "select * from test where severity = 3 and interval = 't' and seq =1;"
            assert_one(cursor, query, ['t', 1, 4, 3])

    def test_ticket_5230(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE foo (
                key text,
                c text,
                v text,
                PRIMARY KEY (key, c)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE foo")

            cursor.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '1', '1')")
            cursor.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '2', '2')")
            cursor.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '3', '3')")

            query = "SELECT c FROM foo WHERE key = 'foo' AND c IN ('1', '2');"
            assert_all(cursor, query, [['1'], ['2']])

    def test_conversion_functions(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                i varint,
                b blob
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, i, b) VALUES (0, blobAsVarint(bigintAsBlob(3)), textAsBlob('foobar'))")
            query = "SELECT i, blobAsText(b) FROM test WHERE k = 0"
            assert_one(cursor, query, [3, 'foobar'])

    # Fixed by CASSANDRA-12654 in 3.12
    @since('2.0', max_version='3.11.99')
    def test_IN_clause_on_last_key(self):
        """
        Tests patch to improve validation by not throwing an assertion when using map, list, or set
        with IN clauses on the last key.
        @jira_ticket CASSANDRA-5376
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                key text,
                c bigint,
                v text,
                x set<text>,
                PRIMARY KEY (key, c)
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            assert_invalid(cursor, "select * from test where key = 'foo' and c in (1,3,4);")

    def test_function_and_reverse_type(self):
        """
        @jira_ticket CASSANDRA-5386
        """
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test (
                k int,
                c timeuuid,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC)
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("INSERT INTO test (k, c, v) VALUES (0, now(), 0);")

    def test_NPE_during_select_with_token(self):
        """
        Test for NPE during CQL3 select with token()
        @jira_ticket CASSANDRA-5404
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (key text PRIMARY KEY)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            # We just want to make sure this doesn't NPE server side
            assert_invalid(cursor, "select * from test where token(key) > token(int(3030343330393233)) limit 1;")

    def test_empty_blob(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, b blob)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, b) VALUES (0, 0x)")
            assert_one(cursor, "SELECT * FROM test", [0, ''.encode()])

    @since('2', max_version='3.99')
    def test_rename(self):
        cursor = self.prepare(start_rpc=True)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        cfdef = CfDef()
        cfdef.keyspace = 'ks'
        cfdef.name = 'test'
        cfdef.column_type = 'Standard'
        cfdef.comparator_type = 'CompositeType(Int32Type, Int32Type, Int32Type)'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'UTF8Type'

        client.set_keyspace('ks')
        client.system_add_column_family(cfdef)

        time.sleep(1)

        cursor.execute("INSERT INTO ks.test (key, column1, column2, column3, value) VALUES ('foo', 4, 3, 2, 'bar')")

        time.sleep(1)

        cursor.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            assert_one(cursor, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])

    def test_clustering_order_and_functions(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            ) WITH CLUSTERING ORDER BY (t DESC)
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            for i in range(0, 5):
                cursor.execute("INSERT INTO test (k, t) VALUES (%d, now())" % i)

            cursor.execute("SELECT dateOf(t) FROM test")

    def test_conditional_update(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 text,
                v3 int
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            # Shouldn't apply
            assert_one(cursor, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [False])
            assert_one(cursor, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS", [False])

            # Should apply
            assert_one(cursor, "INSERT INTO test (k, v1, v2) VALUES (0, 2, 'foo') IF NOT EXISTS", [True])

            # Shouldn't apply
            assert_one(cursor, "INSERT INTO test (k, v1, v2) VALUES (0, 5, 'bar') IF NOT EXISTS", [False, 0, 2, 'foo', None])
            assert_one(cursor, "SELECT * FROM test", [0, 2, 'foo', None], cl=ConsistencyLevel.SERIAL)

            # Should not apply
            assert_one(cursor, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [False, 2])
            assert_one(cursor, "SELECT * FROM test", [0, 2, 'foo', None], cl=ConsistencyLevel.SERIAL)

            # Should apply (note: we want v2 before v1 in the statement order to exercise #5786)
            assert_one(cursor, "UPDATE test SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 = 2", [True])
            assert_one(cursor, "UPDATE test SET v2 = 'bar', v1 = 3 WHERE k = 0 IF EXISTS", [True])
            assert_one(cursor, "SELECT * FROM test", [0, 3, 'bar', None], cl=ConsistencyLevel.SERIAL)

            # Shouldn't apply, only one condition is ok
            assert_one(cursor, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'foo'", [False, 3, 'bar'])
            assert_one(cursor, "SELECT * FROM test", [0, 3, 'bar', None], cl=ConsistencyLevel.SERIAL)

            # Should apply
            assert_one(cursor, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'bar'", [True])
            assert_one(cursor, "SELECT * FROM test", [0, 5, 'foobar', None], cl=ConsistencyLevel.SERIAL)

            # Shouldn't apply
            assert_one(cursor, "DELETE v2 FROM test WHERE k = 0 IF v1 = 3", [False, 5])
            assert_one(cursor, "SELECT * FROM test", [0, 5, 'foobar', None], cl=ConsistencyLevel.SERIAL)

            # Shouldn't apply
            assert_one(cursor, "DELETE v2 FROM test WHERE k = 0 IF v1 = null", [False, 5])
            assert_one(cursor, "SELECT * FROM test", [0, 5, 'foobar', None], cl=ConsistencyLevel.SERIAL)

            # Should apply
            assert_one(cursor, "DELETE v2 FROM test WHERE k = 0 IF v1 = 5", [True])
            assert_one(cursor, "SELECT * FROM test", [0, 5, None, None], cl=ConsistencyLevel.SERIAL)

            # Shouln't apply
            assert_one(cursor, "DELETE v1 FROM test WHERE k = 0 IF v3 = 4", [False, None])

            # Should apply
            assert_one(cursor, "DELETE v1 FROM test WHERE k = 0 IF v3 = null", [True])
            assert_one(cursor, "SELECT * FROM test", [0, None, None, None], cl=ConsistencyLevel.SERIAL)

            # Should apply
            assert_one(cursor, "DELETE FROM test WHERE k = 0 IF v1 = null", [True])
            assert_none(cursor, "SELECT * FROM test", cl=ConsistencyLevel.SERIAL)

            # Shouldn't apply
            assert_one(cursor, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS", [False])

            if self.get_node_version(is_upgraded) > "2.1.1":
                # Should apply
                assert_one(cursor, "DELETE FROM test WHERE k = 0 IF v1 IN (null)", [True])

    @since('2.1.1')
    def test_non_eq_conditional_update(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 text,
                v3 int
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            # non-EQ conditions
            cursor.execute("INSERT INTO test (k, v1, v2) VALUES (0, 2, 'foo')")
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 < 3", [True])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 <= 3", [True])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 > 1", [True])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 >= 1", [True])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 != 1", [True])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 != 2", [False, 2])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 IN (0, 1, 2)", [True])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 IN (142, 276)", [False, 2])
            assert_one(cursor, "UPDATE test SET v2 = 'bar' WHERE k = 0 IF v1 IN ()", [False, 2])

    def test_conditional_delete(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
            )
        """)

        # static columns
        cursor.execute("""
            CREATE TABLE test2 (
                k text,
                s text static,
                i int,
                v text,
                PRIMARY KEY (k, i)
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE test2")

            assert_one(cursor, "DELETE FROM test WHERE k=1 IF EXISTS", [False])

            assert_one(cursor, "INSERT INTO test (k, v1) VALUES (1, 2) IF NOT EXISTS", [True])
            assert_one(cursor, "DELETE FROM test WHERE k=1 IF EXISTS", [True])
            assert_none(cursor, "SELECT * FROM test WHERE k=1", cl=ConsistencyLevel.SERIAL)
            assert_one(cursor, "DELETE FROM test WHERE k=1 IF EXISTS", [False])

            assert_one(cursor, "INSERT INTO test (k, v1) VALUES (2, 2) IF NOT EXISTS USING TTL 1", [True])
            time.sleep(1.5)
            assert_one(cursor, "DELETE FROM test WHERE k=2 IF EXISTS", [False])
            assert_none(cursor, "SELECT * FROM test WHERE k=2", cl=ConsistencyLevel.SERIAL)

            assert_one(cursor, "INSERT INTO test (k, v1) VALUES (3, 2) IF NOT EXISTS", [True])
            assert_one(cursor, "DELETE v1 FROM test WHERE k=3 IF EXISTS", [True])
            assert_one(cursor, "SELECT * FROM test WHERE k=3", [3, None], cl=ConsistencyLevel.SERIAL)
            assert_one(cursor, "DELETE v1 FROM test WHERE k=3 IF EXISTS", [True])
            assert_one(cursor, "DELETE FROM test WHERE k=3 IF EXISTS", [True])

            cursor.execute("INSERT INTO test2 (k, s, i, v) VALUES ('k', 's', 0, 'v') IF NOT EXISTS")
            assert_one(cursor, "DELETE v FROM test2 WHERE k='k' AND i=0 IF EXISTS", [True])
            assert_one(cursor, "DELETE FROM test2 WHERE k='k' AND i=0 IF EXISTS", [True])
            assert_one(cursor, "DELETE v FROM test2 WHERE k='k' AND i=0 IF EXISTS", [False])
            assert_one(cursor, "DELETE FROM test2 WHERE k='k' AND i=0 IF EXISTS", [False])

            # CASSANDRA-6430
            v = self.get_node_version(is_upgraded)
            if v >= "2.1.1" or v < "2.1" and v >= "2.0.11":
                assert_invalid(cursor, "DELETE FROM test2 WHERE k = 'k' IF EXISTS")
                assert_invalid(cursor, "DELETE FROM test2 WHERE k = 'k' IF v = 'foo'")
                assert_invalid(cursor, "DELETE FROM test2 WHERE i = 0 IF EXISTS")
                assert_invalid(cursor, "DELETE FROM test2 WHERE k = 0 AND i > 0 IF EXISTS")
                assert_invalid(cursor, "DELETE FROM test2 WHERE k = 0 AND i > 0 IF v = 'foo'")

    def test_range_key_ordered(self):
        cursor = self.prepare(ordered=True)

        cursor.execute("CREATE TABLE test ( k int PRIMARY KEY)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k) VALUES (-1)")
            cursor.execute("INSERT INTO test(k) VALUES ( 0)")
            cursor.execute("INSERT INTO test(k) VALUES ( 1)")

            assert_all(cursor, "SELECT * FROM test", [[0], [1], [-1]])
            assert_invalid(cursor, "SELECT * FROM test WHERE k >= -1 AND k < 1;")

    def test_select_with_alias(self):
        cursor = self.prepare()
        cursor.execute('CREATE TABLE users (id int PRIMARY KEY, name text)')

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            for id in range(0, 5):
                cursor.execute("INSERT INTO users (id, name) VALUES ({}, 'name{}') USING TTL 10 AND TIMESTAMP 0".format(id, id))

            # test aliasing count(*)
            res = cursor.execute('SELECT count(*) AS user_count FROM users')
            assert 'user_count' == res[0]._fields[0]
            assert 5 == res[0].user_count

            # test aliasing regular value
            res = cursor.execute('SELECT name AS user_name FROM users WHERE id = 0')
            assert 'user_name' == res[0]._fields[0]
            assert 'name0' == res[0].user_name

            # test aliasing writetime
            res = cursor.execute('SELECT writeTime(name) AS name_writetime FROM users WHERE id = 0')
            assert 'name_writetime' == res[0]._fields[0]
            assert 0 == res[0].name_writetime

            # test aliasing ttl
            res = cursor.execute('SELECT ttl(name) AS name_ttl FROM users WHERE id = 0')
            assert 'name_ttl' == res[0]._fields[0]
            assert res[0].name_ttl, (9 in 10)

            # test aliasing a regular function
            res = cursor.execute('SELECT intAsBlob(id) AS id_blob FROM users WHERE id = 0')
            assert 'id_blob' == res[0]._fields[0]
            assert '\x00\x00\x00\x00' == res[0].id_blob.decode()

            logger.debug("Current node version is {}".format(self.get_node_version(is_upgraded)))

            if self.get_node_version(is_upgraded) < LooseVersion('3.8'):
                error_msg = "Aliases aren't allowed in the where clause"
            else:
                error_msg = "Undefined column name"

            # test that select throws a meaningful exception for aliases in where clause
            assert_invalid(cursor, 'SELECT id AS user_id, name AS user_name FROM users WHERE user_id = 0', matching=error_msg)

            if self.get_node_version(is_upgraded) < LooseVersion('3.8'):
                error_msg = "Aliases are not allowed in order by clause"

            # test that select throws a meaningful exception for aliases in order by clause
            assert_invalid(cursor, 'SELECT id AS user_id, name AS user_name FROM users WHERE id IN (0) ORDER BY user_name', matching=error_msg)

    def test_nonpure_function_collection(self):
        """
        @jira_ticket CASSANDRA-5795
        """
        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v list<timeuuid>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            # we just want to make sure this doesn't throw
            cursor.execute("INSERT INTO test(k, v) VALUES (0, [now()])")

    def test_empty_in(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))")
        # Same test, but for compact
        cursor.execute("CREATE TABLE test_compact (k1 int, k2 int, v int, PRIMARY KEY (k1, k2)) WITH COMPACT STORAGE")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE test_compact")

            def fill(table):
                for i in range(0, 2):
                    for j in range(0, 2):
                        cursor.execute("INSERT INTO %s (k1, k2, v) VALUES (%d, %d, %d)" % (table, i, j, i + j))

            def assert_nothing_changed(table):
                assert_all(cursor, "SELECT * FROM {}".format(table), [[1, 0, 1], [1, 1, 2], [0, 0, 0], [0, 1, 1]])

            # Inserts a few rows to make sure we don't actually query something
            fill("test")

            # Test empty IN () in SELECT
            assert_none(cursor, "SELECT v FROM test WHERE k1 IN ()")
            assert_none(cursor, "SELECT v FROM test WHERE k1 = 0 AND k2 IN ()")

            # Test empty IN () in DELETE
            cursor.execute("DELETE FROM test WHERE k1 IN ()")
            assert_nothing_changed("test")

            # Test empty IN () in UPDATE
            cursor.execute("UPDATE test SET v = 3 WHERE k1 IN () AND k2 = 2")
            assert_nothing_changed("test")

            fill("test_compact")

            assert_none(cursor, "SELECT v FROM test_compact WHERE k1 IN ()")
            assert_none(cursor, "SELECT v FROM test_compact WHERE k1 = 0 AND k2 IN ()")

            # Test empty IN () in DELETE
            cursor.execute("DELETE FROM test_compact WHERE k1 IN ()")
            assert_nothing_changed("test_compact")

            # Test empty IN () in UPDATE
            cursor.execute("UPDATE test_compact SET v = 3 WHERE k1 IN () AND k2 = 2")
            assert_nothing_changed("test_compact")

    def test_collection_flush(self):
        """
        @jira_ticket CASSANDRA-5805
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, s set<int>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, s) VALUES (1, {1})")
            self.cluster.flush()
            cursor.execute("INSERT INTO test(k, s) VALUES (1, {2})")
            self.cluster.flush()

            assert_one(cursor, "SELECT * FROM test", [1, set([2])])

    def test_select_distinct(self):
        cursor = self.prepare(ordered=True)

        # Test a regular (CQL3) table.
        cursor.execute('CREATE TABLE regular (pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0))')
        # Test a 'compact storage' table.
        cursor.execute('CREATE TABLE compact (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))) WITH COMPACT STORAGE')
        # Test a 'wide row' thrift table.
        cursor.execute('CREATE TABLE wide (pk int, name text, val int, PRIMARY KEY(pk, name)) WITH COMPACT STORAGE')

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE regular")
            cursor.execute("TRUNCATE compact")
            cursor.execute("TRUNCATE wide")

            for i in range(0, 3):
                cursor.execute('INSERT INTO regular (pk0, pk1, ck0, val) VALUES (%d, %d, 0, 0)' % (i, i))
                cursor.execute('INSERT INTO regular (pk0, pk1, ck0, val) VALUES (%d, %d, 1, 1)' % (i, i))

            assert_all(cursor, 'SELECT DISTINCT pk0, pk1 FROM regular LIMIT 1', [[0, 0]])

            assert_all(cursor, 'SELECT DISTINCT pk0, pk1 FROM regular LIMIT 3', [[0, 0], [1, 1], [2, 2]])

            for i in range(0, 3):
                cursor.execute('INSERT INTO compact (pk0, pk1, val) VALUES (%d, %d, %d)' % (i, i, i))

            assert_all(cursor, 'SELECT DISTINCT pk0, pk1 FROM compact LIMIT 1', [[0, 0]])

            assert_all(cursor, 'SELECT DISTINCT pk0, pk1 FROM compact LIMIT 3', [[0, 0], [1, 1], [2, 2]])

            for i in range(0, 3):
                cursor.execute("INSERT INTO wide (pk, name, val) VALUES (%d, 'name0', 0)" % i)
                cursor.execute("INSERT INTO wide (pk, name, val) VALUES (%d, 'name1', 1)" % i)

            assert_all(cursor, 'SELECT DISTINCT pk FROM wide LIMIT 1', [[0]])

            assert_all(cursor, 'SELECT DISTINCT pk FROM wide LIMIT 3', [[0], [1], [2]])

            # Test selection validation.
            assert_invalid(cursor, 'SELECT DISTINCT pk0 FROM regular', matching="queries must request all the partition key columns")
            assert_invalid(cursor, 'SELECT DISTINCT pk0, pk1, ck0 FROM regular', matching="queries must only request partition key columns")

    def test_select_distinct_with_deletions(self):
        cursor = self.prepare()
        cursor.execute('CREATE TABLE t1 (k int PRIMARY KEY, c int, v int)')

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE t1")

            for i in range(10):
                cursor.execute('INSERT INTO t1 (k, c, v) VALUES (%d, %d, %d)' % (i, i, i))

            rows = list(cursor.execute('SELECT DISTINCT k FROM t1'))
            assert_length_equal(rows, 10)

            key_to_delete = rows[3].k

            cursor.execute('DELETE FROM t1 WHERE k=%d' % (key_to_delete,))
            rows = list(cursor.execute('SELECT DISTINCT k FROM t1'))

            assert_length_equal(rows, 9)

            rows = list(cursor.execute('SELECT DISTINCT k FROM t1 LIMIT 5'))
            assert_length_equal(rows, 5)

            cursor.default_fetch_size = 5
            rows = list(cursor.execute('SELECT DISTINCT k FROM t1'))
            assert_length_equal(rows, 9)

    def test_function_with_null(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timeuuid
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k) VALUES (0)")
            assert_one(cursor, "SELECT dateOf(t) FROM test WHERE k=0", [None])

    def test_cas_simple(self):
        # cursor = self.prepare(nodes=3, rf=3)
        cursor = self.prepare()

        cursor.execute("CREATE TABLE tkns (tkn int, consumed boolean, PRIMARY KEY (tkn));")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tkns")

            for i in range(1, 10):
                query = SimpleStatement("INSERT INTO tkns (tkn, consumed) VALUES ({},FALSE);".format(i), consistency_level=ConsistencyLevel.QUORUM)
                cursor.execute(query)
                assert_one(cursor, "UPDATE tkns SET consumed = TRUE WHERE tkn = {} IF consumed = FALSE;".format(i), [True], cl=ConsistencyLevel.QUORUM)
                assert_one(cursor, "UPDATE tkns SET consumed = TRUE WHERE tkn = {} IF consumed = FALSE;".format(i), [False, True], cl=ConsistencyLevel.QUORUM)

    def test_internal_application_error_on_select(self):
        """
        Test for 'Internal application error' on SELECT .. WHERE col1=val AND col2 IN (1,2)
        @jira_ticket CASSANDRA-6050
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                a int,
                b int
            )
        """)

        cursor.execute("CREATE INDEX ON test(a)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            assert_invalid(cursor, "SELECT * FROM test WHERE a = 3 AND b IN (1, 3)")

    def test_store_sets_with_if_not_exists(self):
        """
        Test to fix bug where sets are not stored by INSERT with IF NOT EXISTS
        @jira_ticket CASSANDRA-6069
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                s set<int>
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            assert_one(cursor, "INSERT INTO test(k, s) VALUES (0, {1, 2, 3}) IF NOT EXISTS", [True])
            assert_one(cursor, "SELECT * FROM test", [0, {1, 2, 3}], cl=ConsistencyLevel.SERIAL)

    def test_add_deletion_info_in_unsorted_column(self):
        """
        Test that UnsortedColumns.addAll(ColumnFamily) adds the deletion info of the CF in argument.
        @jira_ticket CASSANDRA-6115
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int, v int, PRIMARY KEY (k, v))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES (0, 1)")
            cursor.execute("BEGIN BATCH DELETE FROM test WHERE k=0 AND v=1; INSERT INTO test (k, v) VALUES (0, 2); APPLY BATCH")

            assert_one(cursor, "SELECT * FROM test", [0, 2])

    def test_column_name_validation(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k text,
                c int,
                v timeuuid,
                PRIMARY KEY (k, c)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            assert_invalid(cursor, "INSERT INTO test(k, c) VALUES ('', 0)")

            # Insert a value that don't fit 'int'
            assert_invalid(cursor, "INSERT INTO test(k, c) VALUES (0, 10000000000)")

            # Insert a non-version 1 uuid
            assert_invalid(cursor, "INSERT INTO test(k, c, v) VALUES (0, 0, 550e8400-e29b-41d4-a716-446655440000)")

    @since('2.1')
    def test_user_types(self):
        cursor = self.prepare()

        userID_1 = uuid4()
        stmt = """
              CREATE TYPE address (
              street text,
              city text,
              zip_code int,
              phones set<text>
              )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TYPE fullname (
               firstname text,
               lastname text
              )
           """
        cursor.execute(stmt)

        stmt = """
              CREATE TABLE users (
               id uuid PRIMARY KEY,
               name frozen<fullname>,
               addresses map<text, frozen<address>>
              )
           """
        cursor.execute(stmt)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE users")

            stmt = """
                  INSERT INTO users (id, name)
                  VALUES ({id}, {{ firstname: 'Paul', lastname: 'smith'}});
               """.format(id=userID_1)
            cursor.execute(stmt)

            stmt = """
                  SELECT name.firstname FROM users WHERE id = {id}
            """.format(id=userID_1)

            assert_one(cursor, stmt, ['Paul'])
            assert_one(cursor, "SELECT name.firstname FROM users WHERE id = {id}".format(id=userID_1), ['Paul'])

            stmt = """
                  UPDATE users
                  SET addresses = addresses + {{ 'home': {{ street: '...', city: 'SF', zip_code: 94102, phones: {{}} }} }}
                  WHERE id={id};
               """.format(id=userID_1)
            cursor.execute(stmt)

            stmt = """
                  SELECT addresses FROM users WHERE id = {id}
            """.format(id=userID_1)
            # TODO: deserialize the value here and check it's right.

    @since('2.1')
    def test_more_user_types(self):
        """ user type test that does a little more nesting"""
        cursor = self.prepare()

        cursor.execute("""
            CREATE TYPE type1 (
                s set<text>,
                m map<text, text>,
                l list<text>
            )
        """)

        cursor.execute("""
            CREATE TYPE type2 (
                s set<frozen<type1>>,
            )
        """)

        cursor.execute("""
            CREATE TABLE test (id int PRIMARY KEY, val frozen<type2>)
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(id, val) VALUES (0, { s : {{ s : {'foo', 'bar'}, m : { 'foo' : 'bar' }, l : ['foo', 'bar']} }})")

            # TODO: check result once we have an easy way to do it. For now we just check it doesn't crash
            cursor.execute("SELECT * FROM test")

    def test_intersection_logic_returns_empty_result(self):
        """
        Test for bug in the column slice intersection logic where select with "in" clause wrongly returns empty result
        @jira_ticket CASSANDRA-6327
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k, v)
            )
        """)

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                v int,
                c1 int,
                c2 int,
                PRIMARY KEY (k, v)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES (0, 0)")
            self.cluster.flush()
            assert_one(cursor, "SELECT v FROM test WHERE k=0 AND v IN (1, 0)", [0])
            assert_one(cursor, "SELECT v FROM test WHERE v IN (1, 0) ALLOW FILTERING", [0])

            cursor.execute("INSERT INTO test2 (k, v) VALUES (0, 0)")
            self.cluster.flush()
            assert_one(cursor, "SELECT v FROM test2 WHERE k=0 AND v IN (1, 0)", [0])
            assert_one(cursor, "SELECT v FROM test2 WHERE v IN (1, 0) ALLOW FILTERING", [0])

            cursor.execute("DELETE FROM test2 WHERE k = 0")
            cursor.execute("UPDATE test2 SET c2 = 1 WHERE k = 0 AND v = 0")
            assert_one(cursor, "SELECT v FROM test2 WHERE k=0 AND v IN (1, 0)", [0])
            cursor.execute("DELETE c2 FROM test2 WHERE k = 0 AND v = 0")
            assert_none(cursor, "SELECT v FROM test2 WHERE k=0 AND v IN (1, 0)")
            assert_none(cursor, "SELECT v FROM test2 WHERE v IN (1, 0) ALLOW FILTERING")

    def test_large_count(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.default_fetch_size = 10000
            # We know we page at 10K, so test counting just before, at 10K, just after and
            # a bit after that.
            insert_statement = cursor.prepare("INSERT INTO test(k) VALUES (?)")
            execute_concurrent_with_args(cursor, insert_statement, [(i,) for i in range(1, 10000)])

            assert_one(cursor, "SELECT COUNT(*) FROM test", [9999])

            cursor.execute(insert_statement, (10000,))
            assert_one(cursor, "SELECT COUNT(*) FROM test", [10000])

            cursor.execute(insert_statement, (10001,))
            assert_one(cursor, "SELECT COUNT(*) FROM test", [10001])

            execute_concurrent_with_args(cursor, insert_statement, [(i,) for i in range(10002, 15001)])
            assert_one(cursor, "SELECT COUNT(*) FROM test", [15000])

    @since('2.1')
    def test_collection_indexing(self):
        """
        @jira_ticket CASSANDRA-4511
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                v int,
                l list<int>,
                s set<text>,
                m map<text, int>,
                PRIMARY KEY (k, v)
            )
        """)

        cursor.execute("CREATE INDEX ON test(l)")
        cursor.execute("CREATE INDEX ON test(s)")
        cursor.execute("CREATE INDEX ON test(m)")

        time.sleep(5.0)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1})")
            cursor.execute("INSERT INTO test (k, v, l, s, m) VALUES (0, 1, [3, 4],    {'b', 'c'}, {'a' : 1, 'b' : 2})")
            cursor.execute("INSERT INTO test (k, v, l, s, m) VALUES (0, 2, [1],       {'a', 'c'}, {'c' : 3})")
            cursor.execute("INSERT INTO test (k, v, l, s, m) VALUES (1, 0, [1, 2, 4], {},         {'b' : 1})")
            cursor.execute("INSERT INTO test (k, v, l, s, m) VALUES (1, 1, [4, 5],    {'d'},      {'a' : 1, 'b' : 3})")

            # lists
            assert_all(cursor, "SELECT k, v FROM test WHERE l CONTAINS 1", [[1, 0], [0, 0], [0, 2]])
            assert_all(cursor, "SELECT k, v FROM test WHERE k = 0 AND l CONTAINS 1", [[0, 0], [0, 2]])
            assert_all(cursor, "SELECT k, v FROM test WHERE l CONTAINS 2", [[1, 0], [0, 0]])
            assert_none(cursor, "SELECT k, v FROM test WHERE l CONTAINS 6")

            # sets
            assert_all(cursor, "SELECT k, v FROM test WHERE s CONTAINS 'a'", [[0, 0], [0, 2]])
            assert_all(cursor, "SELECT k, v FROM test WHERE k = 0 AND s CONTAINS 'a'", [[0, 0], [0, 2]])
            assert_all(cursor, "SELECT k, v FROM test WHERE s CONTAINS 'd'", [[1, 1]])
            assert_none(cursor, "SELECT k, v FROM test  WHERE s CONTAINS 'e'")

            # maps
            assert_all(cursor, "SELECT k, v FROM test WHERE m CONTAINS 1", [[1, 0], [1, 1], [0, 0], [0, 1]])
            assert_all(cursor, "SELECT k, v FROM test WHERE k = 0 AND m CONTAINS 1", [[0, 0], [0, 1]])
            assert_all(cursor, "SELECT k, v FROM test WHERE m CONTAINS 2", [[0, 1]])
            assert_none(cursor, "SELECT k, v FROM test  WHERE m CONTAINS 4")

    @since('2.1')
    def test_map_keys_indexing(self):
        """
        @jira_ticket CASSANDRA-6383
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                v int,
                m map<text, int>,
                PRIMARY KEY (k, v)
            )
        """)

        cursor.execute("CREATE INDEX ON test(keys(m))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v, m) VALUES (0, 0, {'a' : 1})")
            cursor.execute("INSERT INTO test (k, v, m) VALUES (0, 1, {'a' : 1, 'b' : 2})")
            cursor.execute("INSERT INTO test (k, v, m) VALUES (0, 2, {'c' : 3})")
            cursor.execute("INSERT INTO test (k, v, m) VALUES (1, 0, {'b' : 1})")
            cursor.execute("INSERT INTO test (k, v, m) VALUES (1, 1, {'a' : 1, 'b' : 3})")

            # maps
            assert_all(cursor, "SELECT k, v FROM test WHERE m CONTAINS KEY 'a'", [[1, 1], [0, 0], [0, 1]])
            assert_all(cursor, "SELECT k, v FROM test WHERE k = 0 AND m CONTAINS KEY 'a'", [[0, 0], [0, 1]])
            assert_all(cursor, "SELECT k, v FROM test WHERE m CONTAINS KEY 'c'", [[0, 2]])
            assert_none(cursor, "SELECT k, v FROM test WHERE m CONTAINS KEY 'd'")

    def test_nan_infinity(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (f float PRIMARY KEY)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(f) VALUES (NaN)")
            cursor.execute("INSERT INTO test(f) VALUES (-NaN)")
            cursor.execute("INSERT INTO test(f) VALUES (Infinity)")
            cursor.execute("INSERT INTO test(f) VALUES (-Infinity)")

            selected = rows_to_list(cursor.execute("SELECT * FROM test"))

            # selected should be [[nan], [inf], [-inf]],
            # but assert element-wise because NaN != NaN
            assert_length_equal(selected, 3)
            assert_length_equal(selected[0], 1)
            assert math.isnan(selected[0][0])
            assert selected[1] == [float("inf")]
            assert selected[2] == [float("-inf")]

    def test_static_columns(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                v int,
                PRIMARY KEY (k, p)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, s) VALUES (0, 42)")

            assert_one(cursor, "SELECT * FROM test", [0, None, 42, None])

            # Check that writetime works (#7081) -- we can't predict the exact value easily so
            # we just check that it's non zero
            row = cursor.execute("SELECT s, writetime(s) FROM test WHERE k=0")
            assert list(row[0])[0] == 42 and list(row[0])[1] > 0

            cursor.execute("INSERT INTO test(k, p, s, v) VALUES (0, 0, 12, 0)")
            cursor.execute("INSERT INTO test(k, p, s, v) VALUES (0, 1, 24, 1)")

            # Check the static columns in indeed "static"
            assert_all(cursor, "SELECT * FROM test", [[0, 0, 24, 0], [0, 1, 24, 1]])

            # Check we do correctly get the static column value with a SELECT *, even
            # if we're only slicing part of the partition
            assert_one(cursor, "SELECT * FROM test WHERE k=0 AND p=0", [0, 0, 24, 0])
            assert_one(cursor, "SELECT * FROM test WHERE k=0 AND p=0 ORDER BY p DESC", [0, 0, 24, 0])
            assert_one(cursor, "SELECT * FROM test WHERE k=0 AND p=1", [0, 1, 24, 1])
            assert_one(cursor, "SELECT * FROM test WHERE k=0 AND p=1 ORDER BY p DESC", [0, 1, 24, 1])

            # Test for IN on the clustering key (#6769)
            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND p IN (0, 1)", [[0, 0, 24, 0], [0, 1, 24, 1]])

            # Check things still work if we don't select the static column. We also want
            # this to not request the static columns internally at all, though that part
            # require debugging to assert
            assert_one(cursor, "SELECT p, v FROM test WHERE k=0 AND p=1", [1, 1])

            # Check selecting only a static column with distinct only yield one value
            # (as we only query the static columns)
            assert_one(cursor, "SELECT DISTINCT s FROM test WHERE k=0", [24])
            # But without DISTINCT, we still get one result per row
            assert_all(cursor, "SELECT s FROM test WHERE k=0", [[24], [24]])
            # but that querying other columns does correctly yield the full partition
            assert_all(cursor, "SELECT s, v FROM test WHERE k=0", [[24, 0], [24, 1]])
            assert_one(cursor, "SELECT s, v FROM test WHERE k=0 AND p=1", [24, 1])
            assert_one(cursor, "SELECT p, s FROM test WHERE k=0 AND p=1", [1, 24])
            assert_one(cursor, "SELECT k, p, s FROM test WHERE k=0 AND p=1", [0, 1, 24])

            # Check that deleting a row don't implicitely deletes statics
            cursor.execute("DELETE FROM test WHERE k=0 AND p=0")
            assert_all(cursor, "SELECT * FROM test", [[0, 1, 24, 1]])

            # But that explicitely deleting the static column does remove it
            cursor.execute("DELETE s FROM test WHERE k=0")
            assert_all(cursor, "SELECT * FROM test", [[0, 1, None, 1]])

    @since('2.1')
    def test_static_columns_cas(self):
        """"
        @jira_ticket CASSANDRA-6839
        @jira_ticket CASSANDRA-6561
        """

        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                id int,
                k text,
                version int static,
                v text,
                PRIMARY KEY (id, k)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            # Test that INSERT IF NOT EXISTS concerns only the static column if no clustering nor regular columns
            # is provided, but concerns the CQL3 row targetted by the clustering columns otherwise
            assert_one(cursor, "INSERT INTO test(id, k, v) VALUES (1, 'foo', 'foo') IF NOT EXISTS", [True])
            assert_one(cursor, "INSERT INTO test(id, k, version) VALUES (1, 'foo', 1) IF NOT EXISTS", [False, 1, 'foo', None, 'foo'])
            assert_one(cursor, "INSERT INTO test(id, version) VALUES (1, 1) IF NOT EXISTS", [True])
            assert_one(cursor, "SELECT * FROM test", [1, 'foo', 1, 'foo'], ConsistencyLevel.SERIAL)

            # Dodgy as its not conditional, but this is not allowed with a condition and that's probably fine in practice so go with it
            cursor.execute("DELETE FROM test WHERE id = 1")

            assert_one(cursor, "INSERT INTO test(id, version) VALUES (0, 0) IF NOT EXISTS", [True])

            assert_one(cursor, "UPDATE test SET v='foo', version=1 WHERE id=0 AND k='k1' IF version = 0", [True])
            assert_all(cursor, "SELECT * FROM test", [[0, 'k1', 1, 'foo']], ConsistencyLevel.SERIAL)

            assert_one(cursor, "UPDATE test SET v='bar', version=1 WHERE id=0 AND k='k2' IF version = 0", [False, 1])
            assert_all(cursor, "SELECT * FROM test", [[0, 'k1', 1, 'foo']], ConsistencyLevel.SERIAL)

            assert_one(cursor, "UPDATE test SET v='bar', version=2 WHERE id=0 AND k='k2' IF version = 1", [True])
            assert_all(cursor, "SELECT * FROM test", [[0, 'k1', 2, 'foo'], [0, 'k2', 2, 'bar']], ConsistencyLevel.SERIAL)

            # CASSANDRA-12694 (committed in 3.0.11 and 3.10) changes the behavior below slightly.
            version = self.get_node_version(is_upgraded)
            has_12694 = (version >= '3.0.11' and version < '3.1') or (version >= '3.10')
            # Testing batches
            assert_one(cursor,
                       """
                         BEGIN BATCH
                           UPDATE test SET v='foobar' WHERE id=0 AND k='k1';
                           UPDATE test SET v='barfoo' WHERE id=0 AND k='k2';
                           UPDATE test SET version=3 WHERE id=0 IF version=1;
                         APPLY BATCH
                       """, [False, 0, 'k1', 2] if has_12694 else [False, 0, None, 2])

            assert_one(cursor,
                       """
                         BEGIN BATCH
                           UPDATE test SET v='foobar' WHERE id=0 AND k='k1';
                           UPDATE test SET v='barfoo' WHERE id=0 AND k='k2';
                           UPDATE test SET version=3 WHERE id=0 IF version=2;
                         APPLY BATCH
                       """, [True])
            assert_all(cursor, "SELECT * FROM test", [[0, 'k1', 3, 'foobar'], [0, 'k2', 3, 'barfoo']], ConsistencyLevel.SERIAL)

            assert_all(cursor,
                       """
                         BEGIN BATCH
                           UPDATE test SET version=4 WHERE id=0 IF version=3;
                           UPDATE test SET v='row1' WHERE id=0 AND k='k1' IF v='foo';
                           UPDATE test SET v='row2' WHERE id=0 AND k='k2' IF v='bar';
                         APPLY BATCH
                       """, [[False, 0, 'k1', 3, 'foobar'], [False, 0, 'k2', 3, 'barfoo']])

            assert_one(cursor,
                       """
                         BEGIN BATCH
                           UPDATE test SET version=4 WHERE id=0 IF version=3;
                           UPDATE test SET v='row1' WHERE id=0 AND k='k1' IF v='foobar';
                           UPDATE test SET v='row2' WHERE id=0 AND k='k2' IF v='barfoo';
                         APPLY BATCH
                       """, [True])

            assert_invalid(cursor,
                           """
                             BEGIN BATCH
                               UPDATE test SET version=5 WHERE id=0 IF version=4;
                               UPDATE test SET v='row1' WHERE id=0 AND k='k1';
                               UPDATE test SET v='row2' WHERE id=1 AND k='k2';
                             APPLY BATCH
                           """)

            assert_one(cursor,
                       """
                         BEGIN BATCH
                           INSERT INTO TEST (id, k, v) VALUES(1, 'k1', 'val1') IF NOT EXISTS;
                           INSERT INTO TEST (id, k, v) VALUES(1, 'k2', 'val2') IF NOT EXISTS;
                         APPLY BATCH
                       """, [True])
            assert_all(cursor, "SELECT * FROM test WHERE id=1", [[1, 'k1', None, 'val1'], [1, 'k2', None, 'val2']], ConsistencyLevel.SERIAL)

            assert_one(cursor,
                       """
                         BEGIN BATCH
                           INSERT INTO TEST (id, k, v) VALUES(1, 'k2', 'val2') IF NOT EXISTS;
                           INSERT INTO TEST (id, k, v) VALUES(1, 'k3', 'val3') IF NOT EXISTS;
                         APPLY BATCH
                       """, [False, 1, 'k2', None, 'val2'])

            assert_one(cursor,
                       """
                         BEGIN BATCH
                           UPDATE test SET v='newVal' WHERE id=1 AND k='k2' IF v='val0';
                           INSERT INTO TEST (id, k, v) VALUES(1, 'k3', 'val3') IF NOT EXISTS;
                         APPLY BATCH
                       """, [False, 1, 'k2', None, 'val2'])
            assert_all(cursor, "SELECT * FROM test WHERE id=1", [[1, 'k1', None, 'val1'], [1, 'k2', None, 'val2']], ConsistencyLevel.SERIAL)

            assert_one(cursor,
                       """
                         BEGIN BATCH
                           UPDATE test SET v='newVal' WHERE id=1 AND k='k2' IF v='val2';
                           INSERT INTO TEST (id, k, v, version) VALUES(1, 'k3', 'val3', 1) IF NOT EXISTS;
                         APPLY BATCH
                       """, [True])
            assert_all(cursor, "SELECT * FROM test WHERE id=1", [[1, 'k1', 1, 'val1'], [1, 'k2', 1, 'newVal'], [1, 'k3', 1, 'val3']], ConsistencyLevel.SERIAL)

            assert_one(cursor,
                       """
                         BEGIN BATCH
                           UPDATE test SET v='newVal1' WHERE id=1 AND k='k2' IF v='val2';
                           UPDATE test SET v='newVal2' WHERE id=1 AND k='k2' IF v='val3';
                         APPLY BATCH
                       """, [False, 1, 'k2', 'newVal'])

    def test_static_columns_with_2i(self):
        cursor = self.prepare()
        initial_version = self.cluster.version()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                v int,
                PRIMARY KEY (k, p)
            )
        """)

        cursor.execute("CREATE INDEX ON test(v)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, p, s, v) VALUES (0, 0, 42, 1)")
            cursor.execute("INSERT INTO test(k, p, v) VALUES (0, 1, 1)")
            cursor.execute("INSERT INTO test(k, p, v) VALUES (0, 2, 2)")

            assert_all(cursor, "SELECT * FROM test WHERE v = 1", [[0, 0, 42, 1], [0, 1, 42, 1]])
            assert_all(cursor, "SELECT p, s FROM test WHERE v = 1", [[0, 42], [1, 42]])
            assert_all(cursor, "SELECT p FROM test WHERE v = 1", [[0], [1]])
            if initial_version >= LooseVersion('3.11.7'):  # See CASSANDRA-16332. This fails with a known limitation on versions before 3.11.7 (CASSANDRA-14242)
                assert_all(cursor, "SELECT s FROM test WHERE v = 1", [[42], [42]])

    @since('2.1')
    def test_static_columns_with_distinct(self):
        """
        @jira_ticket CASSANDRA-8087
        @jira_ticket CASSANDRA-8108
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                PRIMARY KEY (k, p)
            )
        """)

        # additional testing for CASSANRA-8087
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                s1 int static,
                s2 int static,
                PRIMARY KEY (k, c1, c2)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE test2")

            cursor.execute("INSERT INTO test (k, p) VALUES (1, 1)")
            cursor.execute("INSERT INTO test (k, p) VALUES (1, 2)")

            assert_all(cursor, "SELECT k, s FROM test", [[1, None], [1, None]])
            assert_one(cursor, "SELECT DISTINCT k, s FROM test", [1, None])
            assert_one(cursor, "SELECT DISTINCT s FROM test WHERE k=1", [None])
            assert_none(cursor, "SELECT DISTINCT s FROM test WHERE k=2")

            cursor.execute("INSERT INTO test (k, p, s) VALUES (2, 1, 3)")
            cursor.execute("INSERT INTO test (k, p) VALUES (2, 2)")

            assert_all(cursor, "SELECT k, s FROM test", [[1, None], [1, None], [2, 3], [2, 3]])
            assert_all(cursor, "SELECT DISTINCT k, s FROM test", [[1, None], [2, 3]])
            assert_one(cursor, "SELECT DISTINCT s FROM test WHERE k=1", [None])
            assert_one(cursor, "SELECT DISTINCT s FROM test WHERE k=2", [3])

            assert_invalid(cursor, "SELECT DISTINCT s FROM test")

            # paging to test for CASSANDRA-8108
            cursor.execute("TRUNCATE test")
            for i in range(10):
                for j in range(10):
                    cursor.execute("INSERT INTO test (k, p, s) VALUES (%s, %s, %s)", (i, j, i))

            cursor.default_fetch_size = 7
            rows = list(cursor.execute("SELECT DISTINCT k, s FROM test"))
            assert list(range(10)) == sorted([r[0] for r in rows])
            assert list(range(10)) == sorted([r[1] for r in rows])

            keys = ",".join(map(str, list(range(10))))

            rows = list(cursor.execute("SELECT DISTINCT k, s FROM test WHERE k IN ({})".format(keys)))
            assert list(range(10)) == [r[0] for r in rows]
            assert list(range(10)) == [r[1] for r in rows]

            # additional testing for CASSANRA-8087
            for i in range(10):
                for j in range(5):
                    for k in range(5):
                        cursor.execute("INSERT INTO test2 (k, c1, c2, s1, s2) VALUES ({}, {}, {}, {}, {})".format(i, j, k, i, i + 1))

            for fetch_size in (None, 2, 5, 7, 10, 24, 25, 26, 1000):
                cursor.default_fetch_size = fetch_size
                rows = list(cursor.execute("SELECT DISTINCT k, s1 FROM test2"))
                assert list(range(10)) == sorted([r[0] for r in rows])
                assert list(range(10)) == sorted([r[1] for r in rows])

                rows = list(cursor.execute("SELECT DISTINCT k, s2 FROM test2"))
                assert list(range(10)) == sorted([r[0] for r in rows])
                assert list(range(1, 11)) == sorted([r[1] for r in rows])

                rows = list(cursor.execute("SELECT DISTINCT k, s1 FROM test2 LIMIT 10"))
                assert list(range(10)) == sorted([r[0] for r in rows])
                assert list(range(10)) == sorted([r[1] for r in rows])

                keys = ",".join(map(str, list(range(10))))
                rows = list(cursor.execute("SELECT DISTINCT k, s1 FROM test2 WHERE k IN (%s)" % (keys,)))
                assert list(range(10)) == [r[0] for r in rows]
                assert list(range(10)) == [r[1] for r in rows]

                keys = ",".join(map(str, list(range(10))))
                rows = list(cursor.execute("SELECT DISTINCT k, s2 FROM test2 WHERE k IN (%s)" % (keys,)))
                assert list(range(10)) == [r[0] for r in rows]
                assert list(range(1, 11)) == [r[1] for r in rows]

                keys = ",".join(map(str, list(range(10))))
                rows = list(cursor.execute("SELECT DISTINCT k, s1 FROM test2 WHERE k IN (%s) LIMIT 10" % (keys,)))
                assert list(range(10)) == sorted([r[0] for r in rows])
                assert list(range(10)) == sorted([r[1] for r in rows])

    def test_select_count_paging(self):
        """
        Test for the #6579 'select count' paging bug
        @jira_ticket CASSANDRA-6579
        """
        cursor = self.prepare()
        cursor.execute("create table test(field1 text, field2 timeuuid, field3 boolean, primary key(field1, field2));")
        cursor.execute("create index test_index on test(field3);")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("insert into test(field1, field2, field3) values ('hola', now(), false);")
            cursor.execute("insert into test(field1, field2, field3) values ('hola', now(), false);")

            # the result depends on which node we're connected to, see CASSANDRA-8216
            if self.get_node_version(is_upgraded) >= '2.2':
                # the coordinator is the upgraded 2.2+ node
                assert_one(cursor, "select count(*) from test where field3 = false limit 1;", [2])
            else:
                # the coordinator is the not-upgraded 2.1 node
                assert_one(cursor, "select count(*) from test where field3 = false limit 1;", [1])

    def test_cas_and_ttl(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v int, lock boolean)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v, lock) VALUES (0, 0, false)")
            cursor.execute("UPDATE test USING TTL 1 SET lock=true WHERE k=0")
            time.sleep(2)
            assert_one(cursor, "UPDATE test SET v = 1 WHERE k = 0 IF lock = null", [True])

    def test_tuple_notation(self):
        """
        Test the syntax introduced in CASSANDRA-4851
        @jira_ticket CASSANDRA-4851
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2, v3))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            for i in range(0, 2):
                for j in range(0, 2):
                    for k in range(0, 2):
                        cursor.execute("INSERT INTO test(k, v1, v2, v3) VALUES (0, %d, %d, %d)" % (i, j, k))

            assert_all(cursor, "SELECT v1, v2, v3 FROM test WHERE k = 0", [[0, 0, 0],
                                                                           [0, 0, 1],
                                                                           [0, 1, 0],
                                                                           [0, 1, 1],
                                                                           [1, 0, 0],
                                                                           [1, 0, 1],
                                                                           [1, 1, 0],
                                                                           [1, 1, 1]])

            assert_all(cursor, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)", [[1, 0, 1], [1, 1, 0], [1, 1, 1]])
            assert_all(cursor, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2) >= (1, 1)", [[1, 1, 0], [1, 1, 1]])
            assert_all(cursor, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2) > (0, 1) AND (v1, v2, v3) <= (1, 1, 0)", [[1, 0, 0], [1, 0, 1], [1, 1, 0]])

            assert_invalid(cursor, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v3) > (1, 0)")

    @since('2.0', max_version='2.99')  # 3.0+ not compatible with protocol version 2
    def test_v2_protocol_IN_with_tuples(self):
        """
        @jira_ticket CASSANDRA-8062
        """
        cursor = self.prepare(protocol_version=2)
        cursor.execute("CREATE TABLE test (k int, c1 int, c2 text, PRIMARY KEY (k, c1, c2))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))

            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, c1, c2) VALUES (0, 0, 'a')")
            cursor.execute("INSERT INTO test (k, c1, c2) VALUES (0, 0, 'b')")
            cursor.execute("INSERT INTO test (k, c1, c2) VALUES (0, 0, 'c')")

            p = cursor.prepare("SELECT * FROM test WHERE k=? AND (c1, c2) IN ?")
            rows = list(cursor.execute(p, (0, [(0, 'b'), (0, 'c')])))
            assert 2 == len(rows)
            assert_length_equal(rows, 2)
            assert (0, 0, 'b') == rows[0]
            assert (0, 0, 'c') == rows[1]

    def test_in_with_desc_order(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 0)")
            cursor.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 1)")
            cursor.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 2)")

            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (0, 2)", [[0, 0, 0], [0, 0, 2]])
            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)", [[0, 0, 0], [0, 0, 2]])
            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC", [[0, 0, 0], [0, 0, 2]])
            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC", [[0, 0, 2], [0, 0, 0]])
            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (0, 2) ORDER BY c1 ASC", [[0, 0, 0], [0, 0, 2]])
            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (0, 2) ORDER BY c1 DESC", [[0, 0, 2], [0, 0, 0]])

    @since('2.1')
    def test_in_order_by_without_selecting(self):
        """
        Test that columns don't need to be selected for ORDER BY when there is a IN
        @jira_ticket CASSANDRA-4911
        """
        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.default_fetch_size = None

            cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (0, 0, 0, 0)")
            cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (0, 0, 1, 1)")
            cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (0, 0, 2, 2)")
            cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (1, 1, 0, 3)")
            cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (1, 1, 1, 4)")
            cursor.execute("INSERT INTO test(k, c1, c2, v) VALUES (1, 1, 2, 5)")

            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)", [[0, 0, 0, 0], [0, 0, 2, 2]])
            assert_all(cursor, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC, c2 ASC", [[0, 0, 0, 0], [0, 0, 2, 2]])

            # check that we don't need to select the column on which we order
            assert_all(cursor, "SELECT v FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)", [[0], [2]])
            assert_all(cursor, "SELECT v FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC", [[0], [2]])
            assert_all(cursor, "SELECT v FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC", [[2], [0]])

            if self.get_node_version(is_upgraded) >= '2.1.17':
                # the coordinator is the upgraded 2.2+ node or a node with CASSSANDRA-12420
                assert_all(cursor, "SELECT v FROM test WHERE k IN (1, 0)", [[0], [1], [2], [3], [4], [5]])
            else:
                # the coordinator is the non-upgraded 2.1 node
                assert_all(cursor, "SELECT v FROM test WHERE k IN (1, 0)", [[3], [4], [5], [0], [1], [2]])
            assert_all(cursor, "SELECT v FROM test WHERE k IN (1, 0) ORDER BY c1 ASC", [[0], [1], [2], [3], [4], [5]])

            # we should also be able to use functions in the select clause (additional test for CASSANDRA-8286)
            results = list(cursor.execute("SELECT writetime(v) FROM test WHERE k IN (1, 0) ORDER BY c1 ASC"))
            # since we don't know the write times, just assert that the order matches the order we expect
            assert results == list(sorted(results))

    def test_cas_and_compact(self):
        """
        Test for CAS with compact storage table, and #6813 in particular
        @jira_ticket CASSANDRA-6813
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE lock (
                partition text,
                key text,
                owner text,
                PRIMARY KEY (partition, key)
            ) WITH COMPACT STORAGE
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE lock")

            cursor.execute("INSERT INTO lock(partition, key, owner) VALUES ('a', 'b', null)")
            assert_one(cursor, "UPDATE lock SET owner='z' WHERE partition='a' AND key='b' IF owner=null", [True])

            assert_one(cursor, "UPDATE lock SET owner='b' WHERE partition='a' AND key='b' IF owner='a'", [False, 'z'])
            assert_one(cursor, "UPDATE lock SET owner='b' WHERE partition='a' AND key='b' IF owner='z'", [True])

            assert_one(cursor, "INSERT INTO lock(partition, key, owner) VALUES ('a', 'c', 'x') IF NOT EXISTS", [True])

    @since('2.1.1')
    def test_whole_list_conditional(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE tlist (
                k int PRIMARY KEY,
                l list<text>
            )""")

        cursor.execute("""
            CREATE TABLE frozentlist (
                k int PRIMARY KEY,
                l frozen<list<text>>
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tlist")
            cursor.execute("TRUNCATE frozentlist")

            for frozen in (False, True):

                table = "frozentlist" if frozen else "tlist"
                cursor.execute("INSERT INTO {}(k, l) VALUES (0, ['foo', 'bar', 'foobar'])".format(table))

                def check_applies(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {}".format(table, condition), [True], cl=self.CL)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])  # read back at default cl.one
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [True], cl=self.CL)
                    assert_none(cursor, "SELECT * FROM {}".format(table))  # read back at default cl.one
                    cursor.execute("INSERT INTO {}(k, l) VALUES (0, ['foo', 'bar', 'foobar'])".format(table))

                check_applies("l = ['foo', 'bar', 'foobar']")
                check_applies("l != ['baz']")
                check_applies("l > ['a']")
                check_applies("l >= ['a']")
                check_applies("l < ['z']")
                check_applies("l <= ['z']")
                check_applies("l IN (null, ['foo', 'bar', 'foobar'], ['a'])")

                # multiple conditions
                check_applies("l > ['aaa', 'bbb'] AND l > ['aaa']")
                check_applies("l != null AND l IN (['foo', 'bar', 'foobar'])")

                def check_does_not_apply(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {}".format(table, condition),
                               [False, ['foo', 'bar', 'foobar']], cl=self.CL)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])  # read back at default cl.one
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition),
                               [False, ['foo', 'bar', 'foobar']], cl=self.CL)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])  # read back at default cl.one

                # should not apply
                check_does_not_apply("l = ['baz']")
                check_does_not_apply("l != ['foo', 'bar', 'foobar']")
                check_does_not_apply("l > ['z']")
                check_does_not_apply("l >= ['z']")
                check_does_not_apply("l < ['a']")
                check_does_not_apply("l <= ['a']")
                check_does_not_apply("l IN (['a'], null)")
                check_does_not_apply("l IN ()")

                # multiple conditions
                check_does_not_apply("l IN () AND l IN (['foo', 'bar', 'foobar'])")
                check_does_not_apply("l > ['zzz'] AND l < ['zzz']")

                def check_invalid(condition, expected=InvalidRequest):
                    # UPDATE statement
                    assert_invalid(cursor, "UPDATE {} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']], cl=self.CL)
                    # DELETE statement
                    assert_invalid(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']], cl=self.CL)

                check_invalid("l = [null]")
                check_invalid("l < null")
                check_invalid("l <= null")
                check_invalid("l > null")
                check_invalid("l >= null")
                check_invalid("l IN null", expected=SyntaxException)
                check_invalid("l IN 367", expected=SyntaxException)

                # @jira_ticket CASSANDRA-10537
                if self.get_node_version(is_upgraded) >= LooseVersion(CASSANDRA_4_1):
                    check_applies("l CONTAINS 'bar'")
                    check_applies("l CONTAINS 'foo' AND l CONTAINS 'foobar'")
                    check_does_not_apply("l CONTAINS 'baz'")
                    check_does_not_apply("l CONTAINS 'bar' AND l CONTAINS 'baz'")
                    check_invalid("m CONTAINS 'bar'")
                    check_invalid("l CONTAINS KEY 123")
                else:
                    check_invalid("m CONTAINS 'bar'", expected=SyntaxException)
                    check_invalid("l CONTAINS KEY 123", expected=SyntaxException)

    @since('2.1')
    def test_list_item_conditional(self):
        # Lists
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE tlist (
                k int PRIMARY KEY,
                l list<text>
            )""")

        cursor.execute("""
            CREATE TABLE frozentlist (
                k int PRIMARY KEY,
                l frozen<list<text>>
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tlist")
            cursor.execute("TRUNCATE frozentlist")

            for frozen in (False, True):

                table = "frozentlist" if frozen else "tlist"

                assert_one(cursor, "INSERT INTO %s(k, l) VALUES (0, ['foo', 'bar', 'foobar']) IF NOT EXISTS" % (table,), [True])

                assert_invalid(cursor, "DELETE FROM %s WHERE k=0 IF l[null] = 'foobar'" % (table,))
                assert_invalid(cursor, "DELETE FROM %s WHERE k=0 IF l[-2] = 'foobar'" % (table,))
                assert_one(cursor, "DELETE FROM %s WHERE k=0 IF l[1] = null" % (table,), [False, ['foo', 'bar', 'foobar']])
                assert_one(cursor, "DELETE FROM %s WHERE k=0 IF l[1] = 'foobar'" % (table,), [False, ['foo', 'bar', 'foobar']])
                assert_one(cursor, "SELECT * FROM %s" % (table,), [0, ['foo', 'bar', 'foobar']], cl=ConsistencyLevel.SERIAL)

                assert_one(cursor, "DELETE FROM %s WHERE k=0 IF l[1] = 'bar'" % (table,), [True])
                assert_none(cursor, "SELECT * FROM %s" % (table,), cl=ConsistencyLevel.SERIAL)

    @since('2.1.1')
    def test_expanded_list_item_conditional(self):
        """
        expanded functionality from CASSANDRA-6839
        @jira_ticket CASSANDRA-6839
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE tlist (
                k int PRIMARY KEY,
                l list<text>
            )""")

        cursor.execute("""
            CREATE TABLE frozentlist (
                k int PRIMARY KEY,
                l frozen<list<text>>
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tlist")
            cursor.execute("TRUNCATE frozentlist")

            for frozen in (False, True):

                table = "frozentlist" if frozen else "tlist"

                cursor.execute("INSERT INTO {}(k, l) VALUES (0, ['foo', 'bar', 'foobar'])".format(table,))

                def check_applies(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {}".format(table, condition), [True])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [True])
                    assert_none(cursor, "SELECT * FROM {}".format(table), cl=ConsistencyLevel.SERIAL)
                    cursor.execute("INSERT INTO {}(k, l) VALUES (0, ['foo', 'bar', 'foobar'])".format(table,))


                check_applies("l[1] < 'zzz'")
                check_applies("l[1] <= 'bar'")
                check_applies("l[1] > 'aaa'")
                check_applies("l[1] >= 'bar'")
                check_applies("l[1] != 'xxx'")
                check_applies("l[1] != null")
                check_applies("l[1] IN (null, 'xxx', 'bar')")
                check_applies("l[1] > 'aaa' AND l[1] < 'zzz'")

                # check beyond end of list
                check_applies("l[3] = null")
                check_applies("l[3] IN (null, 'xxx', 'bar')")

                def check_does_not_apply(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {}".format(table, condition), [False, ['foo', 'bar', 'foobar']])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [False, ['foo', 'bar', 'foobar']])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])

                check_does_not_apply("l[1] < 'aaa'")
                check_does_not_apply("l[1] <= 'aaa'")
                check_does_not_apply("l[1] > 'zzz'")
                check_does_not_apply("l[1] >= 'zzz'")
                check_does_not_apply("l[1] != 'bar'")
                check_does_not_apply("l[1] IN (null, 'xxx')")
                check_does_not_apply("l[1] IN ()")
                check_does_not_apply("l[1] != null AND l[1] IN ()")

                # check beyond end of list
                check_does_not_apply("l[3] != null")
                check_does_not_apply("l[3] = 'xxx'")

                def check_invalid(condition, expected=InvalidRequest):
                    # UPDATE statement
                    assert_invalid(cursor, "UPDATE {} SET l = ['foo', 'bar', 'foobar'] WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])
                    # DELETE statement
                    assert_invalid(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, ['foo', 'bar', 'foobar']])

                check_invalid("l[1] < null")
                check_invalid("l[1] <= null")
                check_invalid("l[1] > null")
                check_invalid("l[1] >= null")
                check_invalid("l[1] IN null", expected=SyntaxException)
                check_invalid("l[1] IN 367", expected=SyntaxException)
                check_invalid("l[1] IN (1, 2, 3)")
                check_invalid("l[1] CONTAINS 367", expected=SyntaxException)
                check_invalid("l[1] CONTAINS KEY 367", expected=SyntaxException)
                check_invalid("l[null] = null")

    @since('2.1.1')
    def test_whole_set_conditional(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE tset (
                k int PRIMARY KEY,
                s set<text>
            )""")

        cursor.execute("""
            CREATE TABLE frozentset (
                k int PRIMARY KEY,
                s frozen<set<text>>
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tset")
            cursor.execute("TRUNCATE frozentset")

            for frozen in (False, True):

                table = "frozentset" if frozen else "tset"
                assert_one(cursor, "INSERT INTO {}(k, s) VALUES (0, {{'bar', 'foo'}}) IF NOT EXISTS".format(table), [True])

                def check_applies(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET s = {{'bar', 'foo'}} WHERE k=0 IF {}".format(table, condition), [True])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'bar', 'foo'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [True])
                    assert_none(cursor, "SELECT * FROM {}".format(table), cl=ConsistencyLevel.SERIAL)
                    assert_one(cursor, "INSERT INTO {}(k, s) VALUES (0, {{'bar', 'foo'}}) IF NOT EXISTS".format(table), [True])

                check_applies("s = {'bar', 'foo'}")
                check_applies("s = {'foo', 'bar'}")
                check_applies("s != {'baz'}")
                check_applies("s > {'a'}")
                check_applies("s >= {'a'}")
                check_applies("s < {'z'}")
                check_applies("s <= {'z'}")
                check_applies("s IN (null, {'bar', 'foo'}, {'a'})")

                # multiple conditions
                check_applies("s > {'a'} AND s < {'z'}")
                check_applies("s IN (null, {'bar', 'foo'}, {'a'}) AND s IN ({'a'}, {'bar', 'foo'}, null)")

                def check_does_not_apply(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET s = {{'bar', 'foo'}} WHERE k=0 IF {}".format(table, condition),
                               [False, {'bar', 'foo'}])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'bar', 'foo'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition),
                               [False, {'bar', 'foo'}])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'bar', 'foo'}], cl=ConsistencyLevel.SERIAL)

                # should not apply
                check_does_not_apply("s = {'baz'}")
                check_does_not_apply("s != {'bar', 'foo'}")
                check_does_not_apply("s > {'z'}")
                check_does_not_apply("s >= {'z'}")
                check_does_not_apply("s < {'a'}")
                check_does_not_apply("s <= {'a'}")
                check_does_not_apply("s IN ({'a'}, null)")
                check_does_not_apply("s IN ()")
                check_does_not_apply("s != null AND s IN ()")

                def check_invalid(condition, expected=InvalidRequest):
                    # UPDATE statement
                    assert_invalid(cursor, "UPDATE {} SET s = {{'bar', 'foo'}} WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'bar', 'foo'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_invalid(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'bar', 'foo'}], cl=ConsistencyLevel.SERIAL)

                check_invalid("s = {null}")
                check_invalid("s < null")
                check_invalid("s <= null")
                check_invalid("s > null")
                check_invalid("s >= null")
                check_invalid("s IN null", expected=SyntaxException)
                check_invalid("s IN 367", expected=SyntaxException)

                # element access is not allow for sets
                check_invalid("s['foo'] = 'foobar'")

                # @jira_ticket CASSANDRA-10537
                if self.get_node_version(is_upgraded) >= LooseVersion(CASSANDRA_4_1):
                    check_applies("s CONTAINS 'foo'")
                    check_applies("s CONTAINS 'foo' AND s CONTAINS 'bar'")
                    check_does_not_apply("s CONTAINS 'baz'")
                    check_invalid("s CONTAINS null")
                    check_invalid("s CONTAINS KEY 123")
                else:
                    check_invalid("s CONTAINS null", expected=SyntaxException)
                    check_invalid("s CONTAINS KEY 123", expected=SyntaxException)

    @since('2.1.1')
    def test_whole_map_conditional(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE tmap (
                k int PRIMARY KEY,
                m map<text, text>
            )""")

        cursor.execute("""
            CREATE TABLE frozentmap (
                k int PRIMARY KEY,
                m frozen<map<text, text>>
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tmap")
            cursor.execute("TRUNCATE frozentmap")

            for frozen in (False, True):
                logger.debug("Testing {} maps".format("frozen" if frozen else "normal"))

                table = "frozentmap" if frozen else "tmap"
                cursor.execute("INSERT INTO %s(k, m) VALUES (0, {'foo' : 'bar'})" % (table,))

                def check_applies(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET m = {{'foo': 'bar'}} WHERE k=0 IF {}".format(table, condition), [True])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [True])
                    assert_none(cursor, "SELECT * FROM {}".format(table), cl=ConsistencyLevel.SERIAL)
                    cursor.execute("INSERT INTO {}(k, m) VALUES (0, {{'foo' : 'bar'}})".format(table))

                check_applies("m = {'foo': 'bar'}")
                check_applies("m > {'a': 'a'}")
                check_applies("m >= {'a': 'a'}")
                check_applies("m < {'z': 'z'}")
                check_applies("m <= {'z': 'z'}")
                check_applies("m != {'a': 'a'}")
                check_applies("m IN (null, {'a': 'a'}, {'foo': 'bar'})")

                # multiple conditions
                check_applies("m > {'a': 'a'} AND m < {'z': 'z'}")
                check_applies("m != null AND m IN (null, {'a': 'a'}, {'foo': 'bar'})")

                def check_does_not_apply(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET m = {{'foo': 'bar'}} WHERE k=0 IF {}".format(table, condition), [False, {'foo': 'bar'}])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [False, {'foo': 'bar'}])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)

                # should not apply
                check_does_not_apply("m = {'a': 'a'}")
                check_does_not_apply("m > {'z': 'z'}")
                check_does_not_apply("m >= {'z': 'z'}")
                check_does_not_apply("m < {'a': 'a'}")
                check_does_not_apply("m <= {'a': 'a'}")
                check_does_not_apply("m != {'foo': 'bar'}")
                check_does_not_apply("m IN ({'a': 'a'}, null)")
                check_does_not_apply("m IN ()")
                check_does_not_apply("m = null AND m != null")

                def check_invalid(condition, expected=InvalidRequest):
                    # UPDATE statement
                    assert_invalid(cursor, "UPDATE {} SET m = {{'foo': 'bar'}} WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_invalid(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)

                check_invalid("m = {null: null}")
                check_invalid("m = {'a': null}")
                check_invalid("m = {null: 'a'}")
                check_invalid("m < null")
                check_invalid("m IN null", expected=SyntaxException)

                # @jira_ticket CASSANDRA-10537
                if self.get_node_version(is_upgraded) >= LooseVersion(CASSANDRA_4_1):
                    check_applies("m CONTAINS 'bar'")
                    check_applies("m CONTAINS KEY 'foo'")
                    check_applies("m CONTAINS 'bar' AND m CONTAINS KEY 'foo'")
                    check_does_not_apply("m CONTAINS 'foo'")
                    check_does_not_apply("m CONTAINS KEY 'bar'")
                    check_invalid("m CONTAINS null")
                    check_invalid("m CONTAINS KEY null")
                else:
                    check_invalid("m CONTAINS 'bar'", expected=SyntaxException)
                    check_invalid("m CONTAINS KEY 'foo'", expected=SyntaxException)
                    check_invalid("m CONTAINS null", expected=SyntaxException)
                    check_invalid("m CONTAINS KEY null", expected=SyntaxException)

    @since('2.1')
    def test_map_item_conditional(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE tmap (
                k int PRIMARY KEY,
                m map<text, text>
            )""")

        cursor.execute("""
            CREATE TABLE frozentmap (
                k int PRIMARY KEY,
                m frozen<map<text, text>>
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tmap")
            cursor.execute("TRUNCATE frozentmap")

            for frozen in (False, True):

                table = "frozentmap" if frozen else "tmap"
                assert_one(cursor, "INSERT INTO %s(k, m) VALUES (0, {'foo' : 'bar'}) IF NOT EXISTS" % (table,), [True])
                assert_invalid(cursor, "DELETE FROM %s WHERE k=0 IF m[null] = 'foo'" % (table,))
                assert_one(cursor, "DELETE FROM %s WHERE k=0 IF m['foo'] = 'foo'" % (table,), [False, {'foo': 'bar'}])
                assert_one(cursor, "DELETE FROM %s WHERE k=0 IF m['foo'] = null" % (table,), [False, {'foo': 'bar'}])
                assert_one(cursor, "SELECT * FROM %s" % (table,), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)

                assert_one(cursor, "DELETE FROM %s WHERE k=0 IF m['foo'] = 'bar'" % (table,), [True])
                assert_none(cursor, "SELECT * FROM %s" % (table,), cl=ConsistencyLevel.SERIAL)

                if self.get_node_version(is_upgraded) > "2.1.1":
                    cursor.execute("INSERT INTO %s(k, m) VALUES (1, null)" % (table,))
                    if frozen:
                        assert_invalid(cursor, "UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)" % (table,))
                    else:
                        assert_one(cursor, "UPDATE %s set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)" % (table,), [True])

    @since('2.1.1')
    def test_expanded_map_item_conditional(self):
        """
        Expanded functionality from CASSANDRA-6839
        @jira_ticket CASSANDRA-6839
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE tmap (
                k int PRIMARY KEY,
                m map<text, text>
            )""")

        cursor.execute("""
            CREATE TABLE frozentmap (
                k int PRIMARY KEY,
                m frozen<map<text, text>>
            )""")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE tmap")
            cursor.execute("TRUNCATE frozentmap")

            for frozen in (False, True):
                logger.debug("Testing {} maps".format("frozen" if frozen else "normal"))

                table = "frozentmap" if frozen else "tmap"
                cursor.execute("INSERT INTO %s (k, m) VALUES (0, {'foo' : 'bar'})" % table)

                def check_applies(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET m = {{'foo': 'bar'}} WHERE k=0 IF {}".format(table, condition), [True])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [True])
                    assert_none(cursor, "SELECT * FROM {}".format(table), cl=ConsistencyLevel.SERIAL)
                    cursor.execute("INSERT INTO {} (k, m) VALUES (0, {{'foo' : 'bar'}})".format(table))

                check_applies("m['xxx'] = null")
                check_applies("m['foo'] < 'zzz'")
                check_applies("m['foo'] <= 'bar'")
                check_applies("m['foo'] > 'aaa'")
                check_applies("m['foo'] >= 'bar'")
                check_applies("m['foo'] != 'xxx'")
                check_applies("m['foo'] != null")
                check_applies("m['foo'] IN (null, 'xxx', 'bar')")
                check_applies("m['xxx'] IN (null, 'xxx', 'bar')")  # m['xxx'] is not set

                # multiple conditions
                check_applies("m['foo'] < 'zzz' AND m['foo'] > 'aaa'")

                def check_does_not_apply(condition):
                    # UPDATE statement
                    assert_one(cursor, "UPDATE {} SET m = {{'foo': 'bar'}} WHERE k=0 IF {}".format(table, condition), [False, {'foo': 'bar'}])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)
                    # DELETE statement
                    assert_one(cursor, "DELETE FROM {} WHERE k=0 IF {}".format(table, condition), [False, {'foo': 'bar'}])
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}], cl=ConsistencyLevel.SERIAL)

                check_does_not_apply("m['foo'] < 'aaa'")
                check_does_not_apply("m['foo'] <= 'aaa'")
                check_does_not_apply("m['foo'] > 'zzz'")
                check_does_not_apply("m['foo'] >= 'zzz'")
                check_does_not_apply("m['foo'] != 'bar'")
                check_does_not_apply("m['xxx'] != null")  # m['xxx'] is not set
                check_does_not_apply("m['foo'] IN (null, 'xxx')")
                check_does_not_apply("m['foo'] IN ()")
                check_does_not_apply("m['foo'] != null AND m['foo'] = null")

                def check_invalid(condition, expected=InvalidRequest):
                    # UPDATE statement
                    assert_invalid(cursor, "UPDATE {} SET m = {{'foo': 'bar'}} WHERE k=0 IF {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}])
                    # DELETE statement
                    assert_invalid(cursor, "DELETE FROM {} WHERE k=0 IF  {}".format(table, condition), expected=expected)
                    assert_one(cursor, "SELECT * FROM {}".format(table), [0, {'foo': 'bar'}])

                check_invalid("m['foo'] < null")
                check_invalid("m['foo'] <= null")
                check_invalid("m['foo'] > null")
                check_invalid("m['foo'] >= null")
                check_invalid("m['foo'] IN null", expected=SyntaxException)
                check_invalid("m['foo'] IN 367", expected=SyntaxException)
                check_invalid("m['foo'] IN (1, 2, 3)")
                check_invalid("m['foo'] CONTAINS 367", expected=SyntaxException)
                check_invalid("m['foo'] CONTAINS KEY 367", expected=SyntaxException)
                check_invalid("m[null] = null")

    @since("2.1.1")
    def test_cas_and_list_index(self):
        """
        @jira_ticket CASSANDRA-7499
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v text,
                l list<text>
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, v, l) VALUES(0, 'foobar', ['foi', 'bar'])")

            assert_one(cursor, "UPDATE test SET l[0] = 'foo' WHERE k = 0 IF v = 'barfoo'", [False, 'foobar'])
            assert_one(cursor, "UPDATE test SET l[0] = 'foo' WHERE k = 0 IF v = 'foobar'", [True])

            # since we write at all, and LWT update (serial), we need to read back at serial (or higher)
            assert_one(cursor, "SELECT * FROM test", [0, ['foo', 'bar'], 'foobar'], cl=ConsistencyLevel.SERIAL)

    @since("2.0")
    def test_static_with_limit(self):
        """
        Test LIMIT when static columns are present
        @jira_ticket CASSANDRA-6956
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                s int static,
                v int,
                PRIMARY KEY (k, v)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k, s) VALUES(0, 42)")
            for i in range(0, 4):
                cursor.execute("INSERT INTO test(k, v) VALUES(0, {})".format(i))

            assert_one(cursor, "SELECT * FROM test WHERE k = 0 LIMIT 1", [0, 0, 42])
            assert_all(cursor, "SELECT * FROM test WHERE k = 0 LIMIT 2", [[0, 0, 42], [0, 1, 42]])
            assert_all(cursor, "SELECT * FROM test WHERE k = 0 LIMIT 3", [[0, 0, 42], [0, 1, 42], [0, 2, 42]])

    @since("2.0")
    def test_static_with_empty_clustering(self):
        """
        @jira_ticket CASSANDRA-7455
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test(
                pkey text,
                ckey text,
                value text,
                static_value text static,
                PRIMARY KEY(pkey, ckey)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(pkey, static_value) VALUES ('partition1', 'static value')")
            cursor.execute("INSERT INTO test(pkey, ckey, value) VALUES('partition1', '', 'value')")

            assert_one(cursor, "SELECT * FROM test", ['partition1', '', 'static value', 'value'])

    @since("1.2")
    def test_limit_compact_table(self):
        """
        @jira_ticket CASSANDRA-7052
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k, v)
            ) WITH COMPACT STORAGE
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            for i in range(0, 4):
                for j in range(0, 4):
                    cursor.execute("INSERT INTO test(k, v) VALUES (%d, %d)" % (i, j))

            assert_all(cursor, "SELECT v FROM test WHERE k=0 AND v > 0 AND v <= 4 LIMIT 2", [[1], [2]])
            assert_all(cursor, "SELECT v FROM test WHERE k=0 AND v > -1 AND v <= 4 LIMIT 2", [[0], [1]])

            assert_all(cursor, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 2", [[0, 1], [0, 2]])
            assert_all(cursor, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > -1 AND v <= 4 LIMIT 2", [[0, 0], [0, 1]])
            assert_all(cursor, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 6", [[0, 1], [0, 2], [0, 3], [1, 1], [1, 2], [1, 3]])

            # This doesn't work -- see #7059
            # assert_all(cursor, "SELECT * FROM test WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING", [[1, 2], [1, 3], [0, 2], [0, 3], [2, 2], [2, 3]])

    def test_key_index_with_reverse_clustering(self):
        """
        @jira_ticket CASSANDRA-6950
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY ((k1, k2), v)
            ) WITH CLUSTERING ORDER BY (v DESC)
        """)

        cursor.execute("CREATE INDEX ON test(k2)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (0, 0, 1)")
            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (0, 1, 2)")
            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (0, 0, 3)")
            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (1, 0, 4)")
            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (1, 1, 5)")
            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (2, 0, 7)")
            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (2, 1, 8)")
            cursor.execute("INSERT INTO test(k1, k2, v) VALUES (3, 0, 1)")

            assert_all(cursor, "SELECT * FROM test WHERE k2 = 0 AND v >= 2 ALLOW FILTERING", [[2, 0, 7], [0, 0, 3], [1, 0, 4]])

    @since('2.1')
    def test_invalid_custom_timestamp(self):
        """
        @jira_ticket CASSANDRA-7067
        """
        cursor = self.prepare()

        # Conditional updates
        cursor.execute("CREATE TABLE test (k int, v int, PRIMARY KEY (k, v))")
        # Counters
        cursor.execute("CREATE TABLE counters (k int PRIMARY KEY, c counter)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")
            cursor.execute("TRUNCATE counters")

            cursor.execute("BEGIN BATCH INSERT INTO test(k, v) VALUES(0, 0) IF NOT EXISTS; INSERT INTO test(k, v) VALUES(0, 1) IF NOT EXISTS; APPLY BATCH")
            assert_invalid(cursor, "BEGIN BATCH INSERT INTO test(k, v) VALUES(0, 2) IF NOT EXISTS USING TIMESTAMP 1; INSERT INTO test(k, v) VALUES(0, 3) IF NOT EXISTS; APPLY BATCH")
            assert_invalid(cursor, "BEGIN BATCH USING TIMESTAMP 1 INSERT INTO test(k, v) VALUES(0, 4) IF NOT EXISTS; INSERT INTO test(k, v) VALUES(0, 1) IF NOT EXISTS; APPLY BATCH")

            cursor.execute("INSERT INTO test(k, v) VALUES(1, 0) IF NOT EXISTS")
            assert_invalid(cursor, "INSERT INTO test(k, v) VALUES(1, 1) IF NOT EXISTS USING TIMESTAMP 5")

            # counters
            cursor.execute("UPDATE counters SET c = c + 1 WHERE k = 0")
            assert_invalid(cursor, "UPDATE counters USING TIMESTAMP 10 SET c = c + 1 WHERE k = 0")

            cursor.execute("BEGIN COUNTER BATCH UPDATE counters SET c = c + 1 WHERE k = 0; UPDATE counters SET c = c + 1 WHERE k = 0; APPLY BATCH")
            assert_invalid(cursor, "BEGIN COUNTER BATCH UPDATE counters USING TIMESTAMP 3 SET c = c + 1 WHERE k = 0; UPDATE counters SET c = c + 1 WHERE k = 0; APPLY BATCH")
            assert_invalid(cursor, "BEGIN COUNTER BATCH USING TIMESTAMP 3 UPDATE counters SET c = c + 1 WHERE k = 0; UPDATE counters SET c = c + 1 WHERE k = 0; APPLY BATCH")

    def test_clustering_order_in(self):
        """
        @jira_ticket CASSANDRA-7105
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                a int,
                b int,
                c int,
                PRIMARY KEY ((a, b), c)
            ) with clustering order by (c desc)
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (a, b, c) VALUES (1, 2, 3)")
            cursor.execute("INSERT INTO test (a, b, c) VALUES (4, 5, 6)")

            assert_one(cursor, "SELECT * FROM test WHERE a=1 AND b=2 AND c IN (3)", [1, 2, 3])
            assert_one(cursor, "SELECT * FROM test WHERE a=1 AND b=2 AND c IN (3, 4)", [1, 2, 3])

    def test_end_of_component_uses_oecBound(self):
        """
        Test that eocBound is always used when deciding which end-of-component to set
        @jira_ticket CASSANDRA-7105
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                a int,
                b int,
                c int,
                d int,
                PRIMARY KEY (a, b)
            )
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (a, b, c, d) VALUES (1, 2, 3, 3)")
            cursor.execute("INSERT INTO test (a, b, c, d) VALUES (1, 4, 6, 5)")

            assert_one(cursor, "SELECT * FROM test WHERE a=1 AND b=2 ORDER BY b DESC", [1, 2, 3, 3])

    def test_SIM_assertion_error(self):
        """
        Test for bogus logic in hasIndexFor when there is more than one searcher and that
        all internal indexes are grouped properly in getIndexSearchersForQuery.
        @jira_ticket CASSANDRA-6612
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE session_data (
                username text,
                session_id text,
                app_name text,
                account text,
                last_access timestamp,
                created_on timestamp,
                PRIMARY KEY (username, session_id, app_name, account)
            );
        """)

        # cursor.execute("create index sessionIndex ON session_data (session_id)")
        cursor.execute("create index sessionAppName ON session_data (app_name)")
        cursor.execute("create index lastAccessIndex ON session_data (last_access)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE session_data")

            assert_one(cursor, "select count(*) from session_data where app_name='foo' and account='bar' and last_access > 4 allow filtering", [0])

            cursor.execute("insert into session_data (username, session_id, app_name, account, last_access, created_on) values ('toto', 'foo', 'foo', 'bar', 12, 13)")

            assert_one(cursor, "select count(*) from session_data where app_name='foo' and account='bar' and last_access > 4 allow filtering", [1])

    def test_blobAs_functions(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            );
        """)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            # A blob that is not 4 bytes should be rejected
            assert_invalid(cursor, "INSERT INTO test(k, v) VALUES (0, blobAsInt(0x01))")

    @pytest.mark.skip("https://issues.apache.org/jira/browse/CASSANDRA-14960")
    def test_invalid_string_literals(self):
        """
        @jira_ticket CASSANDRA-8101
        """
        cursor = self.prepare()
        cursor.execute("create table invalid_string_literals (k int primary key, a ascii, b text)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE invalid_string_literals")

            assert_invalid(cursor, "insert into ks.invalid_string_literals (k, a) VALUES (0, '\u038E\u0394\u03B4\u03E0')")
            # since the protocol requires strings to be valid UTF-8, the error response to this is a ProtocolError
            try:
                cursor.execute("insert into ks.invalid_string_literals (k, b) VALUES (0, '\xc2\x01')")
                pytest.fail("Expected error")
            except ProtocolException as e:
                assert "Cannot decode string as UTF8" in str(e)

    def test_negative_timestamp(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES (1, 1) USING TIMESTAMP -42")

            assert_one(cursor, "SELECT writetime(v) FROM TEST WHERE k = 1", [-42])

    @since('2.2')
    @pytest.mark.skip(reason='awaiting CASSANDRA-7396')
    def test_select_map_key_single_row(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v map<int, text>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES ( 0, {1:'a', 2:'b', 3:'c', 4:'d'})")

            assert_one(cursor, "SELECT v[1] FROM test WHERE k = 0", ['a'])
            assert_one(cursor, "SELECT v[5] FROM test WHERE k = 0", [])
            assert_one(cursor, "SELECT v[1] FROM test WHERE k = 1", [])

            assert_one(cursor, "SELECT v[1..3] FROM test WHERE k = 0", ['a', 'b', 'c'])
            assert_one(cursor, "SELECT v[3..5] FROM test WHERE k = 0", ['c', 'd'])
            assert_invalid(cursor, "SELECT v[3..1] FROM test WHERE k = 0")

            assert_one(cursor, "SELECT v[..2] FROM test WHERE k = 0", ['a', 'b'])
            assert_one(cursor, "SELECT v[3..] FROM test WHERE k = 0", ['c', 'd'])
            assert_one(cursor, "SELECT v[0..] FROM test WHERE k = 0", ['a', 'b', 'c', 'd'])
            assert_one(cursor, "SELECT v[..5] FROM test WHERE k = 0", ['a', 'b', 'c', 'd'])

            assert_one(cursor, "SELECT sizeof(v) FROM test where k = 0", [4])

    @since('2.2')
    @pytest.mark.skip(reason='awaiting CASSANDRA-7396')
    def test_select_set_key_single_row(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v set<text>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES ( 0, {'e', 'a', 'd', 'b'})")

            assert_one(cursor, "SELECT v FROM test WHERE k = 0", [sortedset(['a', 'b', 'd', 'e'])])
            assert_one(cursor, "SELECT v['a'] FROM test WHERE k = 0", [True])
            assert_one(cursor, "SELECT v['c'] FROM test WHERE k = 0", [False])
            assert_one(cursor, "SELECT v['a'] FROM test WHERE k = 1", [])

            assert_one(cursor, "SELECT v['b'..'d'] FROM test WHERE k = 0", ['b', 'd'])
            assert_one(cursor, "SELECT v['b'..'e'] FROM test WHERE k = 0", ['b', 'd', 'e'])
            assert_one(cursor, "SELECT v['a'..'d'] FROM test WHERE k = 0", ['a', 'b', 'd'])
            assert_one(cursor, "SELECT v['b'..'f'] FROM test WHERE k = 0", ['b', 'd', 'e'])
            assert_invalid(cursor, "SELECT v['d'..'a'] FROM test WHERE k = 0")

            assert_one(cursor, "SELECT v['d'..] FROM test WHERE k = 0", ['d', 'e'])
            assert_one(cursor, "SELECT v[..'d'] FROM test WHERE k = 0", ['a', 'b', 'd'])
            assert_one(cursor, "SELECT v['f'..] FROM test WHERE k = 0", [])
            assert_one(cursor, "SELECT v[..'f'] FROM test WHERE k = 0", ['a', 'b', 'd', 'e'])

            assert_one(cursor, "SELECT sizeof(v) FROM test where k = 0", [4])

    @since('2.2')
    @pytest.mark.skip(reason='awaiting CASSANDRA-7396')
    def test_select_list_key_single_row(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v list<text>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES ( 0, ['e', 'a', 'd', 'b'])")

            assert_one(cursor, "SELECT v FROM test WHERE k = 0", [['e', 'a', 'd', 'b']])
            assert_one(cursor, "SELECT v[0] FROM test WHERE k = 0", ['e'])
            assert_one(cursor, "SELECT v[3] FROM test WHERE k = 0", ['b'])
            assert_one(cursor, "SELECT v[0] FROM test WHERE k = 1", [])

            assert_invalid(cursor, "SELECT v[-1] FROM test WHERE k = 0")
            assert_invalid(cursor, "SELECT v[5] FROM test WHERE k = 0")

            assert_one(cursor, "SELECT v[1..3] FROM test WHERE k = 0", ['a', 'd', 'b'])
            assert_one(cursor, "SELECT v[0..2] FROM test WHERE k = 0", ['e', 'a', 'd'])
            assert_invalid(cursor, "SELECT v[0..4] FROM test WHERE k = 0")
            assert_invalid(cursor, "SELECT v[2..0] FROM test WHERE k = 0")

            assert_one(cursor, "SELECT sizeof(v) FROM test where k = 0", [4])

    @since('2.2')
    @pytest.mark.skip(reason='awaiting CASSANDRA-7396')
    def test_select_map_key_multi_row(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v map<int, text>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES ( 0, {1:'a', 2:'b', 3:'c', 4:'d'})")
            cursor.execute("INSERT INTO test (k, v) VALUES ( 1, {1:'a', 2:'b', 5:'e', 6:'f'})")

            assert_all(cursor, "SELECT v[1] FROM test", [['a'], ['a']])
            assert_all(cursor, "SELECT v[5] FROM test", [[], ['e']])
            assert_all(cursor, "SELECT v[4] FROM test", [['d'], []])

            assert_all(cursor, "SELECT v[1..3] FROM test", [['a', 'b', 'c'], ['a', 'b', 'e']])
            assert_all(cursor, "SELECT v[3..5] FROM test", [['c', 'd'], ['e']])
            assert_invalid(cursor, "SELECT v[3..1] FROM test")

            assert_all(cursor, "SELECT v[..2] FROM test", [['a', 'b'], ['a', 'b']])
            assert_all(cursor, "SELECT v[3..] FROM test", [['c', 'd'], ['e', 'f']])
            assert_all(cursor, "SELECT v[0..] FROM test", [['a', 'b', 'c', 'd'], ['a', 'b', 'e', 'f']])
            assert_all(cursor, "SELECT v[..5] FROM test", [['a', 'b', 'c', 'd'], ['a', 'b', 'e']])

            assert_all(cursor, "SELECT sizeof(v) FROM test", [[4], [4]])

    @since('2.2')
    @pytest.mark.skip(reason='awaiting CASSANDRA-7396')
    def test_select_set_key_multi_row(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v set<text>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES ( 0, {'e', 'a', 'd', 'b'})")
            cursor.execute("INSERT INTO test (k, v) VALUES ( 1, {'c', 'f', 'd', 'b'})")

            assert_all(cursor, "SELECT v FROM test", [[sortedset(['b', 'c', 'd', 'f'])], [sortedset(['a', 'b', 'd', 'e'])]])
            assert_all(cursor, "SELECT v['a'] FROM test", [[True], [False]])
            assert_all(cursor, "SELECT v['c'] FROM test", [[False], [True]])

            assert_all(cursor, "SELECT v['b'..'d'] FROM test", [['b', 'd'], ['b', 'c', 'd']])
            assert_all(cursor, "SELECT v['b'..'e'] FROM test", [['b', 'd', 'e'], ['b', 'c', 'd']])
            assert_all(cursor, "SELECT v['a'..'d'] FROM test", [['a', 'b', 'd'], ['b', 'c', 'd']])
            assert_all(cursor, "SELECT v['b'..'f'] FROM test", [['b', 'd', 'e'], ['b', 'c', 'd', 'f']])
            assert_invalid(cursor, "SELECT v['d'..'a'] FROM test")

            assert_all(cursor, "SELECT v['d'..] FROM test", [['d', 'e'], ['d', 'f']])
            assert_all(cursor, "SELECT v[..'d'] FROM test", [['a', 'b', 'd'], ['b', 'c', 'd']])
            assert_all(cursor, "SELECT v['f'..] FROM test", [[], ['f']])
            assert_all(cursor, "SELECT v[..'f'] FROM test", [['a', 'b', 'd', 'e'], ['b', 'c', 'd', 'f']])

            assert_all(cursor, "SELECT sizeof(v) FROM test", [[4], [4]])

    @since('2.2')
    @pytest.mark.skip(reason='awaiting CASSANDRA-7396')
    def test_select_list_key_multi_row(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v list<text>)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE test")

            cursor.execute("INSERT INTO test (k, v) VALUES ( 0, ['e', 'a', 'd', 'b'])")
            cursor.execute("INSERT INTO test (k, v) VALUES ( 1, ['c', 'f', 'd', 'b'])")

            assert_all(cursor, "SELECT v FROM test", [[['c', 'f', 'd', 'b']], [['e', 'a', 'd', 'b']]])
            assert_all(cursor, "SELECT v[0] FROM test", [['e'], ['c']])
            assert_all(cursor, "SELECT v[3] FROM test", [['b'], ['b']])
            assert_invalid(cursor, "SELECT v[-1] FROM test")
            assert_invalid(cursor, "SELECT v[5] FROM test")

            assert_all(cursor, "SELECT v[1..3] FROM test", [['a', 'd', 'b'], ['f', 'd', 'b']])
            assert_all(cursor, "SELECT v[0..2] FROM test", [['e', 'a', 'd'], ['c', 'f', 'd']])
            assert_invalid(cursor, "SELECT v[0..4] FROM test")
            assert_invalid(cursor, "SELECT v[2..0] FROM test")

            assert_all(cursor, "SELECT sizeof(v) FROM test", [[4], [4]])

    def test_deleted_row_select(self):
        """
        Test to make sure deleted rows cannot still be selected out.
        @jira_ticket CASSANDRA-8558
        """
        cursor = self.prepare()
        node1 = self.cluster.nodelist()[0]

        cursor.execute("CREATE  KEYSPACE space1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        cursor.execute("CREATE  TABLE space1.table1(a int, b int, c text,primary key(a,b))")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            cursor.execute("TRUNCATE space1.table1")

            cursor.execute("INSERT INTO space1.table1(a,b,c) VALUES(1,1,'1')")
            node1.nodetool('flush')
            cursor.execute("DELETE FROM space1.table1 where a=1 and b=1")
            node1.nodetool('flush')

            assert_none(cursor, "select * from space1.table1 where a=1 and b=1")

    @pytest.mark.skip("https://issues.apache.org/jira/browse/CASSANDRA-14961")
    def test_secondary_index_query(self):
        """
        Test for fix to bug where secondary index cannot be queried due to Column Family caching changes.
        @jira_ticket CASSANDRA-5732
        """
        cursor = self.prepare(use_cache=True)

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int,
            )
        """)

        if self.node_version_above('2.1'):
            cursor.execute("ALTER TABLE test WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}")
        else:
            cursor.execute("ALTER TABLE test WITH CACHING='ALL'")
        cursor.execute("INSERT INTO test (k,v) VALUES (0,0)")
        cursor.execute("INSERT INTO test (k,v) VALUES (1,1)")
        cursor.execute("CREATE INDEX testindex on test(v)")

        # wait for the index to be fully built
        check_for_index_sessions = tuple(self.patient_exclusive_cql_connection(node) for node in self.cluster.nodelist())
        index_query = (
            """SELECT * FROM system_schema.indexes WHERE keyspace_name = 'ks' AND table_name = 'test' AND index_name = 'testindex'"""
            if self.node_version_above('3.0') else
            """SELECT * FROM system."IndexInfo" WHERE table_name = 'ks' AND index_name = 'test.testindex'"""
        )
        start = time.time()
        while True:
            results = [list(session.execute(index_query)) for session in check_for_index_sessions]
            logger.debug(results)
            if all(results):
                break

            if time.time() - start > 10.0:
                failure_info_query = (
                    'SELECT * FROM system_schema.indexes'
                    if self.node_version_above('3.0') else
                    'SELECT * FROM system."IndexInfo"'
                )
                raise Exception("Failed to build secondary index within ten seconds: %s" % (list(cursor.execute(failure_info_query))))
            time.sleep(0.1)

        assert_all(cursor, "SELECT k FROM test WHERE v = 0", [[0]])

        self.cluster.stop()
        time.sleep(0.5)
        self.cluster.start()
        time.sleep(0.5)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            assert_all(cursor, "SELECT k FROM ks.test WHERE v = 0", [[0]])

    def test_tracing_prevents_startup_after_upgrading(self):
        """
        Test that after upgrading from 2.1 to 3.0, the system_traces.sessions table is properly upgraded to include
        the client column.
        @jira_ticket CASSANDRA-10652
        """
        cursor = self.prepare()

        cursor.execute("CREATE KEYSPACE foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        cursor.execute("CREATE TABLE foo.bar (k int PRIMARY KEY, v int)")

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))

            future = cursor.execute_async("INSERT INTO foo.bar(k, v) VALUES (0, 0)", trace=True)
            future.result()
            future.get_query_trace(max_wait=120)

            self.cluster.flush()

            assert_one(cursor, "SELECT * FROM foo.bar", [0, 0])

    @since('3.0')
    def test_materialized_view_simple(self):
        """
        Test that creates and populate a simple materialized view.
        @jira_ticket CASSANDRA-13382
        """
        cursor = self.prepare(extra_config_options={'enable_materialized_views': 'true'})

        cursor.execute("CREATE KEYSPACE foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        cursor.execute("CREATE TABLE foo.test1 (k int, t int, v int, PRIMARY KEY(k, t))")

        if self.is_40_or_greater():
            cursor.execute("""
                CREATE MATERIALIZED VIEW foo.view1
                AS SELECT * FROM foo.test1
                WHERE k IS NOT NULL AND v IS NOT NULL AND t IS NOT NULL
                PRIMARY KEY (k, v, t)
            """)
        else:
            cursor.execute("""
                CREATE MATERIALIZED VIEW foo.view1
                AS SELECT * FROM foo.test1
                WHERE v IS NOT NULL AND t IS NOT NULL
                PRIMARY KEY (k, v, t)
            """)

        for i in range(0, 10):
            cursor.execute("INSERT INTO foo.test1(k, t, v) VALUES (0, %d, %d)" % (i, 10 - i - 1))

        for is_upgraded, cursor in self.do_upgrade(cursor):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            assert_all(cursor, "SELECT v, t FROM foo.view1 WHERE k = 0", [[i, 10 - i - 1] for i in range(0, 10)])


topology_specs = [
    {'NODES': 3,
     'RF': 3,
     'CL': ConsistencyLevel.ALL},
    {'NODES': 2,
     'RF': 1},
]
specs = [dict(s, UPGRADE_PATH=p, __test__=True)
         for s, p in itertools.product(topology_specs, build_upgrade_pairs())]

for spec in specs:
    suffix = 'Nodes{num_nodes}RF{rf}_{pathname}'.format(num_nodes=spec['NODES'],
                                                        rf=spec['RF'],
                                                        pathname=spec['UPGRADE_PATH'].name)
    gen_class_name = TestCQL.__name__ + suffix
    assert gen_class_name not in globals()

    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or spec['UPGRADE_PATH'].upgrade_meta.matches_current_env_version_family
    cls = type(gen_class_name, (TestCQL,), spec)
    if not upgrade_applies_to_env:
        add_skip(cls, 'test not applicable to env.')
    globals()[gen_class_name] = cls
