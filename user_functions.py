# coding: utf-8

import math
import time

from dtest import Tester
from pyassertions import assert_invalid
from pytools import since

cql_version = "3.0.0"


class TestUserFunctions(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.patient_cql_connection(node1, version=cql_version)
        if create_keyspace:
            self.create_ks(cursor, 'ks', rf)
        return cursor

    @since('3.0')
    def test_migration(self):
        """ Test migration of user functions """
        cluster = self.cluster

        # Uses 3 nodes just to make sure function mutations are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]
        time.sleep(0.2)

        cursor1 = self.patient_cql_connection(node1, version=cql_version)
        cursor2 = self.patient_cql_connection(node2, version=cql_version)
        cursor3 = self.patient_cql_connection(node3, version=cql_version)
        self.create_ks(cursor1, 'ks', 1)

        cursor1.execute("""
            CREATE TABLE udf_kv (
                key    int primary key,
                value  double
            );
        """)
        time.sleep(1)

        cursor1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (1, 1))
        cursor1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (2, 2))
        cursor1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (3, 3))

        cursor1.execute("create or replace function x_sin ( input double ) returns double language java as 'if (input==null) return null; return Double.valueOf(Math.sin(input.doubleValue()));'")
        cursor2.execute("create or replace function x_cos ( input double ) returns double language java as 'if (input==null) return null; return Double.valueOf(Math.cos(input.doubleValue()));'")
        cursor3.execute("create or replace function x_tan ( input double ) returns double language java as 'if (input==null) return null; return Double.valueOf(Math.tan(input.doubleValue()));'")

        time.sleep(1)

        res = cursor1.execute("SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 1)
        assert res == [[1, 1.0, 0.8414709848078965, 0.5403023058681398, 1.5574077246549023]], res

        res = cursor2.execute("SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 2)
        assert res == [[2, 2.0, math.sin(2.0), math.cos(2.0), math.tan(2.0)]], res

        res = cursor3.execute("SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 3)
        assert res == [[3, 3.0, math.sin(3.0), math.cos(3.0), math.tan(3.0)]], res

        cursor2.execute("drop function x_sin")
        cursor3.execute("drop function x_cos")
        cursor1.execute("drop function x_tan")

        assert_invalid(cursor1, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")
        assert_invalid(cursor2, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")
        assert_invalid(cursor3, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")
