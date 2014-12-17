import math, os, sys, time

from dtest import Tester, debug
from pyassertions import assert_invalid, assert_one, assert_all, assert_none
from pytools import since, rows_to_list


class TestUserFunctions(Tester):

    def prepare(self, create_keyspace=True, nodes=1, rf=1):
        cluster = self.cluster

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.patient_cql_connection(node1)
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

        cursor1 = self.patient_exclusive_cql_connection(node1)
        cursor2 = self.patient_exclusive_cql_connection(node2)
        cursor3 = self.patient_exclusive_cql_connection(node3)
        self.create_ks(cursor1, 'ks', 1)
        cursor2.execute("use ks")
        cursor3.execute("use ks")

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

        assert_one(cursor1, "SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 1, [1, 1.0, 0.8414709848078965, 0.5403023058681398, 1.5574077246549023])

        assert_one(cursor2, "SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 2, [2, 2.0, math.sin(2.0), math.cos(2.0), math.tan(2.0)])

        assert_one(cursor3, "SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 3, [3, 3.0, math.sin(3.0), math.cos(3.0), math.tan(3.0)])

        cursor4 = self.patient_cql_connection(node1)

        #check that functions are correctly confined to namespaces
        assert_invalid(cursor4, "SELECT key, value, sin(value), cos(value), tan(value) FROM ks.udf_kv where key = 4", "Unknown function 'sin'")

        #try giving existing function bad input, should error
        assert_invalid(cursor1, "SELECT key, value, x_sin(key), foo_cos(KEYy), foo_tan(key) FROM ks.udf_kv where key = 1", "Type error: key cannot be passed as argument 0 of function ks.x_sin of type double")

        cursor2.execute("drop function x_sin")
        cursor3.execute("drop function x_cos")
        cursor1.execute("drop function x_tan")

        assert_invalid(cursor1, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")
        assert_invalid(cursor2, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")
        assert_invalid(cursor3, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")

        #try creating function returning the wrong type, should error
        assert_invalid(cursor1, "CREATE FUNCTION bad_sin ( input double ) RETURNS double LANGUAGE java AS 'return Math.sin(input.doubleValue());'", "Could not compile function 'ks.bad_sin' from Java source:")

    @since('3.0')
    def udf_overload_test(self):

        session = self.prepare(nodes=3)

        session.execute("CREATE TABLE tab (k text PRIMARY KEY, v int)");
        test = "foo"
        session.execute("INSERT INTO tab (k, v) VALUES ('foo' , 1);")

        # create overloaded udfs
        session.execute("CREATE FUNCTION overloaded(v varchar) RETURNS text LANGUAGE java AS 'return \"f1\";'");
        session.execute("CREATE OR REPLACE FUNCTION overloaded(i int) RETURNS text LANGUAGE java AS 'return \"f2\";'");
        session.execute("CREATE OR REPLACE FUNCTION overloaded(v1 text, v2 text) RETURNS text LANGUAGE java AS 'return \"f3\";'");
        session.execute("CREATE OR REPLACE FUNCTION overloaded(v ascii) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        #ensure that works with correct specificity
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded('foo')");
        assert_none(session, "SELECT v FROM tab WHERE k = overloaded((text) 'foo')");
        assert_none(session, "SELECT v FROM tab WHERE k = overloaded((ascii) 'foo')");
        assert_none(session, "SELECT v FROM tab WHERE k = overloaded((varchar) 'foo')");

        #try non-existent functions
        assert_invalid(session, "DROP FUNCTION overloaded(boolean)");
        assert_invalid(session, "DROP FUNCTION overloaded(bigint)");

        #try dropping overloaded - should fail because ambiguous
        assert_invalid(session, "DROP FUNCTION overloaded");
        session.execute("DROP FUNCTION overloaded(varchar)");
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded((text)'foo')");
        session.execute("DROP FUNCTION overloaded(text, text)");
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded((text)'foo',(text)'bar')");
        session.execute("DROP FUNCTION overloaded(ascii)");
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded((ascii)'foo')");
        #should now work - unambiguous
        session.execute("DROP FUNCTION overloaded");
