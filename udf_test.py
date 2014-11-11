from dtest import Tester, debug
from pyassertions import assert_invalid, assert_all, assert_none
from pytools import since, rows_to_list
import math
import os, sys, time
from ccmlib.cluster import Cluster

class TestUDF(Tester):

    @since('3.0')
    def udf_test(self):
        """ Test User Defined Functions """
        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()
        cqlversion = "3.0.0"
        cursor1 = self.patient_cql_connection(node1, version=cqlversion)

        self.create_ks(cursor1, 'ks', 1)

        cursor1.execute("""
            CREATE TABLE udf_kv (
                key int primary key,
                value double
            );
        """)

        #some test values
        cursor1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (1, 1))
        cursor1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (2, 2))
        cursor1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (3, 3))

        #insert basic math udfs
        cursor1.execute("CREATE FUNCTION foo::sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'")
        cursor1.execute("CREATE FUNCTION foo::cos ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.cos(input.doubleValue()));'")
        cursor1.execute("CREATE FUNCTION foo::tan ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.tan(input.doubleValue()));'")

        time.sleep(1)

        #check that functions works by returning correct value
        assert_all(cursor1,"SELECT key, value, foo::sin(value), foo::cos(value), foo::tan(value) FROM ks.udf_kv where key = 1", [[ 1, 1.0, math.sin(1.0), math.cos(1.0), math.tan(1.0) ]])
        assert_all(cursor1,"SELECT key, value, foo::sin(value), foo::cos(value), foo::tan(value) FROM ks.udf_kv where key = 2", [[ 2, 2.0, math.sin(2.0), math.cos(2.0), math.tan(2.0) ]])
        assert_all(cursor1,"SELECT key, value, foo::sin(value), foo::cos(value), foo::tan(value) FROM ks.udf_kv where key = 3", [[ 3, 3.0, math.sin(3.0), math.cos(3.0), math.tan(3.0) ]])

        #check that functions are correctly confined to namespaces
        assert_invalid(cursor1, "SELECT key, value, sin(value), cos(value), tan(value) FROM ks.udf_kv where key = 4", "Unknown function 'sin'")
        
        #try creating function returning the wrong type, should error
        assert_invalid(cursor1, "CREATE FUNCTION bad::sin ( input double ) RETURNS double LANGUAGE java AS 'return Math.sin(input.doubleValue());'", "Could not compile function 'bad::sin' from Java source:")

        #try giving existing function bad input, should error
        assert_invalid(cursor1, "SELECT key, value, foo::sin(key), foo::cos(KEYy), foo::tan(key) FROM ks.udf_kv where key = 1", "Type error: key cannot be passed as argument 0 of function foo::sin of type double")

    @since('3.0')
    def udf_overload_test(self):

        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()
        cqlversion = "3.0.0"
        cursor1 = self.patient_cql_connection(node1, version=cqlversion)

        self.create_ks(cursor1, 'ks', 1)

        cursor1.execute("CREATE TABLE tab (k text PRIMARY KEY, v int)");
        test = "foo"
        cursor1.execute("INSERT INTO tab (k, v) VALUES ('foo' , 1);")

        # create overloaded udfs
        cursor1.execute("CREATE FUNCTION overloaded(v varchar) RETURNS text LANGUAGE java AS 'return \"f1\";'");
        cursor1.execute("CREATE OR REPLACE FUNCTION overloaded(i int) RETURNS text LANGUAGE java AS 'return \"f2\";'");
        cursor1.execute("CREATE OR REPLACE FUNCTION overloaded(v1 text, v2 text) RETURNS text LANGUAGE java AS 'return \"f3\";'");
        cursor1.execute("CREATE OR REPLACE FUNCTION overloaded(v ascii) RETURNS text LANGUAGE java AS 'return \"f1\";'");

        #ensure that works with correct specificity
        assert_invalid(cursor1, "SELECT v FROM tab WHERE k = overloaded('foo')");
        assert_none(cursor1, "SELECT v FROM tab WHERE k = overloaded((text) 'foo')");
        assert_none(cursor1, "SELECT v FROM tab WHERE k = overloaded((ascii) 'foo')");
        assert_none(cursor1, "SELECT v FROM tab WHERE k = overloaded((varchar) 'foo')");

        #try non-existent functions
        assert_invalid(cursor1, "DROP FUNCTION overloaded(boolean)");
        assert_invalid(cursor1, "DROP FUNCTION overloaded(bigint)");

        #try dropping overloaded - should fail because ambiguous
        assert_invalid(cursor1, "DROP FUNCTION overloaded");
        cursor1.execute("DROP FUNCTION overloaded(varchar)");
        assert_invalid(cursor1, "SELECT v FROM tab WHERE k = overloaded((text)'foo')");
        cursor1.execute("DROP FUNCTION overloaded(text, text)");
        assert_invalid(cursor1, "SELECT v FROM tab WHERE k = overloaded((text)'foo',(text)'bar')");
        cursor1.execute("DROP FUNCTION overloaded(ascii)");
        assert_invalid(cursor1, "SELECT v FROM tab WHERE k = overloaded((ascii)'foo')");
        #should now work - unambiguous
        cursor1.execute("DROP FUNCTION overloaded");
