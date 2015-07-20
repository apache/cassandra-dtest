import math
import time

from dtest import Tester
from assertions import assert_invalid, assert_one, assert_none
from tools import since


@since('2.2')
class TestUserFunctions(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'enable_user_defined_functions': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def prepare(self, create_keyspace=True, nodes=1, rf=1):
        cluster = self.cluster

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            self.create_ks(session, 'ks', rf)
        return session

    def test_migration(self):
        """ Test migration of user functions """
        cluster = self.cluster

        # Uses 3 nodes just to make sure function mutations are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]
        time.sleep(0.2)

        session1 = self.patient_exclusive_cql_connection(node1)
        session2 = self.patient_exclusive_cql_connection(node2)
        session3 = self.patient_exclusive_cql_connection(node3)
        self.create_ks(session1, 'ks', 1)
        session2.execute("use ks")
        session3.execute("use ks")

        session1.execute("""
            CREATE TABLE udf_kv (
                key    int primary key,
                value  double
            );
        """)
        time.sleep(1)

        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (1, 1))
        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (2, 2))
        session1.execute("INSERT INTO udf_kv (key, value) VALUES (%d, %d)" % (3, 3))

        session1.execute("""
            create or replace function x_sin ( input double ) called on null input
            returns double language java as 'if (input==null) return null;
            return Double.valueOf(Math.sin(input.doubleValue()));'
            """)
        session2.execute(""""
            create or replace function x_cos ( input double ) called on null input
            returns double language java as 'if (input==null) return null;
            return Double.valueOf(Math.cos(input.doubleValue()));'
            """)
        session3.execute(""""
            create or replace function x_tan ( input double ) called on null input
            returns double language java as 'if (input==null) return null;
            return Double.valueOf(Math.tan(input.doubleValue()));'
            """)

        time.sleep(1)

        assert_one(session1, "SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 1, [1, 1.0, 0.8414709848078965, 0.5403023058681398, 1.5574077246549023])

        assert_one(session2, "SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 2, [2, 2.0, math.sin(2.0), math.cos(2.0), math.tan(2.0)])

        assert_one(session3, "SELECT key, value, x_sin(value), x_cos(value), x_tan(value) FROM ks.udf_kv where key = %d" % 3, [3, 3.0, math.sin(3.0), math.cos(3.0), math.tan(3.0)])

        session4 = self.patient_cql_connection(node1)

        # check that functions are correctly confined to namespaces
        assert_invalid(session4, "SELECT key, value, sin(value), cos(value), tan(value) FROM ks.udf_kv where key = 4", "Unknown function 'sin'")

        # try giving existing function bad input, should error
        assert_invalid(session1,
                       "SELECT key, value, x_sin(key), foo_cos(KEYy), foo_tan(key) FROM ks.udf_kv where key = 1", "Type error: key cannot be passed as argument 0 of function ks.x_sin of type double")

        session2.execute("drop function x_sin")
        session3.execute("drop function x_cos")
        session1.execute("drop function x_tan")

        assert_invalid(session1, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")
        assert_invalid(session2, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")
        assert_invalid(session3, "SELECT key, value, sin(value), cos(value), tan(value) FROM udf_kv where key = 1")

        # try creating function returning the wrong type, should error
        assert_invalid(session1, """
                      CREATE FUNCTION bad_sin ( input double ) CALLED ON NULL INPUT RETURNS uuid LANGUAGE java AS 'return Math.sin(input);'",
                      "Could not compile function 'ks.bad_sin' from Java source:
                      """)

    def udf_overload_test(self):

        session = self.prepare(nodes=3)

        session.execute("CREATE TABLE tab (k text PRIMARY KEY, v int)")
        session.execute("INSERT INTO tab (k, v) VALUES ('foo' , 1);")

        # create overloaded udfs
        session.execute("CREATE FUNCTION overloaded(v varchar) called on null input RETURNS text LANGUAGE java AS 'return \"f1\";'")
        session.execute("CREATE OR REPLACE FUNCTION overloaded(i int) called on null input RETURNS text LANGUAGE java AS 'return \"f2\";'")
        session.execute("CREATE OR REPLACE FUNCTION overloaded(v1 text, v2 text) called on null input RETURNS text LANGUAGE java AS 'return \"f3\";'")
        session.execute("CREATE OR REPLACE FUNCTION overloaded(v ascii) called on null input RETURNS text LANGUAGE java AS 'return \"f1\";'")

        # ensure that works with correct specificity
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded('foo')")
        assert_none(session, "SELECT v FROM tab WHERE k = overloaded((text) 'foo')")
        assert_none(session, "SELECT v FROM tab WHERE k = overloaded((ascii) 'foo')")
        assert_none(session, "SELECT v FROM tab WHERE k = overloaded((varchar) 'foo')")

        # try non-existent functions
        assert_invalid(session, "DROP FUNCTION overloaded(boolean)")
        assert_invalid(session, "DROP FUNCTION overloaded(bigint)")

        # try dropping overloaded - should fail because ambiguous
        assert_invalid(session, "DROP FUNCTION overloaded")
        session.execute("DROP FUNCTION overloaded(varchar)")
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded((text)'foo')")
        session.execute("DROP FUNCTION overloaded(text, text)")
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded((text)'foo',(text)'bar')")
        session.execute("DROP FUNCTION overloaded(ascii)")
        assert_invalid(session, "SELECT v FROM tab WHERE k = overloaded((ascii)'foo')")
        # should now work - unambiguous
        session.execute("DROP FUNCTION overloaded")

    def udf_scripting_test(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val double);")

        for x in range(1, 4):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, float(x)))

        session.execute("CREATE FUNCTION x_sin(val double) called on null input returns double language javascript as 'Math.sin(val)'")

        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 1, [1, 1.0, math.sin(1.0)])
        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 2, [2, 2.0, math.sin(2.0)])
        assert_one(session, "SELECT key, val, x_sin(val) FROM nums where key = %d" % 3, [3, 3.0, math.sin(3.0)])

        session.execute("create function y_sin(val double) called on null input returns double language javascript as 'Math.sin(val).toString()'")

        assert_invalid(session, "select y_sin(val) from nums where key = 1")

        assert_invalid(session, "create function compilefail(key int) called on null input returns double language javascript as 'foo bar';")

        session.execute("create function plustwo(key int) called on null input returns double language javascript as 'key+2'")

        assert_one(session, "select plustwo(key) from nums where key = 3", [5])

    def default_aggregate_test(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val double);")

        for x in range(1, 10):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, float(x)))

        assert_one(session, "SELECT min(key) FROM nums", [1])
        assert_one(session, "SELECT max(val) FROM nums", [9.0])
        assert_one(session, "SELECT sum(key) FROM nums", [45])
        assert_one(session, "SELECT avg(val) FROM nums", [5.0])
        assert_one(session, "SELECT count(*) FROM nums", [9])

    def aggregate_udf_test(self):
        session = self.prepare()
        session.execute("create table nums (key int primary key, val int);")

        for x in range(1, 4):
            session.execute("INSERT INTO nums (key, val) VALUES (%d, %d)" % (x, x))
        session.execute("create function plus(key int, val int) called on null input returns int language java as 'return Integer.valueOf(key.intValue() + val.intValue());'")
        session.execute("create function stri(key int) called on null input returns text language java as 'return key.toString();'")
        session.execute("create aggregate suma (int) sfunc plus stype int finalfunc stri initcond 10")

        assert_one(session, "select suma(val) from nums", ["16"])

        session.execute("create function test(a int, b double) called on null input returns int language javascript as 'a + b;'")
        session.execute("create aggregate aggy(double) sfunc test stype int")

        assert_invalid(session, "create aggregate aggtwo(int) sfunc aggy stype int")

        assert_invalid(session, "create aggregate aggthree(int) sfunc test stype int finalfunc aggtwo")

    def udf_with_udt_test(self):
        session = self.prepare()

        session.execute("create type test (a text, b int);")

        assert_invalid(session, "create table tab (key int primary key, udt test);")

        session.execute("create table tab (key int primary key, udt frozen<test>);")

        session.execute("insert into tab (key, udt) values (1, {a: 'un', b:1});")
        session.execute("insert into tab (key, udt) values (2, {a: 'deux', b:2});")
        session.execute("insert into tab (key, udt) values (3, {a: 'trois', b:3});")

        session.execute("create function funk(udt test) called on null input returns int language java as 'return Integer.valueOf(udt.getInt(\"b\"));';")

        assert_one(session, "select sum(funk(udt)) from tab", [6])

        assert_invalid(session, "drop type test;")

    @since('2.2')
    def udf_with_udt_keyspace_isolation_test(self):
        """
        Ensure functions dont allow a UDT from another keyspace
        @jira_ticket CASSANDRA-9409
        @since 2.2
        """
        session = self.prepare()

        session.execute("create type udt (a text, b int);")
        self.create_ks(session, 'user_ks', 1)

        # ensure we cannot use a udt from another keyspace as function argument
        assert_invalid(
            session,
            "CREATE FUNCTION overloaded(v ks.udt) called on null input RETURNS text LANGUAGE java AS 'return \"f1\";'",
            "Statement on keyspace user_ks cannot refer to a user type in keyspace ks"
        )

        # ensure we cannot use a udt from another keyspace as return value
        assert_invalid(
            session,
            ("CREATE FUNCTION test(v text) called on null input RETURNS ks.udt "
             "LANGUAGE java AS 'return null;';"),
            "Statement on keyspace user_ks cannot refer to a user type in keyspace ks"
        )

    def aggregate_with_udt_keyspace_isolation_test(self):
        """
        Ensure aggregates dont allow a UDT from another keyspace
        @jira_ticket CASSANDRA-9409
        """
        session = self.prepare()

        session.execute("create type udt (a int);")
        self.create_ks(session, 'user_ks', 1)
        assert_invalid(
            session,
            "create aggregate suma (ks.udt) sfunc plus stype int finalfunc stri initcond 10",
            "Statement on keyspace user_ks cannot refer to a user type in keyspace ks"
        )
