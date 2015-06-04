import doctest
import inspect
import os
import re
import subprocess
import sys
from distutils.version import LooseVersion
from ccmlib import common
from ccmlib.common import is_win
from dtest import Tester
from tools import since


def build_doc_context(tester, test_name, prepare=True, connection=None, nodes=None):
    """
    Takes an instance of dtest.Tester (or a subclass), completes some basic setup, and returns a
    dict of "globs" to be used as a doctest execution context.

    This provides a doctest environment to test cql via python driver, or cqlsh directly.
    Cqlsh commands made via the cqlsh_* commands are sent (almost) exactly as provided, but a keyspace will be injected before commands for convenience.

    If you prefer to customize setup, pass prepare=False and provide your own cql connection object and a list of ccm nodes.
    When nodes are provided, the first node will be used for connections and cqlsh.
    """
    default_ks_name = test_name

    if prepare:
        if connection or nodes:
            raise RuntimeError("Cannot auto prepare doctest context when connection or nodes are provided.")

        tester.cluster.populate(1).start()
        nodes = tester.cluster.nodelist()
        connection = tester.patient_cql_connection(nodes[0])
        connection.execute("CREATE KEYSPACE {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};".format(default_ks_name))
        connection.execute("USE {}".format(default_ks_name))
    else:
        if not (connection and nodes):
            raise RuntimeError("Cannot build doctest context without connection and nodes.")

    def ks(new_ks):
        """
        Change active keyspace
        Makes cql request via python driver, and also hangs onto the ks_name
        in case we are using cqlsh, since cqlsh is closed and will forget between requests.
        """
        # if we are using regular cql this should stick
        connection.execute("USE {};".format(new_ks))

        # sneaky attribute on this func itself in case we are using cqlsh
        # and need to inject a "USE" statement right before running a cqlsh query.
        ks.current_ks = new_ks

    def enabled_ks():
        return getattr(ks, 'current_ks', default_ks_name)

    def _cqlsh(cmds):
        """
        Modified from cqlsh_tests.py
        Attempts to make direct cqlsh communication.
        """
        cdir = nodes[0].get_install_dir()
        cli = os.path.join(cdir, 'bin', common.platform_binary('cqlsh'))
        env = common.make_cassandra_env(cdir, nodes[0].get_path())
        env['LANG'] = 'en_US.UTF-8'
        if LooseVersion(tester.cluster.version()) >= LooseVersion('2.1'):
            host = nodes[0].network_interfaces['binary'][0]
            port = nodes[0].network_interfaces['binary'][1]
        else:
            host = nodes[0].network_interfaces['thrift'][0]
            port = nodes[0].network_interfaces['thrift'][1]
        args = [host, str(port)]
        sys.stdout.flush()
        p = subprocess.Popen([cli] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        p.stdin.write("USE {};".format(enabled_ks()))
        for cmd in cmds.split(';'):
            p.stdin.write(cmd + ';\n')
        p.stdin.write("quit;\n")  # may not be necesary, things could simplify a bit if removed
        return p.communicate()

    def cqlsh(cmds, supress_err=False):
        """
        Run cqlsh commands and return regular output. If stderr occurs this will raise RuntimeError unless supress_err=True.
        """
        output, err = _cqlsh(cmds)

        if err and not supress_err:
            raise RuntimeError("Unexpected cqlsh error: {}".format(err))

        # if output is empty string we want to just return None
        if output:
            return output

    def cqlsh_print(cmds, supress_err=False):
        """
        Run cqlsh commands and print output.
        """
        output = cqlsh(cmds, supress_err=supress_err)

        # python coerces LF to OS-specific line-endings on print or write calls
        # unless the stream is opened in binary mode. It's cleaner just to
        # patch that up here so subsequent doctest comparisons to <BLANKLINE>
        # pass, as they'll fail on Windows w/whitespace + ^M (CRLF)
        if is_win():
            output = re.sub(os.linesep, '\n', output)

        if output:
            print(output)

    def cqlsh_err(cmds):
        """
        Run cqlsh commands expecting error.
        """
        output, err = _cqlsh(cmds)

        if not err:
            raise RuntimeError("Expected cqlsh error but none occurred!")

        return err

    def cqlsh_err_print(cmds):
        """
        Run cqlsh commands expecting error output, and print error output.
        """
        print(cqlsh_err(cmds))

    def cql(query):
        """
        Performs cql query via python driver connection.
        """
        return connection.execute(query)

    return {
        'ks': ks,
        'enabled_ks': enabled_ks,
        'cql': cql,
        'cqlsh': cqlsh,
        'cqlsh_print': cqlsh_print,
        'cqlsh_err': cqlsh_err,
        'cqlsh_err_print': cqlsh_err_print,
        'tester': tester
    }


def run_func_docstring(tester, test_func, globs=None, verbose=False, compileflags=None, optionflags=doctest.ELLIPSIS):
    """
    Similar to doctest.run_docstring_examples, but takes a single function/bound method,
    extracts it's singular docstring (no looking for subobjects with tests),
    runs it, and most importantly raises an exception if the test doesn't pass.

    tester should be an instance of dtest.Tester
    test_func should be a function/bound method the docstring to be tested
    """
    name = test_func.__name__

    if globs is None:
        globs = build_doc_context(tester, name)

    # dumb function that remembers values that it is called with
    # the DocTestRunner.run function called below accepts a callable for logging
    # and this is a hacky but easy way to capture the nicely formatted value for reporting
    def test_output_capturer(content):
        if not hasattr(test_output_capturer, 'content'):
            test_output_capturer.content = ''

        test_output_capturer.content += content

    test = doctest.DocTestParser().get_doctest(inspect.getdoc(test_func), globs, name, None, None)
    runner = doctest.DocTestRunner(verbose=verbose, optionflags=optionflags)
    runner.run(test, out=test_output_capturer, compileflags=compileflags)

    failed, attempted = runner.summarize()

    if failed > 0:
        raise RuntimeError("Doctest failed! Captured output:\n{}".format(test_output_capturer.content))

    if failed + attempted == 0:
        raise RuntimeError("No tests were run!")


@since('2.2')
class ToJsonSelectTests(Tester):
    """
    Tests using toJson with a SELECT statement
    """
    def basic_data_types_test(self):
        """
        Create our schema:

            >>> cqlsh_print('''
            ... CREATE TABLE primitive_type_test (
            ...  key1 text PRIMARY KEY,
            ...  col1 ascii,
            ...  col2 blob,
            ...  col3 inet,
            ...  col4 text,
            ...  col5 timestamp,
            ...  col6 timeuuid,
            ...  col7 uuid,
            ...  col8 varchar,
            ...  col9 bigint,
            ...  col10 decimal,
            ...  col11 double,
            ...  col12 float,
            ...  col13 int,
            ...  col14 varint,
            ...  col15 boolean)
            ... ''')

        Insert a row with only the key defined:

            >>> cqlsh('''
            ... INSERT into primitive_type_test (key1) values ('foo')
            ... ''')

        Get the non-key values as json:

            >>> cqlsh_print('''
            ... SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
            ...        toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
            ...        toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
            ...  FROM primitive_type_test WHERE key1 = 'foo'
            ... ''')
            <BLANKLINE>
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5) | system.tojson(col6) | system.tojson(col7) | system.tojson(col8) | system.tojson(col9) | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                            null |                null |                null |                null |                null |                null |                null |                null |                null |                 null |                 null |                 null |                 null |                 null |                 null
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

        Update the row to have all values defined:

            >>> cqlsh('''
            ... INSERT INTO primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
            ...   VALUES ('foo', 'bar', 0x0011, '127.0.0.1', 'blarg', '2011-02-03 04:05+0000', 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, 'bleh', -9223372036854775808, 1234.45678, 98712312.1222, 98712312.5252, -2147483648, 2147483648, true)
            ... ''')

        Query the values back as json:

            >>> cqlsh_print('''
            ... SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
            ...        toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
            ...        toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
            ...  FROM primitive_type_test WHERE key1 = 'foo'
            ... ''')
            <BLANKLINE>
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)       | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+---------------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011.....................| "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |      9.87123121222E7 |          9.8712312E7 |          -2147483648 |           2147483648 |                 true
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.basic_data_types_test)

    # yes, it's probably weird to use json for counter changes
    def counters_test(self):
        """
        Add a table with a few counters:

            >>> cqlsh('''
            ... CREATE TABLE my_counters (
            ...  key1 text PRIMARY KEY,
            ...  col1 counter,
            ...  col2 counter,
            ...  col3 counter )
            ... ''')

        Add a row with some counter values unset, and one incremented:

            >>> cqlsh("UPDATE my_counters SET col1 = col1+1 WHERE key1 = 'foo'")

        Query the empty/non-empty values back as json:

            >>> cqlsh_print('''
            ... SELECT toJson(col1), toJson(col2), toJson(col3) from my_counters
            ... ''')
            <BLANKLINE>
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3)
            ---------------------+---------------------+---------------------
                               1 |                null |                null
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.counters_test)

    def complex_data_types_test(self):
        """
        Build some user types and a schema that uses them:

            >>> cqlsh("CREATE TYPE t_todo_item (label text, details text)")
            >>> cqlsh("CREATE TYPE t_todo_list (name text, todo_list list<frozen<t_todo_item>>)")
            >>> cqlsh('''
            ... CREATE TYPE t_kitchen_sink (
            ...   item1 ascii,
            ...   item2 blob,
            ...   item3 inet,
            ...   item4 text,
            ...   item5 timestamp,
            ...   item6 timeuuid,
            ...   item7 uuid,
            ...   item8 varchar,
            ...   item9 bigint,
            ...   item10 decimal,
            ...   item11 double,
            ...   item12 float,
            ...   item13 int,
            ...   item14 varint,
            ...   item15 boolean,
            ...   item16 list<int> )
            ... ''')

            >>> cqlsh('''
            ... CREATE TABLE complex_types (
            ...   key1 text PRIMARY KEY,
            ...   mylist list<text>,
            ...   myset set<uuid>,
            ...   mymap map<text, int>,
            ...   mytuple frozen<tuple<text, int, uuid, boolean>>,
            ...   myudt frozen<t_kitchen_sink>,
            ...   mytodolists list<frozen<t_todo_list>>,
            ...   many_sinks list<frozen<t_kitchen_sink>>,
            ...   named_sinks map<text, frozen<t_kitchen_sink>> )
            ... ''')

        Add a row without the complex fields defined:

            >>> cqlsh("INSERT INTO complex_types (key1) values ('foo')")

        Call toJson on the null fields:

            >>> cqlsh_print('''
            ... SELECT toJson(mylist), toJson(myset), toJson(mymap), toJson(mytuple), toJson(myudt), toJson(mytodolists), toJson(many_sinks), toJson(named_sinks)
            ...   FROM complex_types where key1 = 'foo'
            ... ''')
            <BLANKLINE>
             system.tojson(mylist) | system.tojson(myset) | system.tojson(mymap) | system.tojson(mytuple) | system.tojson(myudt) | system.tojson(mytodolists) | system.tojson(many_sinks) | system.tojson(named_sinks)
            -----------------------+----------------------+----------------------+------------------------+----------------------+----------------------------+---------------------------+----------------------------
                              null |                 null |                 null |                   null |                 null |                       null |                      null |                       null
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

        Define a row with the complex data types:

            >>> cqlsh('''
            ... INSERT INTO complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
            ... VALUES (
            ...   'foo',
            ...   ['five', 'six', 'seven', 'eight'],
            ...   {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
            ...   {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
            ...   ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
            ...   {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
            ...   [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
            ...   [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
            ...   {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
            ... ''')

        Query back the json (one field at a time to make it easier to read) and make sure it looks as it should
        Check that the list is returned ok:

            >>> cqlsh_print("SELECT toJson(mylist) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(mylist)
            -----------------------------------
             ["five", "six", "seven", "eight"]
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT toJson(myset) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(myset)
            --------------------------------------------------------------------------------------------------------------------------
             ["080fdd90-ae74-41d6-9883-635625d3b069", "4b66458a-2a19-41d3-af25-6faef4dea9fe", "6cd7fab5-eacc-45c3-8414-6ad0177651d6"]
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT toJson(mymap) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(mymap)
            ---------------------------------------------
             {"four": 4, "one": 1, "three": 3, "two": 2}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT toJson(mytuple) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(mytuple)
            -----------------------------------------------------------
             ["hey", 10, "16e69fba-a656-4932-8a01-6782a34505d9", true]
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT toJson(myudt) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(myudt)
            -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"item1": "heyimascii", "item2": "0x0011", "item3": "127.0.0.1", "item4": "whatev", "item5": "2011...", "item6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "item7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "item8": "bleh", "item9": -9223372036854775808, "item10": 1234.45678, "item11": 9.87123121222E7, "item12": 9.8712312E7, "item13": -2147483648, "item14": 2147483647, "item15": false, "item16": [1, 3, 5, 7, 11, 13]}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT toJson(mytodolists) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(mytodolists)
            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{"name": "stuff to do!", "todo_list": [{"label": "buy groceries", "details": "bread and milk"}, {"label": "pick up car from shop", "details": "$325 due"}, {"label": "call dave", "details": "for some reason"}]}, {"name": "more stuff to do!", "todo_list": [{"label": "buy new car", "details": "the old one is getting expensive"}, {"label": "price insurance", "details": "current cost is $95/mo"}]}]
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT toJson(many_sinks) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(many_sinks)
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012...", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 1.00123121222E7, "item12": 4.0012312E7, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1, 1, 2, 3, 5, 8]}, {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013...", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 2.00123121222E7, "item12": 5.0012312E7, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3, 6, 9, 12, 15]}, {"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014...", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 3.00123121222E7, "item12": 6.0012312E7, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0, 1, 0, 1, 2, 0]}]
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT toJson(named_sinks) from complex_types where key1 = 'foo'")
            <BLANKLINE>
             system.tojson(named_sinks)
            ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"namedsink1": {"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012...", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 1.00123121222E7, "item12": 4.0012312E7, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1, 1, 2, 3, 5, 8]}, "namedsink2": {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013...", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 2.00123121222E7, "item12": 5.0012312E7, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3, 6, 9, 12, 15]}, "namedsink3": {"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014...", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 3.00123121222E7, "item12": 6.0012312E7, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0, 1, 0, 1, 2, 0]}}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.complex_data_types_test)


@since('2.2')
class FromJsonUpdateTests(Tester):
    """
    Tests using fromJson within UPDATE statements.
    """
    def basic_data_types_test(self):
        """
        Create a table with the primitive types:

            >>> cqlsh('''
            ... CREATE TABLE primitive_type_test (
            ...   key1 text PRIMARY KEY,
            ...   col1 ascii,
            ...   col2 blob,
            ...   col3 inet,
            ...   col4 text,
            ...   col5 timestamp,
            ...   col6 timeuuid,
            ...   col7 uuid,
            ...   col8 varchar,
            ...   col9 bigint,
            ...   col10 decimal,
            ...   col11 double,
            ...   col12 float,
            ...   col13 int,
            ...   col14 varint,
            ...   col15 boolean)
            ... ''')

        Create a basic row and update the row using fromJson:

            >>> cqlsh('''INSERT INTO primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
            ... VALUES ('test', 'bar', 0x0011, '127.0.0.1', 'blarg', '2011-02-03 04:05+0000', 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, 'bleh', -9223372036854775808, 1234.45678, 98712312.1222, 98712312.5252, -2147483648, 2147483648, true)
            ... ''')

            >>> cqlsh('''
            ... UPDATE primitive_type_test
            ... SET col1 = fromJson('"bar1"'),
            ...     col2 = fromJson('"0x0012"'),
            ...     col3 = fromJson('"127.0.0.2"'),
            ...     col4 = fromJson('"blarg2"'),
            ...     col5 = fromJson('"2011-02-02 23:05:00.000"'),
            ...     col6 = fromJson('"efe0922a-8638-11e4-b2ac-b4b6763e9d6f"'),
            ...     col7 = fromJson('"05dd0249-25b4-4dec-ba27-54f8730f3c03"'),
            ...     col8 = fromJson('"bleh2"'),
            ...     col9 = fromJson('-8223372036854775808'),
            ...     col10 = fromJson('"2234.45678"'),
            ...     col11 = fromJson('8.87123121222E7'),
            ...     col12 = fromJson('7.8712312E7'),
            ...     col13 = fromJson('-1947483648'),
            ...     col14 = fromJson('"1847483648"'),
            ...     col15 = fromJson('false')
            ... WHERE key1 = 'test'
            ... ''')

        Query back the row and make sure data is represented correctly:

            >>> cqlsh_print('''
            ... SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15
            ...   FROM primitive_type_test WHERE key1 = 'test'
            ... ''')
            <BLANKLINE>
             col1 | col2   | col3      | col4   | col5                     | col6                                 | col7                                 | col8  | col9                 | col10      | col11      | col12      | col13       | col14      | col15
            ------+--------+-----------+--------+--------------------------+--------------------------------------+--------------------------------------+-------+----------------------+------------+------------+------------+-------------+------------+-------
             bar1 | 0x0012 | 127.0.0.2 | blarg2 | 2011.....................| efe0922a-8638-11e4-b2ac-b4b6763e9d6f | 05dd0249-25b4-4dec-ba27-54f8730f3c03 | bleh2 | -8223372036854775808 | 2234.45678 | 8.8712e+07 | 7.8712e+07 | -1947483648 | 1847483648 | False
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.basic_data_types_test)

    def complex_data_types_test(self):
        """"
        UDT and schema setup:

            >>> cqlsh('''
            ... CREATE TYPE t_todo_item (
            ...   label text,
            ...   details text)
            ... ''')

            >>> cqlsh('''
            ... CREATE TYPE t_todo_list (
            ...   name text,
            ...   todo_list list<frozen<t_todo_item>>)
            ... ''')

            >>> cqlsh('''
            ... CREATE TYPE t_kitchen_sink (
            ...   item1 ascii,
            ...   item2 blob,
            ...   item3 inet,
            ...   item4 text,
            ...   item5 timestamp,
            ...   item6 timeuuid,
            ...   item7 uuid,
            ...   item8 varchar,
            ...   item9 bigint,
            ...   item10 decimal,
            ...   item11 double,
            ...   item12 float,
            ...   item13 int,
            ...   item14 varint,
            ...   item15 boolean,
            ...   item16 list<int> )
            ... ''')

            >>> cqlsh('''
            ... CREATE TABLE complex_types (
            ...   key1 text PRIMARY KEY,
            ...   mylist list<text>,
            ...   myset set<uuid>,
            ...   mymap map<text, int>,
            ...   mytuple frozen<tuple<text, int, uuid, boolean>>,
            ...   myudt frozen<t_kitchen_sink>,
            ...   mytodolists list<frozen<t_todo_list>>,
            ...   many_sinks list<frozen<t_kitchen_sink>>,
            ...   named_sinks map<text, frozen<t_kitchen_sink>> )
            ... ''')

        Insert a row using plain cql values, then update the complex types using fromJson:

            >>> cqlsh('''
            ... INSERT INTO complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
            ... VALUES (
            ...   'row1',
            ...   ['five', 'six', 'seven', 'eight'],
            ...   {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
            ...   {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
            ...   ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
            ...   {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
            ...   [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
            ...   [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
            ...   {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
            ... ''')

            >>> cqlsh('''
            ... UPDATE complex_types
            ... SET mylist = fromJson('["nine", "ten", "eleven"]'),
            ...     myset = fromJson('["74887ce9-cea2-4d63-b874-cbe0a376bd3b", "3819f267-7075-4261-a33a-72e1e5a851d9"]'),
            ...     mymap = fromJson('{"five" : 5, "six" : 6, "seven": 7}'),
            ...     mytuple = fromJson('["whah?", 437, "e2058138-9a60-4f72-94f1-f48a21d59ff2", false]'),
            ...     myudt = fromJson('{"item1": "imitem1", "item2": "0x0014", "item3": "127.0.1.3", "item4": "asdf", "item5": "2009-02-01 04:05+0000", "item6": "25ba53f2-8645-11e4-afbc-b4b6763e9d6f", "item7": "97e053fb-225d-400a-9d4d-6b989e7d0cd9", "item8": "fdsa", "item9": -5223372036854775808, "item10": 2134.45678, "item11": 78712312.1222, "item12": 66712312.5252, "item13": -1347483648, "item14": 1097483647, "item15": true, "item16": [13,11,7,5,3,2,1]}'),
            ...     mytodolists = fromJson('[{"name": "a simple todo list", "todo_list": [{"label": "go to the store", "details": "need bread and milk"}, {"label": "drop off rental car", "details": "$180 due"}, {"label": "call bob", "details": "left a message"}]}, {"name": "a second todo list", "todo_list":[{"label": "buy a race car", "details": "need to go faster"}, {"label": "go running", "details": "just because"}]}]'),
            ...     many_sinks = fromJson('[{"item1": "qwerty", "item2": "0x0212", "item3": "127.5.5.5", "item4": "whatever0", "item5": "1999-02-03 04:05+0000", "item6": "3137ee20-8649-11e4-853d-b4b6763e9d6f", "item7": "034ca793-7147-45f5-bc60-2d5e76cc735d", "item8": "rewq", "item9": -6623372036854771111, "item10": 321.45678, "item11": 9992312.1222, "item12": 33012312.5252, "item13": -1547483648, "item14": 1847483648, "item15": false, "item16": [0,0,5,5,10,20]}, {"item1": "ljsdf", "item2": "0x2131", "item3": "10.10.1.5", "item4": "whatever1", "item5": "1987-02-08 02:05+0000", "item6": "a7a7c3d2-8649-11e4-88ff-b4b6763e9d6f", "item7": "9dfc88cd-ab10-4cf8-8046-344c905558c4", "item8": "vbnm", "item9": -1933372036854775808, "item10": 5534.12321, "item11": 19912312.1222, "item12": 49912312.5252, "item13": -997483648, "item14": 1007483648, "item15": true, "item16": [6,9,12,15,18,21]},{"item1": "hjkl", "item2": "0x2006", "item3": "127.3.3.1", "item4": "whatever2", "item5": "1996-04-01 04:05+0000", "item6": "0ed3a30a-864a-11e4-a596-b4b6763e9d6f", "item7": "49a74392-8295-4370-ba49-2ea57aaa5107", "item8": "nanananana", "item9": -1859372036854775808, "item10": 5455.55555, "item11": 29992312.1222, "item12": 59912312.5252, "item13": 1327483647, "item14": 1017483648, "item15": false, "item16": [1,0,5,10,1000]}]'),
            ...     named_sinks = fromJson('{"namedsink5000":{"item1": "aaaaaa", "item2": "0x5555", "item3": "127.1.2.3", "item4": "whatev10", "item5": "1999-02-01 01:05+0000", "item6": "c2c5dc0c-864a-11e4-adc6-b4b6763e9d6f", "item7": "2d630a5d-a9e1-4e4b-b551-fb953e16c1f8", "item8": "8s8s8s8aaaa", "item9": -3323372036854771111, "item10": 1221.45678, "item11": 8812312.1222, "item12": 20112312.5252, "item13": -757483648, "item14": 1017483648, "item15": false, "item16": [6,7,8,9,8,7]},"namedsink5001":{"item1": "bbbbbb", "item2": "0x000092", "item3": "192.168.0.3", "item4": "whatev20", "item5": "2003-08-03 01:05+0000", "item6": "3e4c2a16-864b-11e4-bb20-b4b6763e9d6f", "item7": "e404dd5c-9dc7-4b1d-bce9-eb85bb72297c", "item8": "mariachi", "item9": -2211372036854775808, "item10": 34.12321, "item11": 13132312.1222, "item12": 39912312.5252, "item13": -987483648, "item14": 1047483648, "item15": true, "item16": [500]},"namedsink5002":{"item1": "ccccccccc", "item2": "0x1002", "item3": "192.168.100.5", "item4": "whatev30", "item5": "2017-012-03 04:05+0000", "item6": "65badb36-8652-11e4-a5b9-b4b6763e9d6f", "item7": "aa371f6e-8044-4d11-8aec-2e698e55bb87", "item8": "abcdef", "item9": -1144372036854775808, "item10": 4321.55555, "item11": 21212312.1222, "item12": 33912312.5252, "item13": 1147483647, "item14": 1047483648, "item15": false, "item16": [100,200,500,300]}}')
            ... WHERE key1 = 'row1'
            ... ''')

        Query back the fields one by one and make sure they match the updates:

            >>> cqlsh_print("SELECT mylist from complex_types where key1 = 'row1'")
            <BLANKLINE>
             mylist
            ---------------------------
             ['nine', 'ten', 'eleven']
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT myset from complex_types where key1 = 'row1'")
            <BLANKLINE>
             myset
            ------------------------------------------------------------------------------
             {3819f267-7075-4261-a33a-72e1e5a851d9, 74887ce9-cea2-4d63-b874-cbe0a376bd3b}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT mymap from complex_types where key1 = 'row1'")
            <BLANKLINE>
             mymap
            -----------------------------------
             {'five': 5, 'seven': 7, 'six': 6}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT mytuple from complex_types where key1 = 'row1'")
            <BLANKLINE>
             mytuple
            -------------------------------------------------------------
             ('whah?', 437, e2058138-9a60-4f72-94f1-f48a21d59ff2, False)
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT myudt from complex_types where key1 = 'row1'")
            <BLANKLINE>
             myudt
            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {item1: 'imitem1', item2: 0x0014, item3: '127.0.1.3', item4: 'asdf', item5: '2009...', item6: 25ba53f2-8645-11e4-afbc-b4b6763e9d6f, item7: 97e053fb-225d-400a-9d4d-6b989e7d0cd9, item8: 'fdsa', item9: -5223372036854775808, item10: 2134.45678, item11: 7.8712e+07, item12: 6.6712e+07, item13: -1347483648, item14: 1097483647, item15: True, item16: [13, 11, 7, 5, 3, 2, 1]}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT mytodolists from complex_types where key1 = 'row1'")
            <BLANKLINE>
             mytodolists
            ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{name: 'a simple todo list', todo_list: [{label: 'go to the store', details: 'need bread and milk'}, {label: 'drop off rental car', details: '$180 due'}, {label: 'call bob', details: 'left a message'}]}, {name: 'a second todo list', todo_list: [{label: 'buy a race car', details: 'need to go faster'}, {label: 'go running', details: 'just because'}]}]
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT many_sinks from complex_types where key1 = 'row1'")
            <BLANKLINE>
             many_sinks
            -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             [{item1: 'qwerty', item2: 0x0212, item3: '127.5.5.5', item4: 'whatever0', item5: '1999...', item6: 3137ee20-8649-11e4-853d-b4b6763e9d6f, item7: 034ca793-7147-45f5-bc60-2d5e76cc735d, item8: 'rewq', item9: -6623372036854771111, item10: 321.45678, item11: 9.9923e+06, item12: 3.3012e+07, item13: -1547483648, item14: 1847483648, item15: False, item16: [0, 0, 5, 5, 10, 20]}, {item1: 'ljsdf', item2: 0x2131, item3: '10.10.1.5', item4: 'whatever1', item5: '1987...', item6: a7a7c3d2-8649-11e4-88ff-b4b6763e9d6f, item7: 9dfc88cd-ab10-4cf8-8046-344c905558c4, item8: 'vbnm', item9: -1933372036854775808, item10: 5534.12321, item11: 1.9912e+07, item12: 4.9912e+07, item13: -997483648, item14: 1007483648, item15: True, item16: [6, 9, 12, 15, 18, 21]}, {item1: 'hjkl', item2: 0x2006, item3: '127.3.3.1', item4: 'whatever2', item5: '1996...', item6: 0ed3a30a-864a-11e4-a596-b4b6763e9d6f, item7: 49a74392-8295-4370-ba49-2ea57aaa5107, item8: 'nanananana', item9: -1859372036854775808, item10: 5455.55555, item11: 2.9992e+07, item12: 5.9912e+07, item13: 1327483647, item14: 1017483648, item15: False, item16: [1, 0, 5, 10, 1000]}]
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT named_sinks from complex_types where key1 = 'row1'")
            <BLANKLINE>
             named_sinks
            ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {'namedsink5000': {item1: 'aaaaaa', item2: 0x5555, item3: '127.1.2.3', item4: 'whatev10', item5: '1999...', item6: c2c5dc0c-864a-11e4-adc6-b4b6763e9d6f, item7: 2d630a5d-a9e1-4e4b-b551-fb953e16c1f8, item8: '8s8s8s8aaaa', item9: -3323372036854771111, item10: 1221.45678, item11: 8.8123e+06, item12: 2.0112e+07, item13: -757483648, item14: 1017483648, item15: False, item16: [6, 7, 8, 9, 8, 7]}, 'namedsink5001': {item1: 'bbbbbb', item2: 0x000092, item3: '192.168.0.3', item4: 'whatev20', item5: '2003...', item6: 3e4c2a16-864b-11e4-bb20-b4b6763e9d6f, item7: e404dd5c-9dc7-4b1d-bce9-eb85bb72297c, item8: 'mariachi', item9: -2211372036854775808, item10: 34.12321, item11: 1.3132e+07, item12: 3.9912e+07, item13: -987483648, item14: 1047483648, item15: True, item16: [500]}, 'namedsink5002': {item1: 'ccccccccc', item2: 0x1002, item3: '192.168.100.5', item4: 'whatev30', item5: '2017...', item6: 65badb36-8652-11e4-a5b9-b4b6763e9d6f, item7: aa371f6e-8044-4d11-8aec-2e698e55bb87, item8: 'abcdef', item9: -1144372036854775808, item10: 4321.55555, item11: 2.1212e+07, item12: 3.3912e+07, item13: 1147483647, item14: 1047483648, item15: False, item16: [100, 200, 500, 300]}}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.complex_data_types_test)

    def collection_update_test(self):
        """
        Setup schema, add a row:

            >>> cqlsh('''
            ... CREATE TABLE basic_collections (
            ...   key1 text PRIMARY KEY,
            ...   mylist list<text>,
            ...   myset set<text>,
            ...   mymap map<text, int>)
            ... ''')

            >>> cqlsh("INSERT INTO basic_collections (key1) values ('row1')")

        Issue some updates:

            >>> cqlsh('''
            ... UPDATE basic_collections
            ... SET mylist = fromJson('["c"]'),
            ...     myset = fromJson('["f"]'),
            ...     mymap = fromJson('{"one": 1}')
            ... WHERE key1 = 'row1'
            ... ''')

            >>> cqlsh('''
            ... UPDATE basic_collections
            ... SET mylist = fromJson('["a","b"]') + mylist,
            ...     myset = myset + fromJson('["d","e"]'),
            ...     mymap['two'] = fromJson('2')
            ... WHERE key1 = 'row1'
            ... ''')

        Query the row and make sure it's correct:

            >>> cqlsh_print("SELECT * from basic_collections where key1 = 'row1'")
            <BLANKLINE>
             key1 | mylist          | mymap                | myset
            ------+-----------------+----------------------+-----------------
             row1 | ['a', 'b', 'c'] | {'one': 1, 'two': 2} | {'d', 'e', 'f'}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

        Some more updates of differing types:

            >>> cqlsh('''
            ... UPDATE basic_collections
            ... SET mylist = mylist + fromJson('["d","e"]'),
            ...     myset = myset + fromJson('["g"]'),
            ...     mymap['three'] = fromJson('3')
            ... WHERE key1 = 'row1'
            ... ''')

            >>> cqlsh('''
            ... UPDATE basic_collections
            ... SET mylist = mylist - fromJson('["b"]'),
            ...     myset = myset - fromJson('["d"]'),
            ...     mymap['three'] = fromJson('4')
            ... WHERE key1 = 'row1'
            ... ''')

            Query final state and check it:

            >>> cqlsh_print("SELECT * from basic_collections where key1 = 'row1'")
            <BLANKLINE>
             key1 | mylist               | mymap                            | myset
            ------+----------------------+----------------------------------+-----------------
             row1 | ['a', 'c', 'd', 'e'] | {'one': 1, 'three': 4, 'two': 2} | {'e', 'f', 'g'}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.collection_update_test)


@since('2.2')
class FromJsonSelectTests(Tester):
    """
    Tests using fromJson in conjunction with a SELECT statement
    """
    def selecting_pkey_as_json_test(self):
        """
        Schema setup:

            >>> cqlsh('''
            ... CREATE TYPE t_person_name (
            ...   first text,
            ...   middle text,
            ...   last text)
            ... ''')

            >>> cqlsh('''
            ... CREATE TABLE person_info (
            ...   name frozen<t_person_name> PRIMARY KEY,
            ...   info text )
            ... ''')

        Add a row:

            >>> cqlsh("INSERT INTO person_info (name, info) VALUES ({first: 'test', middle: 'guy', last: 'jones'}, 'enjoys bacon')")

        Query the row back on the primary key with fromJson:

            >>> cqlsh_print('''
            ... SELECT * FROM person_info WHERE name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
            ... ''')
            <BLANKLINE>
             name                                          | info
            -----------------------------------------------+--------------
             {first: 'test', middle: 'guy', last: 'jones'} | enjoys bacon
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.selecting_pkey_as_json_test)

    def select_using_secondary_index_test(self):
        """
        Schema setup and secondary index:

            >>> cqlsh('''
            ... CREATE TYPE t_person_name (
            ...   first text,
            ...   middle text,
            ...   last text )
            ... ''')

            >>> cqlsh('''
            ... CREATE TABLE person_likes (
            ...   id uuid PRIMARY KEY,
            ...   name frozen<t_person_name>,
            ...   like text )
            ... ''')

            >>> cqlsh("CREATE INDEX person_likes_name ON person_likes (name)")

        Add a row:

            >>> cqlsh("INSERT INTO person_likes (id, name, like) VALUES (99b81888-e889-44aa-a511-cbd451c8a024, {first:'test', middle: 'guy', last:'jones'}, 'art')")

        Query the row back using fromJson with the secondary index:

            >>> cqlsh_print('''
            ... SELECT * from person_likes where name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
            ... ''')
            <BLANKLINE>
             id                                   | like | name
            --------------------------------------+------+-----------------------------------------------
             99b81888-e889-44aa-a511-cbd451c8a024 |  art | {first: 'test', middle: 'guy', last: 'jones'}
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.select_using_secondary_index_test)


@since('2.2')
class FromJsonInsertTests(Tester):
    """
    Tests using fromJson within INSERT statements.
    """
    def basic_data_types_test(self):
        """
        Create a table with the primitive types:

            >>> cqlsh('''
            ... CREATE TABLE primitive_type_test (
            ...   key1 text PRIMARY KEY,
            ...   col1 ascii,
            ...   col2 blob,
            ...   col3 inet,
            ...   col4 text,
            ...   col5 timestamp,
            ...   col6 timeuuid,
            ...   col7 uuid,
            ...   col8 varchar,
            ...   col9 bigint,
            ...   col10 decimal,
            ...   col11 double,
            ...   col12 float,
            ...   col13 int,
            ...   col14 varint,
            ...   col15 boolean)
            ... ''')

        Create a full row using fromJson for each value:

            >>> cqlsh('''
            ... INSERT INTO primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
            ...   VALUES (fromJson('"test"'), fromJson('"bar"'), fromJson('"0x0011"'), fromJson('"127.0.0.1"'), fromJson('"blarg"'), fromJson('"2011-02-02 21:05:00.000"'), fromJson('"0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f"'), fromJson('"bdf5e8ac-a75e-4321-9ac8-938fc9576c4a"'), fromJson('"bleh"'), fromJson('-9223372036854775808'), fromJson('"1234.45678"'), fromJson('9.87123121222E7'), fromJson('9.8712312E7'), fromJson('-2147483648'), fromJson('"2147483648"'), fromJson('true'))
            ... ''')

        Query back the row and make sure data is represented correctly:

            >>> cqlsh_print('''
            ... SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15
            ... FROM primitive_type_test WHERE key1 = 'test'
            ... ''')
            <BLANKLINE>
             col1 | col2   | col3      | col4  | col5                     | col6                                 | col7                                 | col8 | col9                 | col10      | col11      | col12      | col13       | col14      | col15
            ------+--------+-----------+-------+--------------------------+--------------------------------------+--------------------------------------+------+----------------------+------------+------------+------------+-------------+------------+-------
              bar | 0x0011 | 127.0.0.1 | blarg | 2011.....................| 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f | bdf5e8ac-a75e-4321-9ac8-938fc9576c4a | bleh | -9223372036854775808 | 1234.45678 | 9.8712e+07 | 9.8712e+07 | -2147483648 | 2147483648 |  True
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

        Query row back as json to see if the json representation queried from the DB matches the json that was used on insert:

            >>> cqlsh_print('''
            ... SELECT toJson(col1), toJson(col2), toJson(col3), toJson(col4), toJson(col5),
            ...        toJson(col6), toJson(col7), toJson(col8), toJson(col9), toJson(col10),
            ...        toJson(col11),toJson(col12),toJson(col13),toJson(col14),toJson(col15)
            ... FROM primitive_type_test WHERE key1 = 'test'
            ... ''')
            <BLANKLINE>
             system.tojson(col1) | system.tojson(col2) | system.tojson(col3) | system.tojson(col4) | system.tojson(col5)       | system.tojson(col6)                    | system.tojson(col7)                    | system.tojson(col8) | system.tojson(col9)  | system.tojson(col10) | system.tojson(col11) | system.tojson(col12) | system.tojson(col13) | system.tojson(col14) | system.tojson(col15)
            ---------------------+---------------------+---------------------+---------------------+---------------------------+----------------------------------------+----------------------------------------+---------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------
                           "bar" |            "0x0011" |         "127.0.0.1" |             "blarg" | "2011.....................| "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f" | "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a" |              "bleh" | -9223372036854775808 |           1234.45678 |      9.87123121222E7 |          9.8712312E7 |          -2147483648 |           2147483648 |                 true
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.basic_data_types_test)

    def complex_data_types_test(self):
        """
        Build some user types and a schema that uses them:

            >>> cqlsh("CREATE TYPE t_todo_item (label text, details text)")
            >>> cqlsh("CREATE TYPE t_todo_list (name text, todo_list list<frozen<t_todo_item>>)")
            >>> cqlsh('''
            ... CREATE TYPE t_kitchen_sink (
            ...   item1 ascii,
            ...   item2 blob,
            ...   item3 inet,
            ...   item4 text,
            ...   item5 timestamp,
            ...   item6 timeuuid,
            ...   item7 uuid,
            ...   item8 varchar,
            ...   item9 bigint,
            ...   item10 decimal,
            ...   item11 double,
            ...   item12 float,
            ...   item13 int,
            ...   item14 varint,
            ...   item15 boolean,
            ...   item16 list<int> )
            ... ''')

            >>> cqlsh('''
            ... CREATE TABLE complex_types (
            ...   key1 text PRIMARY KEY,
            ...   mylist list<text>,
            ...   myset set<uuid>,
            ...   mymap map<text, int>,
            ...   mytuple frozen<tuple<text, int, uuid, boolean>>,
            ...   myudt frozen<t_kitchen_sink>,
            ...   mytodolists list<frozen<t_todo_list>>,
            ...   many_sinks list<frozen<t_kitchen_sink>>,
            ...   named_sinks map<text, frozen<t_kitchen_sink>> )
            ... ''')

            >>> cqlsh('''
            ... INSERT INTO complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
            ... VALUES (
            ...   'row1',
            ...   ['five', 'six', 'seven', 'eight'],
            ...   {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
            ...   {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
            ...   ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
            ...   {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
            ...   [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
            ...   [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
            ...   {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
            ... ''')

        Add a row:

            >>> cqlsh('''
            ... INSERT INTO complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
            ... VALUES (
            ...   fromJson('"row2"'),
            ...   fromJson('["five", "six", "seven", "eight"]'),
            ...   fromJson('["4b66458a-2a19-41d3-af25-6faef4dea9fe", "080fdd90-ae74-41d6-9883-635625d3b069", "6cd7fab5-eacc-45c3-8414-6ad0177651d6"]'),
            ...   fromJson('{"one" : 1, "two" : 2, "three": 3, "four": 4}'),
            ...   fromJson('["hey", 10, "16e69fba-a656-4932-8a01-6782a34505d9", true]'),
            ...   fromJson('{"item1": "heyimascii", "item2": "0x0011", "item3": "127.0.0.1", "item4": "whatev", "item5": "2011-02-03 04:05+0000", "item6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "item7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "item8": "bleh", "item9": -9223372036854775808, "item10": 1234.45678, "item11": 98712312.1222, "item12": 98712312.5252, "item13": -2147483648, "item14": 2147483647, "item15": false, "item16": [1,3,5,7,11,13]}'),
            ...   fromJson('[{"name": "stuff to do!", "todo_list": [{"label": "buy groceries", "details": "bread and milk"}, {"label": "pick up car from shop", "details": "$325 due"}, {"label": "call dave", "details": "for some reason"}]}, {"name": "more stuff to do!", "todo_list":[{"label": "buy new car", "details": "the old one is getting expensive"}, {"label": "price insurance", "details": "current cost is $95/mo"}]}]'),
            ...   fromJson('[{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]}, {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}]'),
            ...   fromJson('{"namedsink1":{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]},"namedsink2":{"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},"namedsink3":{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}}'))
            ... ''')

        Query back the the normal cql inserted row and the fromJson inserted row, and compare to make sure they match (do this one field at a time for easier reading):

            >>> cqlsh_print("SELECT key1, mylist from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | mylist
            ------+-----------------------------------
             row1 | ['five', 'six', 'seven', 'eight']
             row2 | ['five', 'six', 'seven', 'eight']
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, myset from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | myset
            ------+--------------------------------------------------------------------------------------------------------------------
             row1 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}
             row2 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, mymap from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | mymap
            ------+---------------------------------------------
             row1 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}
             row2 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, mytuple from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | mytuple
            ------+---------------------------------------------------------
             row1 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)
             row2 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, myudt from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | myudt
            ------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011...', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}
             row2 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011...', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, mytodolists from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | mytodolists
            ------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]
             row2 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, many_sinks from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | many_sinks
            ------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]
             row2 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, named_sinks from complex_types where key1 in ('row1', 'row2')")
            <BLANKLINE>
             key1 | named_sinks
            ------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}
             row2 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.complex_data_types_test)


@since('2.2')
class FromJsonDeleteTests(Tester):
    """
    Tests using fromJson within DELETE statements.
    """
    def delete_using_pkey_json_test(self):
        """
        Schema setup:

            >>> cqlsh('''
            ... CREATE TYPE t_person_name (
            ...   first text,
            ...   middle text,
            ...   last text)
            ... ''')

            >>> cqlsh('''
            ... CREATE TABLE person_info (
            ...   name frozen<t_person_name> PRIMARY KEY,
            ...   info text)
            ... ''')

        Add a row:

            >>> cqlsh("INSERT INTO person_info (name, info) VALUES ({first: 'test', middle: 'guy', last: 'jones'}, 'enjoys bacon')")

            Make sure the row is there:

            >>> cqlsh_print('''
            ... SELECT * FROM person_info WHERE name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
            ... ''')
            <BLANKLINE>
             name                                          | info
            -----------------------------------------------+--------------
             {first: 'test', middle: 'guy', last: 'jones'} | enjoys bacon
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>

        Delete the row using a fromJson clause:

            >>> cqlsh('''
            ... DELETE FROM person_info WHERE name = fromJson('{"first":"test", "middle":"guy", "last":"jones"}')
            ... ''')

        Make sure the row is gone:

            >>> cqlsh_print("SELECT COUNT(*) from person_info")
            <BLANKLINE>
             count
            -------
                 0
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.delete_using_pkey_json_test)


@since('2.2')
class JsonFullRowInsertSelect(Tester):
    """
    Tests for creating full rows from json documents, selecting full rows back as json documents, and related functionality.
    """
    def simple_schema_test(self):
        """
        Create schema:

            >>> cqlsh('''
            ... CREATE TABLE primitive_type_test (
            ...   key1 text PRIMARY KEY,
            ...   col1 ascii,
            ...   col2 blob,
            ...   col3 inet,
            ...   col4 text,
            ...   col5 timestamp,
            ...   col6 timeuuid,
            ...   col7 uuid,
            ...   col8 varchar,
            ...   col9 bigint,
            ...   col10 decimal,
            ...   col11 double,
            ...   col12 float,
            ...   col13 int,
            ...   col14 varint,
            ...   col15 boolean)
            ... ''')

        Add two rows with all null values, create the first row using a regular INSERT statement, and the second row using JSON. Different key for each row:

            >>> cqlsh("INSERT INTO primitive_type_test (key1) values ('foo')")

            >>> cqlsh('''
            ... INSERT INTO primitive_type_test JSON '{"key1":"bar"}'
            ... ''')

        Query back both rows as JSON:

            >>> cqlsh_print("SELECT JSON * FROM primitive_type_test")
            <BLANKLINE>
             [json]
            -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"key1": "bar", "col1": null, "col10": null, "col11": null, "col12": null, "col13": null, "col14": null, "col15": null, "col2": null, "col3": null, "col4": null, "col5": null, "col6": null, "col7": null, "col8": null, "col9": null}
             {"key1": "foo", "col1": null, "col10": null, "col11": null, "col12": null, "col13": null, "col14": null, "col15": null, "col2": null, "col3": null, "col4": null, "col5": null, "col6": null, "col7": null, "col8": null, "col9": null}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

        Query back both rows as non-JSON to be sure they look ok there:

            >>> cqlsh_print("SELECT * FROM primitive_type_test")
            <BLANKLINE>
             key1 | col1 | col10 | col11 | col12 | col13 | col14 | col15 | col2 | col3 | col4 | col5 | col6 | col7 | col8 | col9
            ------+------+-------+-------+-------+-------+-------+-------+------+------+------+------+------+------+------+------
              bar | null |  null |  null |  null |  null |  null |  null | null | null | null | null | null | null | null | null
              foo | null |  null |  null |  null |  null |  null |  null | null | null | null | null | null | null | null | null
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

        Use a plain insert to update one row, and a JSON insert to update the other:

            >>> cqlsh('''
            ... INSERT INTO primitive_type_test (key1, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
            ...   VALUES ('foo', 'bar', 0x0011, '127.0.0.1', 'blarg', '2011-02-03 04:05+0000', 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, 'bleh', -9223372036854775808, 1234.45678, 98712312.1222, 98712312.5252, -2147483648, 2147483648, true)
            ... ''')

            >>> cqlsh('''
            ... INSERT INTO primitive_type_test JSON '{"key1": "bar", "col1": "bar", "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011-02-02 21:05:00.000", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": -9223372036854775808, "col10": "1234.45678", "col11":9.87123121222E7, "col12": 9.87123121222E7, "col13": -2147483648, "col14": 2147483648, "col15": true}'
            ... ''')

        Query back both rows as JSON:

            >>> cqlsh_print("SELECT JSON * FROM primitive_type_test")
            <BLANKLINE>
             [json]
            --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"key1": "bar", "col1": "bar", "col10": 1234.45678, "col11": 9.87123121222E7, "col12": 9.8712312E7, "col13": -2147483648, "col14": 2147483648, "col15": true, "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011...", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": -9223372036854775808}
             {"key1": "foo", "col1": "bar", "col10": 1234.45678, "col11": 9.87123121222E7, "col12": 9.8712312E7, "col13": -2147483648, "col14": 2147483648, "col15": true, "col2": "0x0011", "col3": "127.0.0.1", "col4": "blarg", "col5": "2011...", "col6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "col7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "col8": "bleh", "col9": -9223372036854775808}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

        Query back both rows, but with only some JSON fields:

            >>> cqlsh_print("SELECT JSON col15, col1, col3, col13, col11, col2, col4 FROM primitive_type_test WHERE key1 in ('foo', 'bar')")
            <BLANKLINE>
             [json]
            ----------------------------------------------------------------------------------------------------------------------------------------
             {"col15": true, "col1": "bar", "col3": "127.0.0.1", "col13": -2147483648, "col11": 9.87123121222E7, "col2": "0x0011", "col4": "blarg"}
             {"col15": true, "col1": "bar", "col3": "127.0.0.1", "col13": -2147483648, "col11": 9.87123121222E7, "col2": "0x0011", "col4": "blarg"}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

        Query rows normally and make sure they look ok there too:

            >>> cqlsh_print("SELECT * FROM primitive_type_test")
            <BLANKLINE>
             key1 | col1 | col10      | col11      | col12      | col13       | col14      | col15 | col2   | col3      | col4  | col5                     | col6                                 | col7                                 | col8 | col9
            ------+------+------------+------------+------------+-------------+------------+-------+--------+-----------+-------+--------------------------+--------------------------------------+--------------------------------------+------+----------------------
              bar |  bar | 1234.45678 | 9.8712e+07 | 9.8712e+07 | -2147483648 | 2147483648 |  True | 0x0011 | 127.0.0.1 | blarg | 2011.....................| 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f | bdf5e8ac-a75e-4321-9ac8-938fc9576c4a | bleh | -9223372036854775808
              foo |  bar | 1234.45678 | 9.8712e+07 | 9.8712e+07 | -2147483648 | 2147483648 |  True | 0x0011 | 127.0.0.1 | blarg | 2011.....................| 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f | bdf5e8ac-a75e-4321-9ac8-938fc9576c4a | bleh | -9223372036854775808
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.simple_schema_test)

    def pkey_requirement_test(self):
        """
        Create schema:

            >>> cqlsh('''
            ... CREATE TABLE primitive_type_test (
            ...   key1 text PRIMARY KEY,
            ...   col1 ascii,
            ...   col2 blob,
            ...   col3 inet,
            ...   col4 text,
            ...   col5 timestamp,
            ...   col6 timeuuid,
            ...   col7 uuid,
            ...   col8 varchar,
            ...   col9 bigint,
            ...   col10 decimal,
            ...   col11 double,
            ...   col12 float,
            ...   col13 int,
            ...   col14 varint,
            ...   col15 boolean)
            ... ''')

        Try to create a JSON row with the pkey omitted from the column list, and omitted from the JSON data:

            >>> cqlsh_err_print('''INSERT INTO primitive_type_test JSON '{"col1": "bar"}' ''')
            <stdin>:2:InvalidRequest: code=2200 [Invalid query] message="Invalid null value in condition for column key1"
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.pkey_requirement_test)

    def null_value_test(self):
        """
        Create schema:

            >>> cqlsh('''
            ... CREATE TABLE primitive_type_test (
            ...   key1 text PRIMARY KEY,
            ...   col1 ascii,
            ...   col2 blob,
            ...   col3 inet,
            ...   col4 text,
            ...   col5 timestamp,
            ...   col6 timeuuid,
            ...   col7 uuid,
            ...   col8 varchar,
            ...   col9 bigint,
            ...   col10 decimal,
            ...   col11 double,
            ...   col12 float,
            ...   col13 int,
            ...   col14 varint,
            ...   col15 boolean)
            ... ''')

        Insert a row where all columns are specified in the column list, but none of the non-pkey items are provided in the JSON data:

            >>> cqlsh('''
            ... INSERT INTO primitive_type_test JSON '{"key1": "foo"}'
            ... ''')

        Confirm columns provided in column list but not specified are null:

            >>> cqlsh_print("SELECT * FROM primitive_type_test WHERE key1 = 'foo'")
            <BLANKLINE>
             key1 | col1 | col10 | col11 | col12 | col13 | col14 | col15 | col2 | col3 | col4 | col5 | col6 | col7 | col8 | col9
            ------+------+-------+-------+-------+-------+-------+-------+------+------+------+------+------+------+------+------
              foo | null |  null |  null |  null |  null |  null |  null | null | null | null | null | null | null | null | null
            <BLANKLINE>
            (1 rows)
            <BLANKLINE>
        """
        run_func_docstring(tester=self, test_func=self.null_value_test)

    def complex_schema_test(self):
        """
        Create some udt's and schema:

            >>> cqlsh('''
            ... CREATE TYPE t_todo_item (
            ...   label text,
            ...   details text )
            ... ''')

            >>> cqlsh('''
            ... CREATE TYPE t_todo_list (
            ...   name text,
            ...   todo_list list<frozen<t_todo_item>> )
            ... ''')

            >>> cqlsh('''
            ... CREATE TYPE t_kitchen_sink (
            ...   item1 ascii,
            ...   item2 blob,
            ...   item3 inet,
            ...   item4 text,
            ...   item5 timestamp,
            ...   item6 timeuuid,
            ...   item7 uuid,
            ...   item8 varchar,
            ...   item9 bigint,
            ...   item10 decimal,
            ...   item11 double,
            ...   item12 float,
            ...   item13 int,
            ...   item14 varint,
            ...   item15 boolean,
            ...   item16 list<int> )
            ... ''')

            >>> cqlsh('''
            ... CREATE TABLE complex_types (
            ...   key1 text PRIMARY KEY,
            ...   mylist list<text>,
            ...   myset set<uuid>,
            ...   mymap map<text, int>,
            ...   mytuple frozen<tuple<text, int, uuid, boolean>>,
            ...   myudt frozen<t_kitchen_sink>,
            ...   mytodolists list<frozen<t_todo_list>>,
            ...   many_sinks list<frozen<t_kitchen_sink>>,
            ...   named_sinks map<text, frozen<t_kitchen_sink>> )
            ... ''')

        Add two rows with all null values, create the first row using a regular INSERT statement, and the second row using JSON. Different key for each row:

            >>> cqlsh('''
            ... INSERT INTO complex_types (key1) values ('row1')
            ... ''')
            >>> cqlsh('''
            ... INSERT INTO complex_types JSON '{"key1":"row2"}'
            ... ''')

        Query back both rows as JSON:

            >>> cqlsh_print("SELECT JSON * FROM complex_types")
            <BLANKLINE>
             [json]
            --------------------------------------------------------------------------------------------------------------------------------------------------------------
             {"key1": "row1", "many_sinks": null, "mylist": null, "mymap": null, "myset": null, "mytodolists": null, "mytuple": null, "myudt": null, "named_sinks": null}
             {"key1": "row2", "many_sinks": null, "mylist": null, "mymap": null, "myset": null, "mytodolists": null, "mytuple": null, "myudt": null, "named_sinks": null}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

        Query back both rows as non-JSON to be sure they look ok there too:

            >>> cqlsh_print('''
            ... SELECT * FROM complex_types
            ... ''')
            <BLANKLINE>
             key1 | many_sinks | mylist | mymap | myset | mytodolists | mytuple | myudt | named_sinks
            ------+------------+--------+-------+-------+-------------+---------+-------+-------------
             row1 |       null |   null |  null |  null |        null |    null |  null |        null
             row2 |       null |   null |  null |  null |        null |    null |  null |        null
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

        Add data for "row1" using a normal insert statement to update the record:

            >>> cqlsh('''
            ... INSERT INTO complex_types (key1, mylist, myset, mymap, mytuple, myudt, mytodolists, many_sinks, named_sinks)
            ... VALUES (
            ...   'row1',
            ...   ['five', 'six', 'seven', 'eight'],
            ...   {4b66458a-2a19-41d3-af25-6faef4dea9fe, 080fdd90-ae74-41d6-9883-635625d3b069, 6cd7fab5-eacc-45c3-8414-6ad0177651d6},
            ...   {'one' : 1, 'two' : 2, 'three': 3, 'four': 4},
            ...   ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, true),
            ...   {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011-02-03 04:05+0000', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 98712312.1222, item12: 98712312.5252, item13: -2147483648, item14: 2147483647, item15: false, item16: [1,3,5,7,11,13]},
            ...   [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list:[{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}],
            ...   [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}],
            ...   {'namedsink1':{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012-02-03 04:05+0000', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 10012312.1222, item12: 40012312.5252, item13: -1147483648, item14: 2047483648, item15: true, item16: [1,1,2,3,5,8]},'namedsink2':{item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013-02-03 04:05+0000', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 20012312.1222, item12: 50012312.5252, item13: -1547483648, item14: 1947483648, item15: false, item16: [3,6,9,12,15]},'namedsink3':{item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014-02-03 04:05+0000', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 30012312.1222, item12: 60012312.5252, item13: 2147483647, item14: 1347483648, item15: true, item16: [0,1,0,1,2,0]}})
            ... ''')

        Add data for "row2" using JSON, but which should be equivalent to "row1" after insert:

            >>> cqlsh('''
            ... INSERT INTO complex_types
            ... JSON '{
            ...   "key1":"row2",
            ...   "mylist":["five", "six", "seven", "eight"],
            ...   "myset":["4b66458a-2a19-41d3-af25-6faef4dea9fe", "080fdd90-ae74-41d6-9883-635625d3b069", "6cd7fab5-eacc-45c3-8414-6ad0177651d6"],
            ...   "mymap":{"one" : 1, "two" : 2, "three": 3, "four": 4},
            ...   "mytuple":["hey", 10, "16e69fba-a656-4932-8a01-6782a34505d9", true],
            ...   "myudt":{"item1": "heyimascii", "item2": "0x0011", "item3": "127.0.0.1", "item4": "whatev", "item5": "2011-02-03 04:05+0000", "item6": "0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f", "item7": "bdf5e8ac-a75e-4321-9ac8-938fc9576c4a", "item8": "bleh", "item9": -9223372036854775808, "item10": 1234.45678, "item11": 98712312.1222, "item12": 98712312.5252, "item13": -2147483648, "item14": 2147483647, "item15": false, "item16": [1,3,5,7,11,13]},
            ...   "mytodolists":[{"name": "stuff to do!", "todo_list": [{"label": "buy groceries", "details": "bread and milk"}, {"label": "pick up car from shop", "details": "$325 due"}, {"label": "call dave", "details": "for some reason"}]}, {"name": "more stuff to do!", "todo_list":[{"label": "buy new car", "details": "the old one is getting expensive"}, {"label": "price insurance", "details": "current cost is $95/mo"}]}],
            ...   "many_sinks":[{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]}, {"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}],
            ...   "named_sinks":{"namedsink1":{"item1": "asdf", "item2": "0x0012", "item3": "127.0.0.2", "item4": "whatev1", "item5": "2012-02-03 04:05+0000", "item6": "d05a10c8-7c12-11e4-949d-b4b6763e9d6f", "item7": "f90b04b1-f9ad-4ffa-b869-a7d894ce6003", "item8": "tyru", "item9": -9223372036854771111, "item10": 4321.45678, "item11": 10012312.1222, "item12": 40012312.5252, "item13": -1147483648, "item14": 2047483648, "item15": true, "item16": [1,1,2,3,5,8]},"namedsink2":{"item1": "fdsa", "item2": "0x0013", "item3": "127.0.0.3", "item4": "whatev2", "item5": "2013-02-03 04:05+0000", "item6": "d8ac38c8-7c12-11e4-8955-b4b6763e9d6f", "item7": "e3e84f21-f28c-4e0f-80e0-068a640ae53a", "item8": "uytr", "item9": -3333372036854775808, "item10": 1234.12321, "item11": 20012312.1222, "item12": 50012312.5252, "item13": -1547483648, "item14": 1947483648, "item15": false, "item16": [3,6,9,12,15]},"namedsink3":{"item1": "zxcv", "item2": "0x0014", "item3": "127.0.0.4", "item4": "whatev3", "item5": "2014-02-03 04:05+0000", "item6": "de30838a-7c12-11e4-a907-b4b6763e9d6f", "item7": "f9381f0e-9467-4d4c-9315-eb9f0232487b", "item8": "fghj", "item9": -2239372036854775808, "item10": 5555.55555, "item11": 30012312.1222, "item12": 60012312.5252, "item13": 2147483647, "item14": 1347483648, "item15": true, "item16": [0,1,0,1,2,0]}}
            ...   }'
            ... ''')

        Query both rows back, one field at a time (for easier reading) and make sure they match:

            >>> cqlsh_print("SELECT key1, mylist from complex_types")
            <BLANKLINE>
             key1 | mylist
            ------+-----------------------------------
             row1 | ['five', 'six', 'seven', 'eight']
             row2 | ['five', 'six', 'seven', 'eight']
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, myset from complex_types")
            <BLANKLINE>
             key1 | myset
            ------+--------------------------------------------------------------------------------------------------------------------
             row1 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}
             row2 | {080fdd90-ae74-41d6-9883-635625d3b069, 4b66458a-2a19-41d3-af25-6faef4dea9fe, 6cd7fab5-eacc-45c3-8414-6ad0177651d6}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, mymap from complex_types")
            <BLANKLINE>
             key1 | mymap
            ------+---------------------------------------------
             row1 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}
             row2 | {'four': 4, 'one': 1, 'three': 3, 'two': 2}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, mytuple from complex_types")
            <BLANKLINE>
             key1 | mytuple
            ------+---------------------------------------------------------
             row1 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)
             row2 | ('hey', 10, 16e69fba-a656-4932-8a01-6782a34505d9, True)
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, myudt from complex_types")
            <BLANKLINE>
             key1 | myudt
            ------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011...', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}
             row2 | {item1: 'heyimascii', item2: 0x0011, item3: '127.0.0.1', item4: 'whatev', item5: '2011...', item6: 0ad6dfb6-7a6e-11e4-bc39-b4b6763e9d6f, item7: bdf5e8ac-a75e-4321-9ac8-938fc9576c4a, item8: 'bleh', item9: -9223372036854775808, item10: 1234.45678, item11: 9.8712e+07, item12: 9.8712e+07, item13: -2147483648, item14: 2147483647, item15: False, item16: [1, 3, 5, 7, 11, 13]}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, mytodolists from complex_types")
            <BLANKLINE>
             key1 | mytodolists
            ------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]
             row2 | [{name: 'stuff to do!', todo_list: [{label: 'buy groceries', details: 'bread and milk'}, {label: 'pick up car from shop', details: '$325 due'}, {label: 'call dave', details: 'for some reason'}]}, {name: 'more stuff to do!', todo_list: [{label: 'buy new car', details: 'the old one is getting expensive'}, {label: 'price insurance', details: 'current cost is $95/mo'}]}]
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, many_sinks from complex_types")
            <BLANKLINE>
             key1 | many_sinks
            ------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]
             row2 | [{item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}]
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

            >>> cqlsh_print("SELECT key1, named_sinks from complex_types")
            <BLANKLINE>
             key1 | named_sinks
            ------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
             row1 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}
             row2 | {'namedsink1': {item1: 'asdf', item2: 0x0012, item3: '127.0.0.2', item4: 'whatev1', item5: '2012...', item6: d05a10c8-7c12-11e4-949d-b4b6763e9d6f, item7: f90b04b1-f9ad-4ffa-b869-a7d894ce6003, item8: 'tyru', item9: -9223372036854771111, item10: 4321.45678, item11: 1.0012e+07, item12: 4.0012e+07, item13: -1147483648, item14: 2047483648, item15: True, item16: [1, 1, 2, 3, 5, 8]}, 'namedsink2': {item1: 'fdsa', item2: 0x0013, item3: '127.0.0.3', item4: 'whatev2', item5: '2013...', item6: d8ac38c8-7c12-11e4-8955-b4b6763e9d6f, item7: e3e84f21-f28c-4e0f-80e0-068a640ae53a, item8: 'uytr', item9: -3333372036854775808, item10: 1234.12321, item11: 2.0012e+07, item12: 5.0012e+07, item13: -1547483648, item14: 1947483648, item15: False, item16: [3, 6, 9, 12, 15]}, 'namedsink3': {item1: 'zxcv', item2: 0x0014, item3: '127.0.0.4', item4: 'whatev3', item5: '2014...', item6: de30838a-7c12-11e4-a907-b4b6763e9d6f, item7: f9381f0e-9467-4d4c-9315-eb9f0232487b, item8: 'fghj', item9: -2239372036854775808, item10: 5555.55555, item11: 3.0012e+07, item12: 6.0012e+07, item13: 2147483647, item14: 1347483648, item15: True, item16: [0, 1, 0, 1, 2, 0]}}
            <BLANKLINE>
            (2 rows)
            <BLANKLINE>

        """
        run_func_docstring(tester=self, test_func=self.complex_schema_test)
