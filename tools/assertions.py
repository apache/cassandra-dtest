import re
from time import sleep
from tools.misc import list_to_hashed_dict

from cassandra import (InvalidRequest, ReadFailure, ReadTimeout, Unauthorized,
                       Unavailable, WriteFailure, WriteTimeout)
from cassandra.query import SimpleStatement


"""
The assertion methods in this file are used to structure, execute, and test different queries and scenarios.
Use these anytime you are trying to check the content of a table, the row count of a table, if a query should
raise an exception, etc. These methods handle error messaging well, and will help discovering and treating bugs.

An example:
Imagine some table, test:

    id | name
    1  | John Doe
    2  | Jane Doe

We could assert the row count is 2 by using:
    assert_row_count(session, 'test', 2)

After inserting [3, 'Alex Smith'], we can ensure the table is correct by:
    assert_all(session, "SELECT * FROM test", [[1, 'John Doe'], [2, 'Jane Doe'], [3, 'Alex Smith']])
or we could check the insert was successful:
    assert_one(session, "SELECT * FROM test WHERE id = 3", [3, 'Alex Smith'])

We could remove all rows in test, and assert this was sucessful with:
    assert_none(session, "SELECT * FROM test")

Perhaps we want to assert invalid queries will throw an exception:
    assert_invalid(session, "SELECT FROM test")
or, maybe after shutting down all the nodes, we want to assert an Unavailable exception is raised:
    assert_unavailable(session.execute, "SELECT * FROM test")
    OR
    assert_exception(session, "SELECT * FROM test", expected=Unavailable)

"""


def _rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def _assert_exception(fun, *args, **kwargs):
    matching = kwargs.pop('matching', None)
    expected = kwargs['expected']
    try:
        if len(args) == 0:
            fun(None)
        else:
            fun(*args)
    except expected as e:
        if matching is not None:
            regex = re.compile(matching)
            assert regex.match(repr(e)) is None
    except Exception as e:
        raise e
    else:
        assert False, "Expecting query to raise an exception, but nothing was raised."


def assert_exception(session, query, matching=None, expected=None):
    if expected is None:
        assert False, "Expected exception should not be None. Your test code is wrong, please set `expected`."

    _assert_exception(session.execute, query, matching=matching, expected=expected)


def assert_unavailable(fun, *args):
    """
    Attempt to execute a function, and assert Unavailable, WriteTimeout, WriteFailure,
    ReadTimeout, or ReadFailure exception is raised.
    @param fun Function to be executed
    @param *args Arguments to be passed to the function

    Examples:
    assert_unavailable(session2.execute, "SELECT * FROM ttl_table;")
    assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)
    """
    _assert_exception(fun, *args, expected=(Unavailable, WriteTimeout, WriteFailure, ReadTimeout, ReadFailure))


def assert_invalid(session, query, matching=None, expected=InvalidRequest):
    """
    Attempt to issue a query and assert that the query is invalid.
    @param session Session to use
    @param query Invalid query to run
    @param matching Optional error message string contained within expected exception
    @param expected Exception expected to be raised by the invalid query

    Examples:
    assert_invalid(session, 'DROP USER nonexistent', "nonexistent doesn't exist")
    """
    assert_exception(session, query, matching=matching, expected=expected)


def assert_unauthorized(session, query, message):
    """
    Attempt to issue a query, and assert Unauthorized is raised.
    @param session Session to use
    @param query Unauthorized query to run
    @param message Expected error message

    Examples:
    assert_unauthorized(session, "ALTER USER cassandra NOSUPERUSER",
                        "You aren't allowed to alter your own superuser status")
    assert_unauthorized(cathy, "ALTER TABLE ks.cf ADD val int",
                        "User cathy has no ALTER permission on <table ks.cf> or any of its parents")
    """
    assert_exception(session, query, matching=message, expected=Unauthorized)


def assert_one(session, query, expected, cl=None):
    """
    Assert query returns one row.
    @param session Session to use
    @param query Query to run
    @param expected Expected results from query
    @param cl Optional Consistency Level setting. Default ONE

    Examples:
    assert_one(session, "LIST USERS", ['cassandra', True])
    assert_one(session, query, [0, 0])
    """
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = _rows_to_list(res)
    assert list_res == [expected], "Expected {} from {}, but got {}".format([expected], query, list_res)


def assert_none(session, query, cl=None):
    """
    Assert query returns nothing
    @param session Session to use
    @param query Query to run
    @param cl Optional Consistency Level setting. Default ONE

    Examples:
    assert_none(self.session1, "SELECT * FROM test where key=2;")
    assert_none(cursor, "SELECT * FROM test WHERE k=2", cl=ConsistencyLevel.SERIAL)
    """
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query)
    list_res = _rows_to_list(res)
    assert list_res == [], "Expected nothing from {}, but got {}".format(query, list_res)


def assert_some(session, query, cl=None, execution_profile=None):
    """
    Assert query returns something
    @param session Session to use
    @param query Query to run
    @param cl Optional Consistency Level setting. Default ONE
     Examples:
    assert_some(self.session1, "SELECT * FROM test where key=2;")
    assert_some(cursor, "SELECT * FROM test WHERE k=2", cl=ConsistencyLevel.SERIAL)
    """
    res = _execute(session, query, cl=cl, execution_profile=execution_profile)
    list_res = _rows_to_list(res)
    assert list_res != [], "Expected something from {}, but got {}".format(query, list_res)


def assert_all(session, query, expected, cl=None, ignore_order=False, timeout=None):
    """
    Assert query returns all expected items optionally in the correct order
    @param session Session in use
    @param query Query to run
    @param expected Expected results from query
    @param cl Optional Consistency Level setting. Default ONE
    @param ignore_order Optional boolean flag determining whether response is ordered
    @param timeout Optional query timeout, in seconds

    Examples:
    assert_all(session, "LIST USERS", [['aleksey', False], ['cassandra', True]])
    assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
    """
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = session.execute(simple_query) if timeout is None else session.execute(simple_query, timeout=timeout)
    list_res = _rows_to_list(res)
    if ignore_order:
        expected = list_to_hashed_dict(expected)
        list_res = list_to_hashed_dict(list_res)
    assert list_res == expected, "Expected {} from {}, but got {}".format(expected, query, list_res)


def assert_almost_equal(*args, **kwargs):
    """
    Assert variable number of arguments all fall within a margin of error.
    @params *args variable number of numerical arguments to check
    @params error Optional margin of error. Default 0.16
    @params error_message Optional error message to print. Default ''

    Examples:
    assert_almost_equal(sizes[2], init_size)
    assert_almost_equal(ttl_session1, ttl_session2[0][0], error=0.005)
    """
    error = kwargs['error'] if 'error' in kwargs else 0.16
    vmax = max(args)
    vmin = min(args)
    error_message = '' if 'error_message' not in kwargs else kwargs['error_message']
    assert vmin > vmax * (1.0 - error) or vmin == vmax, \
        "values not within {:.2f}% of the max: {} ({})".format(error * 100, args, error_message)


def assert_row_count(session, table_name, expected, where=None):
    """
    Assert the number of rows in a table matches expected.
    @param session Session to use
    @param table_name Name of the table to query
    @param expected Number of rows expected to be in table
    @param where string to append to CQL select query as where clause
    Examples:
    assert_row_count(self.session1, 'ttl_table', 1)
    """
    if where is not None:
        query = "SELECT count(*) FROM {} WHERE {};".format(table_name, where)
    else:
        query = "SELECT count(*) FROM {};".format(table_name)
    res = session.execute(query)
    count = res[0][0]
    assert count == expected, "Expected a row count of {} in table '{}', but got {}".format(
        expected, table_name, count
    )


def assert_crc_check_chance_equal(session, table, expected, ks="ks", view=False):
    """
    Assert crc_check_chance equals expected for a given table or view
    @param session Session to use
    @param table Name of the table or view to check
    @param expected Expected value to assert on that query result matches
    @param ks Optional Name of the keyspace
    @param view Optional Boolean flag indicating if the table is a view

    Examples:
    assert_crc_check_chance_equal(session, "compression_opts_table", 0.25)
    assert_crc_check_chance_equal(session, "t_by_v", 0.5, view=True)

    driver still doesn't support top-level crc_check_chance property,
    so let's fetch directly from system_schema
    """
    if view:
        assert_one(session,
                   "SELECT crc_check_chance from system_schema.views WHERE keyspace_name = '{keyspace}' AND "
                   "view_name = '{table}';".format(keyspace=ks, table=table),
                   [expected])
    else:
        assert_one(session,
                   "SELECT crc_check_chance from system_schema.tables WHERE keyspace_name = '{keyspace}' AND "
                   "table_name = '{table}';".format(keyspace=ks, table=table),
                   [expected])


def assert_length_equal(object_with_length, expected_length):
    """
    Assert an object has a specific length.
    @param object_with_length The object whose length will be checked
    @param expected_length The expected length of the object

    Examples:
    assert_length_equal(res, nb_counter)
    """
    assert len(object_with_length) == expected_length, \
        "Expected {} to have length {}, but instead is of length {}"\
        .format(object_with_length, expected_length, len(object_with_length))


def assert_not_running(node):
    """
    Assert that a given node is not running
    @param node The node to check status
    """
    attempts = 0
    while node.is_running() and attempts < 10:
        sleep(1)
        attempts = attempts + 1

    assert not node.is_running()


def assert_read_timeout_or_failure(session, query):
    assert_exception(session, query, expected=(ReadTimeout, ReadFailure))


def assert_stderr_clean(err, acceptable_errors=None):
    """
    Assert that stderr is empty or that it only contains harmless messages
    @param err The stderr to clean
    @param acceptable_errors A list that if used, the user chooses what
                             messages are to be acceptable in stderr.
    """
    if acceptable_errors is None:
        acceptable_errors = ["WARN.*JNA link failure.*unavailable.",
                             "objc.*Class JavaLaunchHelper.*?Which one is undefined.",
                             # Stress tool JMX connection failure, see CASSANDRA-12437
                             "Failed to connect over JMX; not collecting these stats",
                             "Picked up JAVA_TOOL_OPTIONS:.*",
                             # Warnings for backward compatibility should be logged CASSANDRA-15234
                             ".*parameters have been deprecated. They have new names and/or value format; "
                             + "For more information, please refer to NEWS.txt*"]

    regex_str = r"^({}|\s*|\n)*$".format("|".join(acceptable_errors))
    err_str = err.strip()
    # empty string, as good as we can get for a clean stderr output!
    if not err_str:
        return

    match = re.search(regex_str, err_str)

    assert match, "Attempted to check that stderr was empty. Instead, stderr is {}, but the regex used to check " \
                  "stderr is {}".format(err_str, regex_str)


def assert_bootstrap_state(tester, node, expected_bootstrap_state, user=None, password=None):
    """
    Assert that a node is on a given bootstrap state
    @param tester The dtest.Tester object to fetch the exclusive connection to the node
    @param node The node to check bootstrap state
    @param expected_bootstrap_state Bootstrap state to expect
    @param user To connect as for authenticated nodes
    @param password for corresponding user

    Examples:
    assert_bootstrap_state(self, node3, 'COMPLETED')
    """
    session = tester.patient_exclusive_cql_connection(node, user=user, password=password)
    assert_one(session, "SELECT bootstrapped FROM system.local WHERE key='local'", [expected_bootstrap_state])
    session.shutdown()


def assert_lists_equal_ignoring_order(list1, list2, sort_key=None):
    """
    asserts that the contents of the two provided lists are equal
    but ignoring the order that the items of the lists are actually in
    :param list1: list to check if it's contents are equal to list2
    :param list2: list to check if it's contents are equal to list1
    :param sort_key: if the contents of the list are of type dict, the
    key to use of each object to sort the overall object with
    """
    normalized_list1 = []
    for obj in list1:
        normalized_list1.append(obj)

    normalized_list2 = []
    for obj in list2:
        normalized_list2.append(obj)

    if not sort_key:
        sorted_list1 = sorted(normalized_list1, key=lambda elm: elm[0])
        sorted_list2 = sorted(normalized_list2, key=lambda elm: elm[0])
    else:
        # first always sort by "id"
        # that way we get a two factor sort which will increase the chance of ordering lists exactly the same
        if not sort_key == 'id' and 'id' in list1[0].keys():
            sorted_list1 = sorted(sorted(normalized_list1, key=lambda elm: elm["id"]), key=lambda elm: elm[sort_key])
            sorted_list2 = sorted(sorted(normalized_list2, key=lambda elm: elm["id"]), key=lambda elm: elm[sort_key])
        else:
            if isinstance(list1[0]['id'], (int, float)):
                sorted_list1 = sorted(normalized_list1, key=lambda elm: elm[sort_key])
                sorted_list2 = sorted(normalized_list2, key=lambda elm: elm[sort_key])
            else:
                sorted_list1 = sorted(normalized_list1, key=lambda elm: str(elm[sort_key]))
                sorted_list2 = sorted(normalized_list2, key=lambda elm: str(elm[sort_key]))

    assert sorted_list1 == sorted_list2


def assert_lists_of_dicts_equal(list1, list2):
    for adict, bdict in zip(list1, list2):
        assert(len(adict) == len(bdict))
        for key, value in adict.items():
            assert key in bdict
            assert bdict[key] == value
