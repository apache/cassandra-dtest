import re, cql
from cassandra import InvalidRequest, Unavailable
import pdb

def assert_unavailable(fun, *args):
    try:
        if len(args) == 0:
            fun(None)
        else:
            fun(*args)
    except Unavailable as e:
        msg = str(e)
        assert re.search('[Unavailable exception]', msg), "Expecting unavailable exception, got: " + msg
    except Exception as e:
        assert False, "Expecting unavailable exception, got: " + str(e)
    else:
        assert False, "Expecting unavailable exception but no exception was raised"

def assert_invalid(session, query, matching = None):
    try:
        res = session.execute(query)
        assert False, "Expecting query to be invalid: got %s" % res
    except InvalidRequest as e:
        msg = str(e)
        if matching is not None:
            assert re.search(matching, msg), "Error message does not contain " + matching + " (error = " + msg + ")"