import re

def assert_unavailable(fun, *args):
    import cql
    try:
        if len(args) == 0:
            fun(None)
        else:
            fun(*args)
    except cql.OperationalError as e:
        msg = str(e)
        assert re.search('one or more nodes were unavailable', msg), "Expecting unavailable exception, got: " + msg
    except Exception as e:
        assert False, "Expecting unavailable exception, got: " + str(e)
    else:
        assert False, "Expecting unavailable exception but no exception was raised"

def assert_almost_equal(v1, v2, error=0.1):
    if v1 != v2:
        vmax = max(v1, v2)
        vmin = min(v1, v2)
        assert vmin > vmax * (1.0 - error), str(v1) + " !~ " + str(v2)
