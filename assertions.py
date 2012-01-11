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

def assert_almost_equal(*args, **kwargs):
    try:
        error = kwargs['error']
    except KeyError:
        error = 0.13

    vmax = max(args)
    vmin = min(args)
    assert vmin > vmax * (1.0 - error), "values not within %.2f%% of the max: %s" % (error * 100, args)
