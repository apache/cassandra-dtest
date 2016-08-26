import time


class get_rate_limited_function(object):
    """
    Close over a function and a time limit in seconds. The resulting object can
    be called like the function, but will not delegate to the function if that
    function was called through the object in the time limit.

    Clients can ignore the time limit by calling the function directly as the
    func attribute of the object.
    """
    def __init__(self, func, limit):
        self.func, self.limit = func, limit
        self.last_called = False

    def __call__(self, *args, **kwargs):
        elapsed = time.time() - self.last_called
        if elapsed >= self.limit:
            self.last_called = time.time()
            return self.func(*args, **kwargs)

    def __repr__(self):
        return '{cls_name}(func={func}, limit={limit}, last_called={last_called})'.format(
            cls_name=self.__class__.__name__,
            func=self.func,
            limit=self.limit,
            last_called=self.last_called,
        )


def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
