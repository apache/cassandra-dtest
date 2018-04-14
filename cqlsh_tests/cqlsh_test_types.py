import datetime
import sys

from collections import namedtuple
from contextlib import contextmanager

from cassandra.util import SortedSet


@contextmanager
def _cqlshlib(cqlshlib_path):
    """
    Returns the cqlshlib module found at the specified path.
    """
    # This method accomplishes its goal by manually adding the library to
    # sys.path, returning the module, then restoring the old path once the
    # context manager exits. This isn't great for maintainability and should
    # be replaced if cqlshlib is made easier to interact with.
    saved_path = list(sys.path)

    try:
        sys.path = sys.path + [cqlshlib_path]
        import cqlshlib
        yield cqlshlib
    finally:
        sys.path = saved_path


def maybe_quote(s):
    """
    Return a quoted string representation for strings, unicode and date time parameters,
    otherwise return a string representation of the parameter.
    """
    return "'{}'".format(s) if isinstance(s, (str, Datetime)) else str(s)


class Address(namedtuple('Address', ('name', 'number', 'street', 'phones'))):
    __slots__ = ()

    def __repr__(self):
        phones_str = "{{{}}}".format(', '.join(maybe_quote(p) for p in sorted(self.phones)))
        return "{{name: {}, number: {}, street: '{}', phones: {}}}".format(self.name,
                                                                           self.number,
                                                                           self.street,
                                                                           phones_str)

    def __str__(self):
        phones_str = "{{{}}}".format(', '.join(maybe_quote(p) for p in sorted(self.phones)))
        return "{{name: {}, number: {}, street: '{}', phones: {}}}".format(self.name,
                                                                           self.number,
                                                                           self.street,
                                                                           phones_str)


class Datetime(datetime.datetime):
    """
    Extend standard datetime.datetime class with cql formatting.
    This could be cleaner if this class was declared inside TestCqlshCopy, but then pickle
    wouldn't have access to the class.
    """
    def __new__(cls, year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None, cqlshlib_path=None):
        self = datetime.datetime.__new__(cls, year, month, day, hour, minute, second, microsecond, tzinfo)
        if (cqlshlib_path is not None):
            with _cqlshlib(cqlshlib_path) as cqlshlib:
                from cqlshlib.formatting import DEFAULT_TIMESTAMP_FORMAT, round_microseconds
                self.default_time_format = DEFAULT_TIMESTAMP_FORMAT
                self.round_microseconds = round_microseconds
        else:
            self.default_time_format = '%Y-%m-%d %H:%M:%S%z'
        return self

    def __repr__(self):
        return self._format_for_csv()

    def __str__(self):
        return self._format_for_csv()

    def _format_for_csv(self):
        ret = self.strftime(self.default_time_format)
        return self.round_microseconds(ret) if self.round_microseconds else ret


class ImmutableDict(frozenset):
    """
    Immutable dictionary implementation to represent map types.
    We need to pass BoundStatement.bind() a dict() because it calls iteritems(),
    except we can't create a dict with another dict as the key, hence we use a class
    that adds iteritems to a frozen set of tuples (which is how dict are normally made
    immutable in python).
    Must be declared in the top level of the module to be available for pickling.
    """
    iteritems = frozenset.__iter__

    def items(self):
        for k, v in self.iteritems():
            yield k, v

    def __repr__(self):
        return '{{{}}}'.format(', '.join(['{0}: {1}'.format(maybe_quote(k), maybe_quote(v)) for k, v in sorted(self.items())]))


class ImmutableSet(SortedSet):

    def __repr__(self):
        return '{{{}}}'.format(', '.join([maybe_quote(t) for t in sorted(self._items)]))

    def __str__(self):
        return '{{{}}}'.format(', '.join([maybe_quote(t) for t in sorted(self._items)]))

    def __hash__(self):
        return hash(tuple([e for e in self]))


class Name(namedtuple('Name', ('firstname', 'lastname'))):
    __slots__ = ()

    def __repr__(self):
        return "{{firstname: '{}', lastname: '{}'}}".format(self.firstname, self.lastname)

    def __str__(self):
        return "{{firstname: '{}', lastname: '{}'}}".format(self.firstname, self.lastname)


class UTC(datetime.tzinfo):
    """
    A utility class to specify a UTC timezone.
    """

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)
