import datetime
import logging
import re

from collections import namedtuple

from cassandra.util import SortedSet

logger = logging.getLogger(__name__)


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


def drop_microseconds(val):
    """
    For COPY TO, we need to round microsecond to milliseconds because server side
    TimestampSerializer.dateStringPatterns only parses milliseconds. If we keep microseconds,
    users may try to import with COPY FROM a file generated with COPY TO and have problems if
    prepared statements are disabled, see CASSANDRA-11631.
    """
    def drop_micros(m):
        return m.group(0)[:12] + '+'

    # Matches H:MM:SS.000000+ and drops the last 3 digits before the +
    ret = re.sub('\\d{2}\\:\\d{2}\\:\\d{2}\\.(\\d{6})\\+', drop_micros, val)
    logger.debug("Rounded microseconds: {} -> {}".format(val, ret))
    return ret


class Datetime(datetime.datetime):
    """
    Extend standard datetime.datetime class with cql formatting.
    This could be cleaner if this class was declared inside TestCqlshCopy, but then pickle
    wouldn't have access to the class.
    """
    def __new__(cls, year, month, day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None,
                time_format='%Y-%m-%d %H:%M:%S%z', round_timestamp=True):
        self = datetime.datetime.__new__(cls, year, month, day, hour, minute, second, microsecond, tzinfo)
        self.default_time_format = time_format
        self.round_timestamp = round_timestamp
        return self

    def __repr__(self):
        return self._format_for_csv()

    def __str__(self):
        return self._format_for_csv()

    def _format_for_csv(self):
        ret = self.strftime(self.default_time_format)
        return drop_microseconds(ret) if self.round_timestamp else ret

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
