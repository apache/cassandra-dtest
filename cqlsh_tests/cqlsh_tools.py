from __future__ import unicode_literals

import csv
import random
import six

import cassandra

from cassandra.cluster import ResultSet


class DummyColorMap(object):

    def __getitem__(self, *args):
        return ''


def csv_rows(filename, delimiter=None):
    """
    Given a filename, opens a csv file and yields it line by line.
    """
    reader_opts = {}
    if delimiter is not None:
        reader_opts['delimiter'] = delimiter
    with open(filename, 'r') as csvfile:
        for row in csv.reader(csvfile, **reader_opts):
            yield [unicode(field, encoding='utf-8') for field in row] if six.PY2 else row


def assert_csvs_items_equal(filename1, filename2):
    with open(filename1, 'r') as x, open(filename2, 'r') as y:
        list_x = list(x.readlines())
        list_y = list(y.readlines())
        list_x.sort()
        list_y.sort()
        assert list_x == list_y


def random_list(gen=None, n=None):
    if gen is None:
        def gen():
            return random.randint(-1000, 1000)
    if n is None:
        def length():
            return random.randint(1, 5)
    else:
        def length():
            return n

    return [gen() for _ in range(length())]


def write_rows_to_csv(filename, data):
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)
        csvfile.close


def deserialize_date_fallback_int(byts, protocol_version):
    timestamp_ms = cassandra.marshal.int64_unpack(byts)
    try:
        return cassandra.util.datetime_from_timestamp(timestamp_ms / 1000.0)
    except OverflowError:
        return timestamp_ms


def monkeypatch_driver():
    """
    Monkeypatches the `cassandra` driver module in the same way
    that clqsh does. Returns a dictionary containing the original values of
    the monkeypatched names.
    """
    cache = {'BytesType_deserialize': cassandra.cqltypes.BytesType.deserialize,
             'DateType_deserialize': cassandra.cqltypes.DateType.deserialize,
             'support_empty_values': cassandra.cqltypes.CassandraType.support_empty_values}

    cassandra.cqltypes.BytesType.deserialize = staticmethod(lambda byts, protocol_version: bytearray(byts))
    cassandra.cqltypes.DateType.deserialize = staticmethod(deserialize_date_fallback_int)
    cassandra.cqltypes.CassandraType.support_empty_values = True

    if hasattr(cassandra, 'deserializers'):
        cache['DesDateType'] = cassandra.deserializers.DesDateType
        del cassandra.deserializers.DesDateType

    return cache


def unmonkeypatch_driver(cache):
    """
    Given a dictionary that was used to cache parts of `cassandra` for
    monkeypatching, restore those values to the `cassandra` module.
    """
    cassandra.cqltypes.BytesType.deserialize = staticmethod(cache['BytesType_deserialize'])
    cassandra.cqltypes.DateType.deserialize = staticmethod(cache['DateType_deserialize'])
    cassandra.cqltypes.CassandraType.support_empty_values = cache['support_empty_values']

    if hasattr(cassandra, 'deserializers'):
        cassandra.deserializers.DesDateType = cache['DesDateType']


# assert_resultset_contains uses the typing module, which has different syntax in Python 2 vs. Python 3
if six.PY3:
    from .cqlsh_tools_py3 import assert_resultset_contains
elif six.PY2:
    from cqlsh_tools_py2 import assert_resultset_contains
