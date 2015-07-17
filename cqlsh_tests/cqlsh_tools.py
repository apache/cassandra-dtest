import csv
import datetime
import random
import time

import cassandra
from nose.tools import assert_items_equal


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
    with open(filename, 'rb') as csvfile:
        for row in csv.reader(csvfile, **reader_opts):
            yield row


def strip_timezone_if_time_string(s):
    try:
        time_string_no_tz = s[:19]
        time_struct = time.strptime(time_string_no_tz, '%Y-%m-%d %H:%M:%S')
        dt_no_timezone = datetime.datetime(*time_struct[:6])
        return dt_no_timezone.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return s


def assert_csvs_items_equal(filename1, filename2):
    with open(filename1, 'r') as x, open(filename2, 'r') as y:
        assert_items_equal(list(x.readlines()), list(y.readlines()))


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
    with open(filename, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)
        csvfile.close


def monkeypatch_driver():
    """
    Monkeypatches the `cassandra` driver module in the same way
    that clqsh does. Returns a dictionary containing the original values of
    the monkeypatched names.
    """
    cache = {'deserialize': cassandra.cqltypes.BytesType.deserialize,
             'support_empty_values': cassandra.cqltypes.CassandraType.support_empty_values}

    cassandra.cqltypes.BytesType.deserialize = staticmethod(lambda byts, protocol_version: bytearray(byts))
    cassandra.cqltypes.CassandraType.support_empty_values = True

    return cache


def unmonkeypatch_driver(cache):
    """
    Given a dictionary that was used to cache parts of `cassandra` for
    monkeypatching, restore those values to the `cassandra` module.
    """
    cassandra.cqltypes.BytesType.deserialize = staticmethod(cache['deserialize'])
    cassandra.cqltypes.CassandraType.support_empty_values = cache['support_empty_values']
