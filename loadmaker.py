#!/usr/bin/env python

import inspect
import os
import pickle
import platform
import re
import sys
import tempfile
import time
import uuid
import pprint

#try:
import pycassa
import pycassa.system_manager as system_manager
#except ImportError:
#    pass


class LoadMaker(object):
    """
    Sends data to cassandra, then loads it back to make sure
    it comes back right. You can send data multiple times, and read
    back any range of data that has been entered.

    How I will use this:
        I'll start a thread with multiple LoadMakers. While they are
        running, I'll upgrade the first node. When the upgrade is done,
        I'll read back what they inserted to make sure it is all right.
        Then I'll turn them to insert mode and upgrade the next node, etc.
        After all the nodes are upgraded, I'll read back all the data
        to make sure it is right. I won't ever delete anything.
    """

    _DEFAULTS = {
        'keyspace_name': 'Keyspace_lm',
        'keyspace_strategy_options': {'replication_factor': '1'},

        'column_family_name': None, # needs to be overwritten for the moment
        'consistency_level': 'QUORUM',
        'column_family_type': 'standard',
        'validation_type': 'UTF8Type',
        'comparator_type': 'UTF8Type',
        'subcomparator_type': 'UTF8Type',
        'key_validation_type': 'UTF8Type',
        'num_cols': 5,
        'num_subcols': 5,
    }

    def __init__(self, **kwargs):
        self._params = LoadMaker._DEFAULTS.copy()

        # allow for overwriting any of the defaults
        for key, value in kwargs.items():
            if key not in self._params.keys():
                raise AttributeError("%s is an illegal arguement!" % key)
            self._params[key] = value

        print self._params['consistency_level']
        # validate the consistency_level
        self._params['consisteny_level'] = getattr(
                pycassa.cassandra.ttypes.ConsistencyLevel,
                self._params['consistency_level'])
        print self._params['consistency_level']
        self._inserted_key_count = 0

        self.create_keyspace()
        self.create_column_family()

    def generate(self, num_keys=10000, server_list=['localhost:9160']):
        """
        Generates a bunch of data and inserts it into cassandra
        """
        column_family_type = self._params['column_family_type']
        validation_type = self._params['validation_type']
        comparator_type = self._params['comparator_type']
        subcomparator_type = self._params['subcomparator_type']
        key_validation_type = self._params['key_validation_type']

        num_cols = self._params['num_cols']
        num_subcols = self._params['num_subcols']

        rows = self._gen_rows(self._inserted_key_count,
                self._inserted_key_count + num_keys)

        pool = self._get_pool(server_list=server_list)
        cf = pycassa.ColumnFamily(pool, self._params['column_family_name'])
        print cf

        pprint.pprint(rows)
        sm = system_manager.SystemManager()
        pprint.pprint(sm.get_keyspace_column_families(self._params['keyspace_name']))
        print self._params['consistency_level']
        cf.batch_insert(rows, write_consistency_level=self._params['consistency_level'])

    # TODO: Improve the type and increase the amount of validation performed here
    def validate(self, start_index, end_index, server_list=['localhost:9160']):
        """
        gets the rows from start_index (inclusive) to end_index (exclusive) and
        compares them against what they are supposed to be. If end_index is
        greater then what has been inserted an exception is thrown.
        """

        assert(start_index < end_index)
        assert(end_index <= self.inserted_key_count)

        column_family_type = self._params['column_family_type']
        validation_type = self._params['validation_type']
        comparator_type = self._params['comparator_type']
        subcomparator_type = self._params['subcomparator_type']
        key_validation_type = self._params['key_validation_type']

        num_cols = self._params['num_cols']
        num_subcols = self._params['num_subcols']

        cl = getattr(pycassa.cassandra.ttypes.ConsistencyLevel, directives['consistency_level'])

        pool = _get_pool(server_list=server_list)
        cf = pycassa.ColumnFamily(pool, column_family_name)

        read_rows = cf.multiget(rows.keys())

        if len(list(read_rows)) < len(rows):
            raise Exception("number of rows (%s) doesn't match expected number (%s)" % (str(len(read_rows)), str(len(rows))))

        counts = cf.multiget_count(rows.keys())

        for count in counts.values():
            if count != num_cols:
                raise Exception("counted columns (%s) doesn't match expected number (%s)" % (str(count), str(num_cols)))


    def _gen_rows(self, start_index, end_index):
        """
        Generates a bunch of rows from start_index (inclusive) to end_index (exclusive).
        """
        rows = dict()
        for row_num in xrange(start_index, end_index):
            if self._params['column_family_type'].lower() == 'super':
                sub_cols = dict((
                        self._generate_col_name(self._params['subcomparato_typer'], i),
                        self._generate_col_value(self._params['validation_type'], i)
                        ) for i in xrange(self._params['num_subcols']))
                cols = dict((
                        self._generate_col_name(self._params['comparator'], i),
                        sub_cols) for i in xrange(num_cols))
            else:
                cols = dict((
                        self._generate_col_name(self._params['comparator_type'], i),
                        self._generate_col_value(self._params['validation_type'], i))
                        for i in xrange(self._params['num_cols']))

            rows[self._generate_row_key(self._params['key_validation_type'], row_num)] = cols

        return rows

    def _generate_row_key(self, key_validator, num):
        return self._convert(key_validator, prefix='row_', num=num)

    def _generate_col_name(self, subcomparator, num):
        return self._convert(subcomparator, prefix='col_', num=num)

    def _generate_col_value(self, validator, num):
        return self._convert(validator, prefix='val_', num=num)

    def _convert(self, target_type, prefix=None, num=None):
        if target_type in ('AsciiType', 'BytesType'):
            return str(prefix) + str(num)

        if target_type == 'UTF8Type':
            return unicode(prefix + str(num), 'utf8')

        if target_type in ('IntegerType'):
            return int(num)

        if target_type == 'LongType':
            return long(num)

#        if target_type == 'LexicalUUIDType':
#            return uuid.uuid1()

#        if target_type == 'TimeUUIDType':
#            return uuid.uuid1()

        if target_type == 'CounterColumnType':
            return int(num)

    def _get_pool(self, timeout=30, server_list=['localhost:9160']):
        return pycassa.ConnectionPool(self._params['keyspace_name'], timeout=timeout,
                server_list=server_list)

    def _get_consistency_level_type(self, cl):
        return getattr(pycassa.cassandra.ttypes.ConsistencyLevel, cl)


    def create_keyspace(self):
        keyspace_name = self._params['keyspace_name']

        sm = system_manager.SystemManager()

        keyspaces = sm.list_keyspaces()
        if keyspace_name not in keyspaces:
            sm.create_keyspace(keyspace_name, strategy_options=
                    self._params['keyspace_strategy_options'])

        sm.close()


    def create_column_family(self):
        sm = system_manager.SystemManager()
        # sm.create_column_family prints to stdout, which
        # we don't want.
        # We redirect it temporarily here.
        sys.stdout = open(os.devnull, 'w')
        sm.create_column_family(
            self._params['keyspace_name'], self._params['column_family_name']
        )
        sys.stdout = sys.__stdout__
        sm.close()














