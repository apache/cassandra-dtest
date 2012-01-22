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
import logging

import pycassa
import pycassa.system_manager as system_manager

logger = logging.getLogger('input')
logger.setLevel(logging.INFO)

class LoadMaker(object):
    """
    Allows you to send data to cassandra multiple times using the generate()
    function. Then using the validate() function you can load it back and
    verify that what comes back is what was sent.

    Defaults are provided in _DEFAULTS, and each can be overwritten
    by passing parameters with the same name to the constructor.
    """

    _DEFAULTS = {
        'keyspace_name': 'Keyspace_lm',
        'keyspace_strategy_options': {'replication_factor': '3'},

        'is_counter': False,
        'column_family_name': None, # needs to be overwritten for the moment
        'consistency_level': 'QUORUM',
        'column_family_type': 'standard',
        'validation_type': 'UTF8Type',
        'comparator_type': 'UTF8Type',
        'subcomparator_type': 'UTF8Type', # only applies to super columns
        'key_validation_type': 'UTF8Type',
        'num_cols': 5,
        'num_subcols': 5, # only applies to super columns
        'num_counter_rows': 100, # only applies to counter columns
    }

    def __init__(self, **kwargs):
        self._params = LoadMaker._DEFAULTS.copy()

        # allow for overwriting any of the defaults
        for key, value in kwargs.items():
            if key not in self._params.keys():
                raise AttributeError("%s is an illegal arguement!" % key)
            self._params[key] = value

        # validate the consistency_level
        self._params['consistency_level'] = getattr(
                pycassa.cassandra.ttypes.ConsistencyLevel,
                self._params['consistency_level'])

        self._inserted_key_count = 0
        
        self.create_keyspace()
        self.create_column_family()
    
    def __str__(self):
        return "LoadMaker<" + str(self._params) + ">"

    def generate(self, num_keys=10000, server_list=['localhost:9160']):
        """
        Generates a bunch of data and inserts it into cassandra
        """
        new_inserted_key_count = self._inserted_key_count + num_keys

        pool = self._get_pool(server_list=server_list)
        cf = pycassa.ColumnFamily(pool, self._params['column_family_name'])

        if self._params['is_counter']:
            self._generate_counter(num_keys, cf)
        else:
            rows = self._gen_rows(self._inserted_key_count, new_inserted_key_count)
            cf.batch_insert(rows, write_consistency_level=self._params['consistency_level'])

        self._inserted_key_count = new_inserted_key_count
        

    def _iterate_over_counter_columns(self, num_keys, func):
        """
        calls func on every column of the counter column family.

        func should have a signature like this:
        func(row_key, col_name)
        """
        for row_index in xrange(num_keys):
            row_key = self._generate_row_key(row_index)
            for col_index in xrange(self._params['num_cols']):
                col_name = self._generate_col_name(
                        self._params['comparator_type'], col_index)
                    func(row_key, col_name)


    def _generate_counter(self, num_keys, column_family):
        """
        increments all counters. There are num_keys counter rows, 
        each with self._params['num_cols'] individual counters.
        This increments each by one.
        """
        def add_func(row_key, col_name):
            column_family.add(row_key, col_name)
        self._iterate_over_counter_columns(num_keys, func)
        

    # TODO: Improve the type and increase the amount of validation performed here
    def validate(self, start_index=0, end_index=sys.maxint, step=1, server_list=['localhost:9160']):
        """
        gets the rows from start_index (inclusive) to end_index (exclusive) and
        compares them against what they are supposed to be. If end_index
        is greater than what has been inserted, it will read to the last
        value that was inserted.
        """
        if end_index > self._inserted_key_count:
            end_index = self._inserted_key_count
        assert(start_index <= end_index)

        pool = self._get_pool(server_list=server_list)
        cf = pycassa.ColumnFamily(pool, self._params['column_family_name'])

        if self._params['is_counter']:
            self._validate_counter(num_
            
        else:
            # generate what we expect to read
            rows = self._gen_rows(start_index, end_index, step)
            read_rows = cf.multiget(rows.keys(), read_consistency_level=
                    self._params['consistency_level'])

            if len(list(read_rows)) < len(rows):
                raise Exception("number of rows (%s) doesn't match expected number (%s)" % (str(len(read_rows)), str(len(rows))))

            # check every row to make sure everything matches
            for row_key, row_value in rows.items():
                read_row_value = read_rows[row_key]
                if row_value != read_row_value:
                    raise AssertionError(
                    "The value written does not match the value read! written: %s written: %s" %
                    (pprint.pformat(row_value), pprint.pformat(read_row_value)))
                
    def _validate_counter(self, cf):
        def add_func(row_key, col_name):
            column_family.add(row_key, col_name)
        self._iterate_over_counter_columns(num_keys, func)
        

    def _gen_rows(self, start_index, end_index, step=1):
        """
        Generates a bunch of rows from start_index (inclusive) to end_index (exclusive).
        """
        rows = dict()
        for row_num in xrange(start_index, end_index, step):
            if self._params['column_family_type'].lower() == 'super':
                sub_cols = dict((
                        self._generate_col_name(self._params['subcomparator_type'], i),
                        self._generate_col_value(i)
                        ) for i in xrange(self._params['num_subcols']))
                cols = dict((
                        self._generate_col_name(self._params['comparator_type'], i),
                        sub_cols) for i in xrange(self._params['num_cols']))
            else:
                cols = dict((
                        self._generate_col_name(self._params['comparator_type'], i),
                        self._generate_col_value(i))
                        for i in xrange(self._params['num_cols']))

            rows[self._generate_row_key(row_num)] = cols

        return rows

    def _generate_row_key(self, num):
        return self._convert(self._params['key_validation_type'], prefix='row_', num=num)

    def _generate_col_name(self, subcomparator, num):
        return self._convert(subcomparator, prefix='col_', num=num)

    def _generate_col_value(self, num):
        return self._convert(self._params['validation_type'], prefix='val_', num=num)

    def _convert(self, target_type, prefix=None, num=None):
        if target_type in ('AsciiType', 'BytesType'):
            return str(prefix) + str(num)

        if target_type == 'UTF8Type':
            return str(prefix + str(num))

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
        if self._params['column_family_name'] in sm.get_keyspace_column_families(
                self._params['keyspace_name']).keys():
            # column family already exists
            return
        # sm.create_column_family prints to stdout, which
        # we don't want.
        # We redirect it temporarily here.
        sys.stdout = open(os.devnull, 'w')
        if self._params['is_counter']:
            sm.create_column_family(
                self._params['keyspace_name'], self._params['column_family_name'],
                super=self._params['column_family_type'].lower()=='super',
                default_validation_class='CounterColumnType',
            )
        else:
            sm.create_column_family(
                self._params['keyspace_name'], self._params['column_family_name'],
                super=self._params['column_family_type'].lower()=='super',
            )
            
        sys.stdout = sys.__stdout__
        sm.close()














