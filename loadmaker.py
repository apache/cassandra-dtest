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
import threading
import logging

import pycassa
import pycassa.system_manager as system_manager


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
        'num_counter_rows': 10, # only applies to counter columns
    }

    def __init__(self, server_list=['localhost'], **kwargs):

        # allow for overwriting any of the defaults
        self._params = LoadMaker._DEFAULTS.copy()
        for key, value in kwargs.items():
            if key not in self._params.keys():
                raise AttributeError("%s is an illegal arguement!" % key)
            self._params[key] = value

        # validate the consistency_level
        self._params['validated_consistency_level'] = getattr(
                pycassa.cassandra.ttypes.ConsistencyLevel,
                self._params['consistency_level'])
        # column_family_type should be lowercase so that future comparisons will work.
        self._params['column_family_type'] = self._params['column_family_type'].lower()

        self._inserted_key_count = 0
        self._num_generate_calls = 0

        self.create_keyspace()
        self.create_column_family()

        self.set_server_list(server_list)

    def __str__(self):
        d = {'is_counter': self._params['is_counter'],
             'column_family_type': self._params['column_family_type']}
        return "<LoadMaker %s>" % str(d)

    def str_info(self):
        # print out the _params and some other stuff
        params = dict(list(self._params.items()) + list({
            '_inserted_key_count': self._inserted_key_count,
            '_num_generate_calls': self._num_generate_calls,
        }.items()))
        return "LoadMaker<" + str(params) + ">"

    def generate(self, num_keys=10000):
        """
        Generates a bunch of data and inserts it into cassandra
        """
        logging.debug( "Generate() starting " + str(self))
        new_inserted_key_count = self._inserted_key_count + num_keys

        cf = pycassa.ColumnFamily(self._connection_pool, self._params['column_family_name'])

        if self._params['is_counter']:
            self._generate_counter(cf)
        else:
            rows = self._gen_rows(self._inserted_key_count, new_inserted_key_count)
            cf.batch_insert(rows, write_consistency_level=self._params['validated_consistency_level'])
            logging.debug("Generate inserted %d rows" % len(rows))

        self._inserted_key_count = new_inserted_key_count
        self._num_generate_calls += 1

    def _iterate_over_counter_columns(self, func):
        """
        calls func on every column that should be in the counter column family.

        func should have a signature like this:
        func(row_key, col_name, subcol_name=None)
        """
        # if we haven't gotten around to generating any data yet, bail now.
        col_count = 0
        for row_index in xrange(self._params['num_counter_rows']):
            row_key = self._generate_row_key(row_index)
            for col_index in xrange(self._params['num_cols']):
                col_name = self._generate_col_name(
                        self._params['comparator_type'], col_index)
                if self._params['column_family_type'] == 'super':
                    for subcol_index in xrange(self._params['num_cols']):
                        subcol_name = self._generate_col_name(
                                self._params['subcomparator_type'], subcol_index)
                        func(row_key, col_name, subcol_name)
                        col_count += 1
                else:
                    func(row_key, col_name)
                    col_count += 1
        logging.debug("iterated over %d counter columns" % col_count)


    def _generate_counter(self, cf):
        """
        increments all counters. There are num_keys counter rows,
        each with self._params['num_cols'] individual counters.
        This increments each by one.
        """
        def add_func(row_key, col_name, subcol_name=None):
            cf.add(row_key, col_name, super_column=subcol_name,
                    write_consistency_level=self._params['validated_consistency_level'])
        self._iterate_over_counter_columns(add_func)


    def validate(self, start_index=0, end_index=sys.maxint, step=1, server_list=['localhost:9160']):
        """
        gets the rows from start_index (inclusive) to end_index (exclusive) and
        compares them against what they are supposed to be. If end_index
        is greater than what has been inserted, it will read to the last
        value that was inserted.
        """
        logging.debug("validate() starting " + str(self))
        if end_index > self._inserted_key_count:
            end_index = self._inserted_key_count
        assert(start_index <= end_index)

        cf = pycassa.ColumnFamily(self._connection_pool, 
                self._params['column_family_name'])

        if self._params['is_counter']:
            self._validate_counter(cf)

        else:
            # generate what we expect to read
            rows = self._gen_rows(start_index, end_index, step)
            read_rows = cf.multiget(rows.keys(), read_consistency_level=
                    self._params['validated_consistency_level'])

            if len(list(read_rows)) < len(rows):
                raise Exception("number of rows (%s) doesn't match expected number (%s)" % (str(len(read_rows)), str(len(rows))))

            # check every row to make sure everything matches
            for row_key, row_value in rows.items():
                read_row_value = read_rows[row_key]
                if row_value != read_row_value:
                    raise AssertionError(
                    "The value written does not match the value read! written: %s written: %s" %
                    (pprint.pformat(row_value), pprint.pformat(read_row_value)))

        logging.debug("validate() succeeded")

    def _validate_counter(self, cf):
        def validate_func(row_key, col_name, subcol_name=None):
            assert self._num_generate_calls > 0, "Data must be generated before validating!"
            # cf.get() returns something like this: OrderedDict([('col_2', 3)])
            try:
                from_db = cf.get(row_key, [col_name], super_column=subcol_name,
                        read_consistency_level=self._params['validated_consistency_level'])
            except:
                print "cf.get failed!", row_key, col_name, subcol_name, self
                raise
            val = from_db[col_name]
            assert val == self._num_generate_calls, "A counter did not have the right value! %s != %s" %(val, self._num_generate_calls)
        self._iterate_over_counter_columns(validate_func)


    def _gen_rows(self, start_index, end_index, step=1):
        """
        Generates a bunch of rows from start_index (inclusive) to end_index (exclusive).
        """
        rows = dict()
        for row_num in xrange(start_index, end_index, step):
            if self._params['column_family_type'] == 'super':
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

    def set_server_list(self, server_list):
        self._connection_pool = pycassa.ConnectionPool(
                self._params['keyspace_name'], timeout=30,
                server_list=server_list, prefill=True, max_retries=0)

    def _get_consistency_level_type(self, cl):
        return getattr(pycassa.cassandra.ttypes.ConsistencyLevel, cl)


    def create_keyspace(self):
        keyspace_name = self._params['keyspace_name']

        sm = system_manager.SystemManager()

        keyspaces = sm.list_keyspaces()
        if keyspace_name not in keyspaces:
            sm.create_keyspace(keyspace_name, strategy_options=
                    self._params['keyspace_strategy_options'])
            logging.info("Created keyspace %s" % keyspace_name)
        else:
            logging.info("keyspace %s already existed" % keyspace_name)

#        logging.debug("keyspace replication factor: " + str(
#                sm.describe_keyspace(keyspace_name)['replication_factor']))
        sm.close()


    def create_column_family(self):
        sm = system_manager.SystemManager()
        cf_name = self._params['column_family_name']
        if cf_name in sm.get_keyspace_column_families(
                self._params['keyspace_name']).keys():
            # column family already exists
            logging.info("column family %s already exists" % cf_name)
        else:
            # sm.create_column_family prints to stdout, which
            # we don't want.
            # We redirect it temporarily here.
            sys.stdout = open(os.devnull, 'w')
            if self._params['is_counter']:
                sm.create_column_family(
                    self._params['keyspace_name'], self._params['column_family_name'],
                    super=self._params['column_family_type']=='super',
                    default_validation_class='CounterColumnType',
                )
            else:
                sm.create_column_family(
                    self._params['keyspace_name'], self._params['column_family_name'],
                    super=self._params['column_family_type']=='super',
                )

            sys.stdout = sys.__stdout__
            logging.info("Created column family %s" % cf_name)
        logging.debug("column family %s: %s" % (cf_name, 
            sm.get_keyspace_column_families(self._params['keyspace_name'])[cf_name]
        ))
        sm.close()




class ContinuousLoader(threading.Thread):
    """
    Hits the db continuously with LoadMaker. Can handle multiple kinds
    of loads (standard, super, counter, and super counter)

    Applies each type of load in a round-robin fashion
    """
    def __init__(self, load_makers=[]):
        """
        load_makers is a list of load_makers to run
        """
        self._load_makers = load_makers
        self._inserting_lock = threading.Lock()
        self._is_loading = True
        super(ContinuousLoader, self).__init__()
        self.setDaemon(True)
        self.exception = None

        # make sure each loader gets called at least once.
        logging.debug("calling ContinuousLoader()._generate_load_once() from __init__().")
        self._generate_load_once()

        # now fire up the loaders to continuously load the system.
        self.start()

    def run(self):
        """
        applies load whenever it isn't paused.
        """
        print "Loadmaker started"
        while True:
            self._generate_load_once()

    def _generate_load_once(self):
        """
        runs one round of load with all the load_makers.
        """
        logging.debug("ContinuousLoader()._generate_load_once() starting")
        for load_maker in self._load_makers:
            self._inserting_lock.acquire()
            try:
                load_maker.generate(num_keys=100)
            except Exception, e:
                # if anything goes wrong, store the exception
                e.args = e.args + (str(load_maker), )
                self.exception = (e, sys.exc_info()[2])
                raise
            finally:
                self._inserting_lock.release()
        logging.debug("ContinuousLoader()._generate_load_once() done.")

    def check_exc(self):
        """
        checks to see if anything has gone wrong inserting data, and bails
        out if it has.
        """
        if self.exception:
            raise self.exception[0], None, self.exception[1]

    def read_and_validate(self, step=100, pause_before_validate=3):
        """
        reads back all the data that has been inserted.
        Pauses loading while validating. Cannot already be paused.
        """
        logging.debug("read_and_validate()")
        self.check_exc()
        self.pause()
        logging.debug("Sleeping %.2f seconds.." % pause_before_validate)
        time.sleep(pause_before_validate)
        for load_maker in self._load_makers:
            load_maker.validate(step=step)
        self.unpause()

    def pause(self):
        """
        acquires the _inserting_lock to stop the loading from happening.
        """
        assert self._is_loading == True, "Called Pause while not loading!"
        self._inserting_lock.acquire()
        logging.debug("paused continuousloader...")
        self._is_loading = False

    def unpause(self):
        """
        releases the _inserting_lock to resume loading.
        """
        assert self._is_loading == False, "Called Pause while loading!"
        logging.debug("unpausing continuousloader...")
        self._inserting_lock.release()
        self._is_loading = True

    def update_server_list(self, server_list):
        if self._is_loading:
            self._inserting_lock.acquire()
        for lm in self._load_makers:
            lm.set_server_list(server_list)
        if self._is_loading:
            self._inserting_lock.release()



