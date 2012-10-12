#!/usr/bin/env python

from dtest import debug
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
import cql

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO


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
        'replication_factor': '3',

        'is_counter': False,
        'is_indexed': False,
        'column_family_name': None, # needs to be overwritten for the moment
        'consistency_level': 'QUORUM',
        'column_family_type': 'standard',
        'validation_type': 'text',
        'comparator_type': 'text',
        'key_validation_type': 'text',
        'num_cols': 5,
        'num_counter_rows': 10, # only applies to counter columns
    }


    def __init__(self, host='localhost', port=9160, create_ks=True, create_cf=True, **kwargs):

        # allow for overwriting any of the defaults
        self._params = LoadMaker._DEFAULTS.copy()
        cf_name_generated = 'cf'
        for key, value in kwargs.items():
            if key not in self._params.keys():
                raise AttributeError("%s is an illegal arguement!" % key)
            self._params[key] = value
            cf_name_generated += '_' + key + '_' + re.sub('\W', '', str(value))

        # use our generated column family name if not given:
        if self._params['column_family_name'] is None:
            # keep the column family name within 32 chars:
            if len(cf_name_generated) > 32:
                import hashlib
                cf_name_generated = hashlib.md5(cf_name_generated).hexdigest()
            self._params['column_family_name'] = ('cf_' + cf_name_generated)[:32]

        # column_family_type should be lowercase so that future comparisons will work.
        self._params['column_family_type'] = self._params['column_family_type'].lower()

        self._num_generate_calls = 0

        # Keys are in a sort of ever-growing queue. They are inserted at the
        # end of high indexes, and updated and deleted from low indexes.
        # Currently you can only delete() keys that you have update()d.
        self._inserted_key_count = 0
        self._updated_key_count = 0
        self._deleted_key_count = 0

        # time each DB operationan and return the last one. Only times,
        # as much as possible, the DB portion of the operation.
        self.last_operation_time = 0

        self._is_keyspace_created = False
        self.refresh_connection(host, port)
        if create_ks:
            self.create_keyspace(self._cursor)
        self._is_keyspace_created = True
        cql_str = "USE %s" % self._params['keyspace_name']
        self.execute_query(cql_str)

        if create_cf:
            self.create_column_family()
        elif self._params['is_counter']:
            # Now find the value that the counters should have. They should all have the same value.
            row_key = self._generate_row_key(0)
            col_name = self._generate_col_name(0)
            cql_str = "SELECT '%s' FROM %s WHERE KEY='%s'"%(
                    col_name, self._params['column_family_name'], row_key)
            self.execute_query(cql_str)
            from_db = self._cursor.fetchone()
            self._num_generate_calls = from_db[0] or 0

    
    @property
    def _cursor(self):
        if self._cached_cursor == None:
            self.refresh_connection()
        return self._cached_cursor
                

    def refresh_connection(self, host=None, port=None, cql_version="2.0.0", num_retries=10):
        """
        establish a connection to the server. retry if needed.
        """
        if host:
            self._host = host
        if port:
            self._port = port
        self._cql_version = cql_version or None
            
        for try_num in xrange(1+num_retries):
            try:
                if self._cql_version:
                    con = cql.connect(self._host, self._port, keyspace=None, cql_version=self._cql_version)
                else:
                    con = cql.connect(self._host, self._port, keyspace=None)
                self._cached_cursor = con.cursor()
                if self._is_keyspace_created:
                    cql_str = "USE %s" % self._params['keyspace_name']
                    self.execute_query(cql_str)
            except:
                if try_num < num_retries:
                    time.sleep(1)
                else:
                    raise


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
    
    
    def batch_insert(self, rows):
        cql_stream = StringIO.StringIO()
        cql_stream.write("BEGIN BATCH USING CONSISTENCY %s\n" % self._params['consistency_level'])
        for key, col_dict in rows.items():
            col_names = col_dict.keys()
            columns = ', '.join("'"+str(a)+"'" for a in col_names)
            values = ', '.join("'"+str(col_dict[a])+"'" for a in col_names)
            cql_stream.write("INSERT INTO %s (KEY, %s) VALUES ('%s', %s);\n" % (
                    self._params['column_family_name'],
                    columns,
                    key,
                    values,
                    ))
        cql_stream.write("APPLY BATCH;")
        self.execute_query(cql_stream.getvalue())


    def generate(self, num_keys=10000):
        """
        Generates a bunch of data and inserts it into cassandra
        """
        debug("Generate() starting " + str(self))
        new_inserted_key_count = self._inserted_key_count + num_keys
        
        if self._params['is_counter']:
            self._generate_counter()
        else:
            rows = self._gen_rows(self._inserted_key_count, new_inserted_key_count)
            self.batch_insert(rows)
            debug("Generate inserted %d rows" % len(rows))

        self._inserted_key_count = new_inserted_key_count
        self._num_generate_calls += 1

        return self


    def update(self, num_keys=1000):
        """
        Update some keys that were previously inserted.
        """
        saved_updated_key_count = self._updated_key_count
        self._updated_key_count += num_keys
        assert self._updated_key_count <= self._inserted_key_count, "You have to generate() more then you update()!"

        if self._params['is_counter']:
            raise NotImplemented("Counter updates have not been implemented yet.")
        else:
            rows = self._gen_rows(saved_updated_key_count, self._updated_key_count)
            # do the update
            self.batch_insert(rows)
            debug("update() inserted %d rows" % len(rows))

            # remove the first column from each row
            for row_key in rows.keys():
                col_name = self._generate_col_name(0)
                cql_str = "DELETE '%s' FROM %s WHERE KEY='%s'"%(col_name, self._params['column_family_name'], row_key)
                self.execute_query(cql_str)

        return self


    def delete(self, num_keys=100):
        """
        deletes some rows.
        """
        saved_deleted_key_count = self._deleted_key_count
        self._deleted_key_count += num_keys
        assert self._deleted_key_count <= self._updated_key_count, "You have to update() more then you delete()!"

        row_keys = [self._generate_row_key(i) for i in xrange(saved_deleted_key_count, self._deleted_key_count)]
        for row_key in row_keys:
            cql_str = "DELETE FROM %s WHERE KEY='%s'"%(self._params['column_family_name'], row_key)
            self.execute_query(cql_str)
        return self
            

    def _iterate_over_counter_columns(self, func):
        """
        calls func on every column that should be in the counter column family.

        func should have a signature like this:
        func(row_key, col_name, subcol_name=None)
        """
        col_count = 0
        for row_index in xrange(self._params['num_counter_rows']):
            row_key = self._generate_row_key(row_index)
            for col_index in xrange(self._params['num_cols']):
                col_name = self._generate_col_name(col_index)
                func(row_key, col_name)
                col_count += 1
        debug("iterated over %d counter columns" % col_count)


    def _generate_counter(self):
        """
        increments all counters. There are num_keys counter rows,
        each with self._params['num_cols'] individual counters.
        This increments each by one.
        """
        def add_func(row_key, col_name, subcol_name=None):
            cql_str = """UPDATE %(cf_name)s SET %(col_name)s=%(col_name)s+1
            WHERE KEY=%(row_key)s;""" % {
                'cf_name': self._params['column_family_name'],
                'col_name': col_name,
                'row_key': row_key,
            }
            self.execute_query(cql_str, num_retries=0)

        self._iterate_over_counter_columns(add_func)


    def multiget(self, keys):
        assert len(keys) > 0, "At least one key must be specified!"
        keys_str = ', '.join("'"+str(key)+"'" for key in keys)
        cql_str = "SELECT * FROM %s USING CONSISTENCY %s WHERE KEY in (%s)" % (
            self._params['column_family_name'], self._params['consistency_level'], keys_str)
        self.execute_query(cql_str)
        rows = self._cursor.result
        out = {}
        for row in rows:
            row_key = row[0].value
            out[row_key] = {}
            cols = row[1:]
            for col in cols:
                col_name = col.name
                col_value = col.value
                out[row_key][col_name] = col_value

        return out
        

    def validate(self, start_index=0, end_index=sys.maxint, step=1, server_list=['localhost:9160']):
        """
        gets the rows from start_index (inclusive) to end_index (exclusive) and
        compares them against what they are supposed to be. If end_index
        is greater than what has been inserted, it will read to the last
        value that was inserted.
        """
        debug("validate() starting " + str(self))
        if end_index > self._inserted_key_count:
            end_index = self._inserted_key_count
        assert(start_index <= end_index)

        if self._params['is_counter']:
            self._validate_counter()

        else:
            # generate what we expect to read
            rows = self._gen_rows(start_index, end_index, step)
            read_rows = self.multiget(rows.keys())

            if len(list(read_rows)) < len(rows):
                raise Exception("number of rows (%s) doesn't match expected number (%s)" % (str(len(read_rows)), str(len(rows))))

            # check every row to make sure everything matches
            for row_key, row_value in rows.items():
                read_row_value = read_rows[row_key]
                if row_value != read_row_value:
                    raise AssertionError(
                    "The value written does not match the value read! should be: %s was: %s" %
                    (pprint.pformat(row_value), pprint.pformat(read_row_value)))

            # make sure that deleted rows really are gone.
            row_keys = [self._generate_row_key(i) for i in xrange(0, self._deleted_key_count, step)]

        debug("validate() succeeded")
        return self


    def _validate_counter(self):
        def validate_func(row_key, col_name):
            assert self._num_generate_calls > 0, "Data must be generated before validating!"
            cql_str = "SELECT '%s' FROM %s WHERE KEY='%s'"%(col_name, self._params['column_family_name'], row_key)
            self.execute_query(cql_str)
            from_db = self._cursor.fetchone()
            val = from_db[0]
            assert self._num_generate_calls == val, "A counter did not have the right value! %s != %s, QUERY: %s" %(val, self._num_generate_calls, cql_str)
        self._iterate_over_counter_columns(validate_func)


    def _gen_rows(self, start_index, end_index, step=1):
        """
        Generates a bunch of rows from start_index (inclusive) to end_index (exclusive).
        Properly generates updated or non-updated rows.
        """
        rows = dict()
        for row_num in xrange(max(start_index, self._deleted_key_count), end_index, step):
            if row_num < self._updated_key_count:
                is_update = True
                shift_by_one = 1
            else:
                is_update = False
                shift_by_one = 0
            cols = dict((
                    self._generate_col_name(i),
                    self._generate_col_value(i, is_update))
                    for i in xrange(shift_by_one, self._params['num_cols']+shift_by_one))

            rows[self._generate_row_key(row_num)] = cols

        return rows


    def _generate_row_key(self, num):
        return self._convert(prefix='row_', num=num)


    def _generate_col_name(self, num):
        return self._convert(prefix='col_', num=num)


    def _generate_col_value(self, num, is_update=False):
        # is_update means that the value should be modified to indicate it has been updated.
        prefix = 'val_'
        if is_update:
            num += 10000
            prefix = 'val_updated_'
        return self._convert(prefix=prefix, num=num)


    def _convert(self, prefix=None, num=None):
        return str(prefix + str(num))


    def create_keyspace(self, cursor):
        keyspace_name = self._params['keyspace_name']
        cql_str = ("CREATE KEYSPACE %s WITH strategy_class=SimpleStrategy AND "
                "strategy_options:replication_factor=%d" % (keyspace_name, int(self._params['replication_factor'])))
        try:
            self.execute_query(cql_str)
        except cql.ProgrammingError, e:
            # the ks already exists
            pass


    def create_column_family(self):
        """ The columnfamily should not already exist! """
        cf_name = self._params['column_family_name']
        if self._params['is_counter']:
            cql_str = """
            CREATE COLUMNFAMILY %s (KEY text PRIMARY KEY) WITH default_validation=counter""" % cf_name
            self.execute_query(cql_str)
        else:
            cql_str = """
            CREATE COLUMNFAMILY %s (KEY %s PRIMARY KEY) WITH comparator=%s
            AND default_validation=%s""" % (cf_name, 
                self._params['key_validation_type'],
                self._params['comparator_type'],
                self._params['validation_type'])
            self.execute_query(cql_str)

    
    def execute_query(self, cql_str, num_retries=10):
        """
        execute the query, and retry several times if needed.
        """
        debug(cql_str)
        for try_num in xrange(num_retries+1):
            try:
                self._cursor.execute(cql_str)
            except cql.ProgrammingError:
                raise
            except Exception, e:
                if try_num == num_retries:
                    raise
                # try to reconnect
                time.sleep(1)
                self.refresh_connection()
            else:
                break



class ContinuousLoader(threading.Thread):
    """
    Hits the db continuously with LoadMaker. Can handle standard and 
    counter columnfamilies

    Applies each type of load in a round-robin fashion
    """
    def __init__(self, load_makers=[], sleep_between=1):
        """
        load_makers is a list of load_makers to run

        sleep_between will slow down loading of the cluster
        by sleeping between every insert operation this many seconds.
        """
        self._load_makers = load_makers
        self._sleep_between = sleep_between
        self._inserting_lock = threading.Lock()
        self._is_loading = True
        self._should_exit = False
        super(ContinuousLoader, self).__init__()
        self.setDaemon(True)
        self.exception = None

        # make sure each loader gets called at least once.
        debug("calling ContinuousLoader()._generate_load_once() from __init__().")
        self._generate_load_once()

        # now fire up the loaders to continuously load the system.
        self.start()


    def run(self):
        """
        applies load whenever it isn't paused.
        """
        debug("Loadmaker started")
        while True:
            if self._should_exit:
                break
            self._generate_load_once()
        debug("continuous loader exiting.")


    def _generate_load_once(self):
        """
        runs one round of load with all the load_makers.
        """
        debug("ContinuousLoader()._generate_load_once() starting")
        for load_maker in self._load_makers:
            self._inserting_lock.acquire()

            # exit if needed.
            if self._should_exit:
                return

            try:
                load_maker.generate(num_keys=3)
            except Exception, e:
                # if anything goes wrong, store the exception
                e.args = e.args + (str(load_maker), )
                self.exception = (e, sys.exc_info()[2])
                raise
            finally:
                self._inserting_lock.release()
            if self._sleep_between:
                time.sleep(self._sleep_between)
        debug("ContinuousLoader()._generate_load_once() done.")


    def exit(self):
        self._should_exit = True
        if self._is_loading == False:
            self.unpause()


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
        debug("read_and_validate()")
        self.check_exc()
        self.pause()
        debug("Sleeping %.2f seconds.." % pause_before_validate)
        time.sleep(pause_before_validate)
        for load_maker in self._load_makers:
            try:
                load_maker.validate(step=step)
            except Exception, e:
                # put the loader into the exception to make life easier
                e.args = e.args + (str(load_maker), )
                raise
        self.unpause()


    def pause(self):
        """
        acquires the _inserting_lock to stop the loading from happening.
        """
        assert self._is_loading == True, "Called Pause while not loading!"
        self._inserting_lock.acquire()
        debug("paused continuousloader...")
        self._is_loading = False


    def unpause(self):
        """
        releases the _inserting_lock to resume loading.
        """
        assert self._is_loading == False, "Called Pause while loading!"
        debug("unpausing continuousloader...")
        self._inserting_lock.release()
        self._is_loading = True



