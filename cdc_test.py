from __future__ import division

import os
import time
from itertools import izip as zip

from cassandra import WriteFailure
from cassandra.concurrent import (execute_concurrent,
                                  execute_concurrent_with_args)

from dtest import Tester, debug
from tools import since
from utils.fileutils import size_of_files_in_dir
from utils.funcutils import get_rate_limited_function


def _set_cdc_on_table(session, table_name, value, ks_name=None):
    """
    Uses <session> to set CDC to <value> on <ks_name>.<table_name>.
    """
    table_string = ks_name + '.' + table_name if ks_name else table_name
    value_string = 'true' if value else 'false'
    stmt = 'ALTER TABLE ' + table_string + ' WITH CDC = ' + value_string

    debug(stmt)
    session.execute(stmt)


def _get_set_cdc_func(session, ks_name, table_name):
    """
    Close over a session, keyspace name, and table name and return a function
    that takes enables CDC on that keyspace if its argument is truthy and
    otherwise disables it.
    """
    def set_cdc(value):
        return _set_cdc_on_table(
            session=session,
            ks_name=ks_name, table_name=table_name,
            value=value
        )
    return set_cdc


def _get_commitlog_files(node_path):
    commitlog_dir = os.path.join(node_path, 'commitlogs')
    return {
        os.path.join(commitlog_dir, name)
        for name in os.listdir(commitlog_dir)
    }


def _get_cdc_raw_files(node_path, cdc_raw_dir_name='cdc_raw'):
    commitlog_dir = os.path.join(node_path, cdc_raw_dir_name)
    return {
        os.path.join(commitlog_dir, name)
        for name in os.listdir(commitlog_dir)
    }


@since('3.8')
class TestCDC(Tester):
    """
    @jira_ticket CASSANDRA-8844

    Test the correctness of some features of CDC, Change Data Capture, which
    provides a view of the commitlog on tables for which it is enabled.
    """

    def prepare(self, ks_name,
                table_name=None, cdc_enabled_table=None,
                data_schema=None,
                configuration_overrides=None):
        """
        Create a 1-node cluster, start it, create a keyspace, and if
        <table_name>, create a table in that keyspace. If <cdc_enabled_table>,
        that table is created with CDC enabled. If <data_schema>, use that
        string to specify the schema of the table -- for example, a valid value
        is '(a int PRIMARY KEY, b int)'. The <configuration_overrides> is
        treated as a dict-like object and passed to
        self.cluster.set_configuration_options.
        """
        config_defaults = {
            'cdc_enabled': True,
            # we want to be able to generate new segments quickly
            'commitlog_segment_size_in_mb': 2,
        }
        if configuration_overrides is None:
            configuration_overrides = {}
        self.cluster.populate(1)
        self.cluster.set_configuration_options(dict(config_defaults, **configuration_overrides))
        self.cluster.start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_ks(session, ks_name, rf=1)

        if table_name is not None:
            self.assertIsNotNone(cdc_enabled_table, 'if creating a table in prepare, must specify whether or not CDC is enabled on it')
            self.assertIsNotNone(data_schema, 'if creating a table in prepare, must specify its schema')
            stmt = ('CREATE TABLE ' + table_name +
                    ' ' + data_schema + ' '
                    'WITH CDC = ' + ('true' if cdc_enabled_table else 'false'))
            debug(stmt)
            session.execute(stmt)

        return node, session

    def _assert_cdc_data_readable_on_round_trip(self, start_with_cdc_enabled):
        """
        Parameterized test asserting that data written to a table is still
        readable after flipping the CDC flag on that table, then flipping it
        again. Starts with CDC enabled if start_with_cdc_enabled, otherwise
        starts with it disabled.
        """
        ks_name, table_name = 'ks', 'tab'
        sequence = [True, False, True] if start_with_cdc_enabled else [False, True, False]
        start_enabled, alter_path = sequence[0], list(sequence[1:])

        node, session = self.prepare(ks_name=ks_name, table_name=table_name,
                                     cdc_enabled_table=start_enabled,
                                     data_schema='(a int PRIMARY KEY, b int)')
        set_cdc = _get_set_cdc_func(session=session, ks_name=ks_name, table_name=table_name)

        insert_stmt = session.prepare('INSERT INTO ' + table_name + ' (a, b) VALUES (?, ?)')
        data = tuple(zip(list(range(1000)), list(range(1000))))
        execute_concurrent_with_args(session, insert_stmt, data)

        # We need data to be in commitlogs, not sstables.
        self.assertEqual([], list(node.get_sstables(ks_name, table_name)))

        for enable in alter_path:
            set_cdc(enable)
            self.assertItemsEqual(session.execute('SELECT * FROM ' + table_name), data)

    def test_cdc_enabled_data_readable_on_round_trip(self):
        """
        Test that data is readable after an enabled->disabled->enabled round
        trip.
        """
        self._assert_cdc_data_readable_on_round_trip(start_with_cdc_enabled=True)

    def test_cdc_disabled_data_readable_on_round_trip(self):
        """
        Test that data is readable after an disabled->enabled->disabled round
        trip.
        """
        self._assert_cdc_data_readable_on_round_trip(start_with_cdc_enabled=False)

    def test_insertion_and_commitlog_behavior_after_reaching_cdc_total_space(self):
        """
        Test that C* behaves correctly when CDC tables have consumed all the
        space available to them. In particular: after writing
        cdc_total_space_in_mb MB into CDC commitlogs:

        - CDC writes are rejected
        - non-CDC writes are accepted
        - on flush, CDC commitlogs are copied to cdc_raw
        - on flush, non-CDC commitlogs are not copied to cdc_raw

        This is a lot of behavior to validate in one test, but we do so to
        avoid running multiple tests that each write 1MB of data to fill
        cdc_total_space_in_mb.
        """
        ks_name, full_cdc_table_name = 'ks', 'full_cdc_tab'

        configuration_overrides = {
            # Make CDC space as small as possible so we can fill it quickly.
            'cdc_total_space_in_mb': 16,
        }
        node, session = self.prepare(
            ks_name=ks_name,
            table_name=full_cdc_table_name, cdc_enabled_table=True,
            data_schema='(a uuid PRIMARY KEY, b uuid, c uuid, d uuid, e uuid, '
                        'f uuid, g uuid, h uuid, i uuid, j uuid, k uuid, l uuid, '
                        'm uuid, n uuid, o uuid, p uuid)',
            configuration_overrides=configuration_overrides
        )
        insert_stmt = session.prepare(
            'INSERT INTO ' + ks_name + '.' + full_cdc_table_name +
            ' (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) '
            'VALUES (uuid(), uuid(), uuid(), uuid(), uuid(), '
            'uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), '
            'uuid(), uuid(), uuid(), uuid(), uuid())')

        # Later, we'll also make assertions about the behavior of non-CDC
        # tables, so we create one here.
        non_cdc_table_name = 'non_cdc_tab'
        session.execute('CREATE TABLE ' + ks_name + '.' + non_cdc_table_name + ' '
                        '(a uuid PRIMARY KEY, b uuid, c uuid, d uuid, e uuid, '
                        'f uuid, g uuid, h uuid, i uuid, j uuid, k uuid, l uuid, '
                        'm uuid, n uuid, o uuid, p uuid)')
        # We'll also make assertions about the behavior of CDC tables when
        # other CDC tables have already filled the designated space for CDC
        # commitlogs, so we create the second CDC table here.
        emtpy_cdc_table_name = 'empty_cdc_tab'
        session.execute('CREATE TABLE ' + ks_name + '.' + emtpy_cdc_table_name + ' '
                        '(a uuid PRIMARY KEY, b uuid, c uuid, d uuid, e uuid, '
                        'f uuid, g uuid, h uuid, i uuid, j uuid, k uuid, l uuid, '
                        'm uuid, n uuid, o uuid, p uuid) '
                        'WITH CDC = true')

        # Here, we insert values into the first CDC table until we get a
        # WriteFailure. This should happen when the CDC commitlogs take up 1MB
        # or more.
        debug('flushing non-CDC commitlogs')
        node.flush()
        # Then, we insert rows into the CDC table until we can't anymore.
        start, rows_loaded, error_found = time.time(), 0, False
        rate_limited_debug = get_rate_limited_function(debug, 5)
        debug('beginning data insert to fill CDC commitlogs')
        while not error_found:
            # We want to fail if inserting data takes too long. Locally this
            # takes about 10s, but let's be generous.
            self.assertLessEqual(
                (time.time() - start), 600,
                "It's taken more than 10 minutes to reach a WriteFailure trying "
                'to overrun the space designated for CDC commitlogs. This could '
                "be because data isn't being written quickly enough in this "
                'environment, or because C* is failing to reject writes when '
                'it should.'
            )

            # If we haven't logged from here in the last 5s, do so.
            rate_limited_debug(
                '  data load step has lasted {s:.2f}s, '
                'loaded {r} rows'.format(s=(time.time() - start), r=rows_loaded))

            batch_results = list(execute_concurrent(
                session,
                ((insert_stmt, ()) for _ in range(1000)),
                concurrency=500,
                # Don't propogate errors to the main thread. We expect at least
                # one WriteFailure, so we handle it below as part of the
                # results recieved from this method.
                raise_on_first_error=False
            ))

            # Here, we track the number of inserted values by getting the
            # number of successfully completed statements...
            rows_loaded += len([br for br in batch_results if br[0]])
            # then, we make sure that the only failures are the expected
            # WriteFailures.
            self.assertEqual([],
                             [result for (success, result) in batch_results
                              if not success and not isinstance(result, WriteFailure)])
            # Finally, if we find a WriteFailure, that means we've inserted all
            # the CDC data we can and so we flip error_found to exit the loop.
            if any(isinstance(result, WriteFailure) for (_, result) in batch_results):
                debug("write failed (presumably because we've overrun "
                      'designated CDC commitlog space) after '
                      'loading {r} rows in {s:.2f}s'.format(
                          r=rows_loaded,
                          s=time.time() - start))
                error_found = True

        self.assertLess(0, rows_loaded,
                        'No CDC rows inserted. This may happen when '
                        'cdc_total_space_in_mb > commitlog_segment_size_in_mb')

        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        commitlogs_size = size_of_files_in_dir(commitlog_dir)
        debug('Commitlog dir ({d}) is {b}B'.format(d=commitlog_dir, b=commitlogs_size))
        # This is a weak assertion -- there can be all kinds of, e.g., system
        # data in the commitlogs, so this doesn't necessarily mean there's 1MB
        # of data in CDC commitlogs. However, if there's less, we have a
        # problem.
        self.assertGreaterEqual(commitlogs_size, 1024 ** 2)

        # We should get a WriteFailure when trying to write to the CDC table
        # that's filled the designated CDC space...
        with self.assertRaises(WriteFailure):
            session.execute(
                'INSERT INTO ' + ks_name + '.' + full_cdc_table_name + ' '
                '(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) '
                'VALUES (uuid(), uuid(), uuid(), uuid(), uuid(), '
                'uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), '
                'uuid(), uuid(), uuid(), uuid(), uuid())'
            )
        # or any CDC table.
        with self.assertRaises(WriteFailure):
            session.execute(
                'INSERT INTO ' + ks_name + '.' + emtpy_cdc_table_name + ' '
                '(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) '
                'VALUES (uuid(), uuid(), uuid(), uuid(), uuid(), '
                'uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), '
                'uuid(), uuid(), uuid(), uuid(), uuid())'
            )

        # Now we test for behaviors of non-CDC tables when we've exceeded
        # cdc_total_space_in_mb.
        #
        # First, we flush and save the names of all the new discarded CDC
        # segments
        node.flush()
        pre_non_cdc_write_cdc_raw_segments = _get_cdc_raw_files(node.get_path())
        # save the names of all the commitlog segments written up to this
        # point:
        pre_non_cdc_write_segments = _get_commitlog_files(node.get_path())

        # Check that writing to non-CDC tables succeeds even when writes to CDC
        # tables are rejected:
        non_cdc_prepared_insert = session.prepare(
            'INSERT INTO ' + ks_name + '.' + non_cdc_table_name + ' '
            '(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) '
            'VALUES (uuid(), uuid(), uuid(), uuid(), uuid(), '
            'uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), '
            'uuid(), uuid(), uuid(), uuid(), uuid())'
        )
        session.execute(non_cdc_prepared_insert, ())

        # Check the following property: any new commitlog segments written to
        # after cdc_raw has reached its maximum configured size should not be
        # moved to cdc_raw, on commitlog discard, because any such commitlog
        # segments are written to non-CDC tables.
        #
        # First, write to non-cdc tables.
        start, time_limit = time.time(), 600
        debug('writing to non-cdc table')
        # We write until we get a new commitlog segment.
        while _get_commitlog_files(node.get_path()) <= pre_non_cdc_write_segments:
            elapsed = time.time() - start
            rate_limited_debug('  non-cdc load step has lasted {s:.2f}s'.format(s=elapsed))
            self.assertLessEqual(
                elapsed, time_limit,
                "It's been over a {s}s and we haven't written a new "
                "commitlog segment. Something is wrong.".format(s=time_limit)
            )
            execute_concurrent(
                session,
                ((non_cdc_prepared_insert, ()) for _ in range(1000)),
                concurrency=500,
                raise_on_first_error=True,
            )

        # Finally, we check that flushing doesn't move any new segments to cdc_raw:
        node.flush()
        self.assertEqual(pre_non_cdc_write_cdc_raw_segments, _get_cdc_raw_files(node.get_path()))

    # TODO: add tests to determine that CDC data is correctly flushed to
    # cdc_raw and not discarded. Our basic goal is to write dataset A to CDC
    # tables and dataset B to non-CDC tables, flush, then check that a superset
    # of data A is available to a client in cdc_raw. We can do this by:
    #   - writing a mixed CDC/non-CDC dataset, tracking all data that should be
    #     exposed via CDC,
    #   - flushing,
    #   - saving off the contents of cdc_raw,
    #   - shutting down the cluster,
    #   - starting a new cluster,
    #   - initializing schema as necessary,
    #   - shutting down the new cluster,
    #   - moving the saved cdc_raw contents to commitlog directories,
    #   - starting the new cluster to commitlog replay, then
    #   - asserting all data that should have been exposed via CDC is in the
    #     table(s) that have been written to tables in the new cluster.
